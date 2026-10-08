//! Cross-checks a [`SourcesCheckpoint`] against the declared sources and the
//! engine snapshot before recovery is allowed to consume any record.

use std::collections::{BTreeMap, BTreeSet};

use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_engine::{EngineSnapshot, decode_schema};

use crate::runtime::sources::{InputSource, Sources};

use super::{SavedSource, SourcesCheckpoint, codec_err};

/// Validate `checkpoint` against `sources` and `engine`.
pub(super) fn validate(
    checkpoint: &SourcesCheckpoint,
    sources: &Sources,
    engine: &EngineSnapshot,
) -> Result<(), ConnectorError> {
    let saved = saved_by_id(&checkpoint.entries)?;
    let declared = declared_by_id(sources);
    let saved_ids: BTreeSet<InputId> = saved.keys().copied().collect();
    let declared_ids: BTreeSet<InputId> = declared.keys().copied().collect();
    if saved_ids != declared_ids {
        return Err(unsupported(
            "checkpoint source ids do not match the declared sources",
        ));
    }
    for (id, entry) in &saved {
        validate_entry(entry, declared[id])?;
    }
    validate_snapshot(&saved, engine)
}

/// Index the checkpoint entries by id, rejecting duplicate ids or names.
fn saved_by_id(entries: &[SavedSource]) -> Result<BTreeMap<InputId, &SavedSource>, ConnectorError> {
    let mut by_id = BTreeMap::new();
    let mut names = BTreeSet::new();
    for entry in entries {
        if by_id.insert(entry.id, entry).is_some() {
            return Err(unsupported(format!("duplicate source id {:?}", entry.id)));
        }
        if !names.insert(entry.name.as_str()) {
            return Err(unsupported(format!("duplicate source name {}", entry.name)));
        }
    }
    Ok(by_id)
}

/// Index the declared inputs by id; [`Sources::new`] already enforces uniqueness.
fn declared_by_id(sources: &Sources) -> BTreeMap<InputId, &InputSource> {
    sources
        .entries()
        .iter()
        .map(|input| (input.id, input))
        .collect()
}

/// Compare one checkpoint entry with the declared input it names.
fn validate_entry(entry: &SavedSource, input: &InputSource) -> Result<(), ConnectorError> {
    if entry.name != input.name {
        return Err(unsupported(format!("source {:?} was renamed", entry.id)));
    }
    let saved_schema = decode_schema(&entry.schema).map_err(codec_err)?;
    if saved_schema != input.source.schema() {
        return Err(unsupported(format!(
            "schema for source {:?} does not match",
            entry.id
        )));
    }
    if entry.watermark_lag != input.watermark.map(|watermark| watermark.lag) {
        return Err(unsupported(format!(
            "watermark lag for source {:?} does not match",
            entry.id
        )));
    }
    if entry.event_time_column != input.source.event_time_column() {
        return Err(unsupported(format!(
            "event-time column for source {:?} does not match",
            entry.id
        )));
    }
    let splits: BTreeSet<i32> = input
        .source
        .splits()?
        .iter()
        .map(|split| split.id)
        .collect();
    for split in entry.state.offsets.keys() {
        if !splits.contains(split) {
            return Err(unsupported(format!(
                "offset for unknown split {split} in source {:?}",
                entry.id
            )));
        }
    }
    Ok(())
}

/// Compare the checkpoint ids and materialized schemas with the engine snapshot.
fn validate_snapshot(
    saved: &BTreeMap<InputId, &SavedSource>,
    engine: &EngineSnapshot,
) -> Result<(), ConnectorError> {
    let engine_ids: BTreeSet<InputId> = engine.inputs.iter().map(|input| input.id).collect();
    if engine_ids.len() != engine.inputs.len() {
        return Err(unsupported("engine snapshot has duplicate inputs"));
    }
    let saved_ids: BTreeSet<InputId> = saved.keys().copied().collect();
    if engine_ids != saved_ids {
        return Err(unsupported(
            "engine snapshot inputs do not match the checkpoint",
        ));
    }
    for input in &engine.inputs {
        let entry = saved[&input.id];
        if let Some(bytes) = &input.schema {
            let materialized = decode_schema(bytes).map_err(codec_err)?;
            let saved_schema = decode_schema(&entry.schema).map_err(codec_err)?;
            if materialized != saved_schema {
                return Err(unsupported(format!(
                    "engine schema for {:?} does not match",
                    input.id
                )));
            }
        }
        match (entry.watermark_lag, &input.spec) {
            (Some(lag), Some(spec)) => {
                if spec.lag != lag || entry.event_time_column != Some(spec.time_col) {
                    return Err(unsupported(format!(
                        "engine watermark for {:?} does not match",
                        input.id
                    )));
                }
            }
            (None, None) => {}
            _ => {
                return Err(unsupported(format!(
                    "engine watermark presence for {:?} does not match",
                    input.id
                )));
            }
        }
    }
    Ok(())
}

/// Build an unsupported error for an incompatible declaration or checkpoint.
fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::Unsupported(message.into())
}
