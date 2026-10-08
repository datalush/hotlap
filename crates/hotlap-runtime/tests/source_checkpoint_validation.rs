//! Tests for checkpoint validation against declared sources and the engine.

#[path = "cross_source_support/source.rs"]
mod source_support;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::InputId;
use hotlap_connectors::source::{Source, Split};
use hotlap_engine::{
    ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot, InputSnapshot, WatermarkSpec, encode_schema,
};
use hotlap_runtime::runtime::pipeline::Watermark;
use hotlap_runtime::runtime::source_checkpoint::SourcesCheckpoint;
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use source_support::ControlledSource;

fn schema(nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, nullable)]))
}

fn split(id: i32) -> Split {
    Split { id, start: 0 }
}

fn input(
    id: u32,
    name: &str,
    source: Arc<ControlledSource>,
    watermark: Option<Watermark>,
) -> InputSource {
    InputSource { id: InputId(id), name: name.to_string(), source, watermark }
}

/// A set of two sources, ids 0 and 1, with distinct applied offsets.
fn two_sources() -> (Sources, Arc<ControlledSource>, Arc<ControlledSource>) {
    let s = schema(false);
    let (a, _ta) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (b, _tb) = ControlledSource::new(s, vec![split(0)]);
    a.commit(0, 5).unwrap();
    b.commit(0, 9).unwrap();
    let sources = Sources::new(vec![input(1, "b", b.clone(), None), input(0, "a", a.clone(), None)])
        .unwrap();
    (sources, a, b)
}

/// A snapshot whose inputs mirror the checkpoint, as a first push would leave it.
fn snapshot_matching(checkpoint: &SourcesCheckpoint, sources: &Sources) -> EngineSnapshot {
    let inputs = checkpoint
        .entries
        .iter()
        .map(|entry| {
            let splits = sources
                .get(entry.id)
                .unwrap()
                .source
                .splits()
                .unwrap()
                .into_iter()
                .map(|split| (split.id, 0))
                .collect();
            let spec = entry.watermark_lag.map(|lag| WatermarkSpec {
                time_col: entry.event_time_column.unwrap_or(0),
                lag,
            });
            InputSnapshot {
                id: entry.id,
                schema: Some(entry.schema.clone()),
                spec,
                watermark: 0,
                splits,
                late: 0,
            }
        })
        .collect();
    EngineSnapshot {
        format_version: ENGINE_SNAPSHOT_FORMAT_VERSION,
        epoch: 0,
        frozen: false,
        inputs,
        views: Vec::new(),
    }
}

#[test]
fn matching_checkpoint_validates() {
    let (sources, _a, _b) = two_sources();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let snapshot = snapshot_matching(&checkpoint, &sources);
    checkpoint.validate(&sources, &snapshot).unwrap();
}

#[test]
fn reordered_declaration_validates() {
    let (sources, a, b) = two_sources();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let snapshot = snapshot_matching(&checkpoint, &sources);
    let reordered = Sources::new(vec![input(0, "a", a, None), input(1, "b", b, None)]).unwrap();
    checkpoint.validate(&reordered, &snapshot).unwrap();
}

#[test]
fn missing_or_renamed_source_does_not_validate() {
    let (sources, a, b) = two_sources();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let snapshot = snapshot_matching(&checkpoint, &sources);
    let fewer = Sources::new(vec![input(1, "b", b.clone(), None)]).unwrap();
    assert!(checkpoint.validate(&fewer, &snapshot).is_err());
    let renamed = Sources::new(vec![input(0, "a2", a, None), input(1, "b", b, None)]).unwrap();
    assert!(checkpoint.validate(&renamed, &snapshot).is_err());
}

#[test]
fn changed_schema_does_not_validate() {
    let (sources, _a, b) = two_sources();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let snapshot = snapshot_matching(&checkpoint, &sources);
    let (a2, _ta) = ControlledSource::new(schema(true), vec![split(0)]);
    let changed = Sources::new(vec![input(0, "a", a2, None), input(1, "b", b, None)]).unwrap();
    assert!(checkpoint.validate(&changed, &snapshot).is_err());
}

#[test]
fn changed_lag_or_event_time_column_does_not_validate() {
    let s = schema(false);
    let (a, _ta) = ControlledSource::new(s.clone(), vec![split(0)]);
    a.set_event_time_column(Some(0));
    let (b, _tb) = ControlledSource::new(s, vec![split(0)]);
    let declared = Sources::new(vec![
        input(0, "a", a.clone(), Some(Watermark { lag: 5 })),
        input(1, "b", b.clone(), None),
    ])
    .unwrap();
    let checkpoint = SourcesCheckpoint::capture(&declared).unwrap();
    let snapshot = snapshot_matching(&checkpoint, &declared);

    let wrong_lag = Sources::new(vec![
        input(0, "a", a.clone(), Some(Watermark { lag: 7 })),
        input(1, "b", b.clone(), None),
    ])
    .unwrap();
    assert!(checkpoint.validate(&wrong_lag, &snapshot).is_err());

    a.set_event_time_column(Some(2));
    let wrong_column = Sources::new(vec![
        input(0, "a", a, Some(Watermark { lag: 5 })),
        input(1, "b", b, None),
    ])
    .unwrap();
    assert!(checkpoint.validate(&wrong_column, &snapshot).is_err());
}

#[test]
fn offset_for_unknown_split_does_not_validate() {
    let (sources, _a, _b) = two_sources();
    let mut checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let snapshot = snapshot_matching(&checkpoint, &sources);
    checkpoint.entries[0].state.offsets.insert(9, 1);
    assert!(checkpoint.validate(&sources, &snapshot).is_err());
}

#[test]
fn duplicate_checkpoint_ids_do_not_validate() {
    let (sources, _a, _b) = two_sources();
    let mut checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let snapshot = snapshot_matching(&checkpoint, &sources);
    checkpoint.entries[1].id = checkpoint.entries[0].id;
    assert!(checkpoint.validate(&sources, &snapshot).is_err());
}

#[test]
fn snapshot_schema_and_input_set_are_checked() {
    let (sources, _a, _b) = two_sources();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let base = snapshot_matching(&checkpoint, &sources);

    // A schema absent before the first push is allowed.
    let mut absent = base.clone();
    for input in &mut absent.inputs {
        input.schema = None;
    }
    checkpoint.validate(&sources, &absent).unwrap();

    // A materialized schema must match the checkpoint's.
    let mut mismatched = base.clone();
    mismatched.inputs[0].schema = Some(encode_schema(&schema(true)).unwrap());
    assert!(checkpoint.validate(&sources, &mismatched).is_err());

    // The snapshot's input set must match the checkpoint's.
    let mut missing = base;
    missing.inputs.pop();
    assert!(checkpoint.validate(&sources, &missing).is_err());
}
