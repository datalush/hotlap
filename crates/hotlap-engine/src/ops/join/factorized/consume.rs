//! Consume an owned prepared group result without publishing join state.

use arrow::datatypes::SchemaRef;
use arrow::row::RowConverter;
use hotlap_core::plan::AggSpec;
use hotlap_core::snapshot::GroupEntry;

use crate::error::EngineError;
use crate::ops::group_aggregate::columns::render;
use crate::ops::group_aggregate::materialize::{Change, materialize};

use super::prepare::{GroupChange, PreparedCut};

#[allow(dead_code)] // Wired by the later graph-cut stage; kept internal meanwhile.
pub(crate) struct ConsumedGroups {
    /// Touched-key replacement set; caller publishes only after all output succeeds.
    pub changes: Vec<(Vec<u8>, Option<GroupEntry>)>,
    pub output: crate::batch::ZSetBatch,
}

#[allow(dead_code)] // Wired by the later graph-cut stage; kept internal meanwhile.
pub(crate) fn consume(
    prepared: &PreparedCut,
    schema: SchemaRef,
    group_key: &[usize],
    aggs: &[AggSpec],
) -> Result<ConsumedGroups, EngineError> {
    let prior_groups: std::collections::HashMap<Vec<u8>, GroupEntry> =
        prepared.prior_groups.iter().cloned().collect();
    consume_changes(&prepared.groups, &prior_groups, schema, group_key, aggs)
}

pub(crate) fn consume_changes(
    groups: &[GroupChange],
    prior_groups: &std::collections::HashMap<Vec<u8>, GroupEntry>,
    schema: SchemaRef,
    group_key: &[usize],
    aggs: &[AggSpec],
) -> Result<ConsumedGroups, EngineError> {
    let mut changes = Vec::with_capacity(groups.len());
    for change in groups {
        match change {
            GroupChange::Remove { key } => {
                let previous = prior_groups
                    .get(key)
                    .map(|entry| render(&entry.values))
                    .transpose()?;
                if let Some(previous) = previous {
                    changes.push(Change {
                        key: key.clone(),
                        previous: Some(previous),
                        current: None,
                    });
                }
            }
            GroupChange::Upsert { key, entry } => {
                let previous = prior_groups
                    .get(key)
                    .map(|prior| render(&prior.values))
                    .transpose()?;
                let current = render(&entry.values)?;
                if previous.as_ref() != Some(&current) {
                    changes.push(Change {
                        key: key.clone(),
                        previous,
                        current: Some(current),
                    });
                }
            }
        }
    }
    changes.sort_by(|a, b| a.key.cmp(&b.key));
    let final_changes = groups
        .iter()
        .map(|change| match change {
            GroupChange::Upsert { key, entry } => (key.clone(), Some(entry.clone())),
            GroupChange::Remove { key } => (key.clone(), None),
        })
        .collect();
    let converter = RowConverter::new(
        group_key
            .iter()
            .map(|&i| arrow::row::SortField::new(schema.field(i).data_type().clone()))
            .collect(),
    )?;
    let output = materialize(group_key, aggs, Some(&schema), Some(&converter), &changes)?;
    Ok(ConsumedGroups {
        changes: final_changes,
        output,
    })
}
