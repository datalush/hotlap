//! End-to-end tests for [`EngineCore`] against full recomputation.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::{EngineCore, consolidate};

fn text_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

/// Builds a Z-set of `(key, value, diff)` triples over the text schema.
fn text_zset(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let batch = RecordBatch::try_new(text_schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// Reads `(key, count)` pairs from a group-count output.
fn count_pairs(z: &ZSetBatch) -> Vec<(i64, i64)> {
    let keys = ints(z, 0);
    let counts = ints(z, 1);
    let mut out: Vec<(i64, i64)> = keys.into_iter().zip(counts).collect();
    out.sort();
    out
}

fn ints(z: &ZSetBatch, column: usize) -> Vec<i64> {
    let array = z
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..z.len()).map(|index| array.value(index)).collect()
}

/// Full-recompute oracle for `GROUP BY k`: sums diffs per key, dropping zeros.
fn recompute(history: &[(i64, &str, i64)]) -> Vec<(i64, i64)> {
    let mut counts: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for (key, _value, diff) in history {
        *counts.entry(*key).or_default() += diff;
    }
    counts.into_iter().filter(|(_, count)| *count != 0).collect()
}

/// Concatenates changelogs and consolidates them to net rows.
fn consolidated(changelogs: &[ZSetBatch]) -> ZSetBatch {
    let schema = changelogs[0].schema();
    let batches: Vec<&RecordBatch> = changelogs.iter().map(|z| &z.batch).collect();
    let batch = concat_batches(&schema, batches).unwrap();
    let diffs: Vec<&dyn Array> = changelogs.iter().map(|z| z.diff.as_ref()).collect();
    let all = ZSetBatch::new(batch, concat(&diffs).unwrap()).unwrap();
    consolidate(&all).unwrap()
}

fn group_plan() -> Plan {
    Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
    }
}

#[test]
fn group_count_snapshot_matches_recomputation_across_epochs() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.tap_view(ViewId(0)).unwrap();

    let epochs: Vec<Vec<(i64, &str, i64)>> = vec![
        vec![(1, "a", 1), (1, "b", 1), (2, "a", 1)],
        vec![(3, "c", 1), (2, "a", -1)],
        vec![(1, "a", -1)],
    ];

    let mut history: Vec<(i64, &str, i64)> = Vec::new();
    let mut changelogs: Vec<ZSetBatch> = Vec::new();
    for epoch in &epochs {
        history.extend(epoch.iter().copied());
        core.push(InputId(0), &text_zset(epoch)).unwrap();

        let snapshot = core.snapshot(ViewId(0)).unwrap();
        assert_eq!(count_pairs(&snapshot), recompute(&history));
        changelogs.push(core.take_changes(ViewId(0)).unwrap());
    }

    // The concatenated changelog consolidates to the same state as the snapshot.
    assert_eq!(
        count_pairs(&consolidated(&changelogs)),
        count_pairs(&core.snapshot(ViewId(0)).unwrap())
    );
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
}

#[test]
fn take_changes_is_empty_without_a_new_push() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core.push(InputId(0), &text_zset(&[(1, "a", 1)])).unwrap();

    let first = core.take_changes(ViewId(0)).unwrap();
    assert_eq!(count_pairs(&first), vec![(1, 1)]);
    let second = core.take_changes(ViewId(0)).unwrap();
    assert!(second.is_empty());
}

#[test]
fn untapped_view_has_no_changes() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &text_zset(&[(1, "a", 1)])).unwrap();

    assert!(core.take_changes(ViewId(0)).unwrap().is_empty());
}
