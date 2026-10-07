//! Engine-level inner-join tests against full recomputation.

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

fn ints(z: &ZSetBatch, column: usize) -> Vec<i64> {
    let array = z
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..z.len()).map(|index| array.value(index)).collect()
}

fn strings(z: &ZSetBatch, column: usize) -> Vec<String> {
    let array = z
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..z.len())
        .map(|index| array.value(index).to_string())
        .collect()
}

fn diff_ints(z: &ZSetBatch) -> Vec<i64> {
    let array = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len()).map(|index| array.value(index)).collect()
}

/// Joined rows as sorted `((key, left, right), diff)` tuples.
fn join_rows(z: &ZSetBatch) -> Vec<((i64, String, String), i64)> {
    if z.is_empty() {
        return Vec::new();
    }
    let keys = ints(z, 0);
    let left = strings(z, 1);
    let right = strings(z, 3);
    let diffs = diff_ints(z);
    let mut out: Vec<((i64, String, String), i64)> = (0..z.len())
        .map(|i| ((keys[i], left[i].clone(), right[i].clone()), diffs[i]))
        .collect();
    out.sort();
    out
}

/// Full-recompute oracle for an inner equi-join over `k`.
fn recompute_join(
    left: &[(i64, &str, i64)],
    right: &[(i64, &str, i64)],
) -> Vec<((i64, String, String), i64)> {
    let mut out: std::collections::BTreeMap<(i64, String, String), i64> = Default::default();
    for (lk, lv, ld) in left {
        for (rk, rv, rd) in right {
            if lk == rk {
                *out.entry((*lk, lv.to_string(), rv.to_string())).or_default() += ld * rd;
            }
        }
    }
    out.into_iter().filter(|(_, diff)| *diff != 0).collect()
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

#[test]
fn join_matches_recomputation_with_retractions_across_pushes() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();

    let mut left: Vec<(i64, &str, i64)> = Vec::new();
    let mut right: Vec<(i64, &str, i64)> = Vec::new();
    let mut changelogs: Vec<ZSetBatch> = Vec::new();

    let left_pushes: Vec<Vec<(i64, &str, i64)>> = vec![
        vec![(1, "x", 1), (2, "y", 1)],
        vec![(2, "y", -1), (3, "z", 1)],
    ];
    let right_pushes: Vec<Vec<(i64, &str, i64)>> = vec![
        vec![(1, "p", 1), (1, "q", 1), (3, "r", 1)],
        vec![(3, "r", -1)],
    ];

    for round in 0..2 {
        left.extend(left_pushes[round].iter().copied());
        core.push(InputId(0), &text_zset(&left_pushes[round]))
            .unwrap();
        assert_eq!(
            join_rows(&core.snapshot(ViewId(0)).unwrap()),
            recompute_join(&left, &right)
        );

        right.extend(right_pushes[round].iter().copied());
        core.push(InputId(1), &text_zset(&right_pushes[round]))
            .unwrap();
        assert_eq!(
            join_rows(&core.snapshot(ViewId(0)).unwrap()),
            recompute_join(&left, &right)
        );
        changelogs.push(core.take_changes(ViewId(0)).unwrap());
    }

    // The changelog consolidates to the same join state as the snapshot.
    assert_eq!(
        join_rows(&consolidated(&changelogs)),
        join_rows(&core.snapshot(ViewId(0)).unwrap())
    );
}
