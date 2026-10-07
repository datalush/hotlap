use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_engine::ZSetBatch;
use hotlap_engine::consolidate;
use hotlap_engine::ops::Join;

fn schema(value_name: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new(value_name, DataType::Utf8, false),
    ]))
}

/// Builds a Z-set of `(key, value, diff)` rows over `schema`.
fn zset(schema: SchemaRef, rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn left(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    zset(schema("lv"), rows)
}

fn right(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    zset(schema("rv"), rows)
}

type JoinedRow = (i64, String, i64, String);

/// Reads a joined Z-set as sorted `((lk, lv, rk, rv), diff)` rows.
fn joined_rows(z: &ZSetBatch) -> Vec<(JoinedRow, i64)> {
    let lk = z.batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let lv = z.batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let rk = z.batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    let rv = z.batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<(JoinedRow, i64)> = (0..z.len())
        .map(|i| {
            let row = (
                lk.value(i),
                lv.value(i).to_string(),
                rk.value(i),
                rv.value(i).to_string(),
            );
            (row, diffs.value(i))
        })
        .collect();
    out.sort();
    out
}

/// Concatenates the join changelogs and consolidates them to net joined rows.
fn consolidated(changelogs: &[ZSetBatch]) -> Vec<(JoinedRow, i64)> {
    let schema = changelogs[0].schema();
    let batches: Vec<&RecordBatch> = changelogs.iter().map(|z| &z.batch).collect();
    let batch = concat_batches(&schema, batches).unwrap();
    let diffs: Vec<&dyn Array> = changelogs.iter().map(|z| z.diff.as_ref()).collect();
    let all = ZSetBatch::new(batch, concat(&diffs).unwrap()).unwrap();
    joined_rows(&consolidate(&all).unwrap())
}

/// Full-recompute oracle: joins the net left and right relations on the key.
fn recompute_join(left: &[(i64, &str, i64)], right: &[(i64, &str, i64)]) -> Vec<(JoinedRow, i64)> {
    let net = |rows: &[(i64, &str, i64)]| {
        let mut map: BTreeMap<(i64, String), i64> = BTreeMap::new();
        for (key, value, diff) in rows {
            *map.entry((*key, value.to_string())).or_default() += diff;
        }
        map.retain(|_, diff| *diff != 0);
        map
    };
    let (left, right) = (net(left), net(right));
    let mut out: BTreeMap<JoinedRow, i64> = BTreeMap::new();
    for ((lk, lv), ld) in &left {
        for ((rk, rv), rd) in &right {
            if lk == rk {
                *out.entry((*lk, lv.clone(), *rk, rv.clone())).or_default() += ld * rd;
            }
        }
    }
    out.retain(|_, diff| *diff != 0);
    out.into_iter().collect()
}

#[test]
fn join_matches_full_recompute_with_retractions() {
    let left_epochs = [
        vec![(1, "a", 1), (2, "b", 1)],
        vec![(1, "a", 1), (3, "c", 1)],
        vec![(2, "b", -1)],
    ];
    let right_epochs = [
        vec![(1, "x", 1)],
        vec![(1, "x", 1), (1, "y", 1)],
        vec![(1, "x", -1)],
    ];

    let mut join = Join::new(&[0], &[0]);
    let (mut history_left, mut history_right) = (Vec::new(), Vec::new());
    let mut changelogs = Vec::new();
    for (left_batch, right_batch) in left_epochs.iter().zip(&right_epochs) {
        history_left.extend(left_batch.iter().copied());
        history_right.extend(right_batch.iter().copied());
        changelogs.push(join.apply(&left(left_batch), &right(right_batch)).unwrap());
    }

    let expected = recompute_join(&history_left, &history_right);
    assert_eq!(consolidated(&changelogs), expected);
    assert_eq!(
        expected,
        vec![
            ((1, "a".to_string(), 1, "x".to_string()), 2),
            ((1, "a".to_string(), 1, "y".to_string()), 2),
        ]
    );
}

#[test]
fn retracting_entire_side_empties_join() {
    let mut join = Join::new(&[0], &[0]);
    let first = join.apply(&left(&[(1, "a", 1)]), &right(&[(1, "x", 1)])).unwrap();
    assert_eq!(
        joined_rows(&first),
        vec![((1, "a".to_string(), 1, "x".to_string()), 1)]
    );

    let second = join.apply(&left(&[]), &right(&[(1, "x", -1)])).unwrap();
    assert_eq!(
        joined_rows(&second),
        vec![((1, "a".to_string(), 1, "x".to_string()), -1)]
    );
    assert!(consolidated(&[first, second]).is_empty());
}

#[test]
fn retracting_left_row_retracts_its_joined_tuples() {
    let mut join = Join::new(&[0], &[0]);
    join.apply(&left(&[(1, "a", 1)]), &right(&[(1, "x", 1), (1, "y", 1)]))
        .unwrap();

    let second = join.apply(&left(&[(1, "a", -1)]), &right(&[])).unwrap();
    assert_eq!(
        joined_rows(&second),
        vec![
            ((1, "a".to_string(), 1, "x".to_string()), -1),
            ((1, "a".to_string(), 1, "y".to_string()), -1),
        ]
    );
}

#[test]
fn join_multiplies_side_diffs() {
    let mut join = Join::new(&[0], &[0]);
    let out = join
        .apply(&left(&[(1, "a", 2)]), &right(&[(1, "x", 3)]))
        .unwrap();
    assert_eq!(
        joined_rows(&out),
        vec![((1, "a".to_string(), 1, "x".to_string()), 6)]
    );
}

#[test]
fn join_of_empty_sides_is_empty_with_left_right_schema() {
    let mut join = Join::new(&[0], &[0]);
    let out = join.apply(&left(&[]), &right(&[])).unwrap();
    assert!(out.is_empty());
    assert_eq!(out.schema().fields().len(), 4);
    assert_eq!(out.schema().field(3).name(), "rv");
}

#[test]
fn join_rejects_mismatched_key_arity() {
    let mut join = Join::new(&[0], &[0, 1]);
    assert!(
        join.apply(&left(&[(1, "a", 1)]), &right(&[(1, "x", 1)]))
            .is_err()
    );
}
