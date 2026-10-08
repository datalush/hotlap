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
    let lk = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lv = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rk = z
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rv = z
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
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
fn zero_crossing_matches_full_recompute_across_epochs() {
    // The joined pair retracts to zero, is re-inserted, then retracts again as
    // each side cycles; the net changelog must equal full recomputation.
    let left_epochs = [
        vec![(1, "a", 1)],
        vec![(1, "a", -1)],
        vec![(1, "a", 1)],
        vec![],
    ];
    let right_epochs = [vec![(1, "x", 1)], vec![], vec![], vec![(1, "x", -1)]];

    let mut join = Join::new(&[0], &[0]);
    let (mut history_left, mut history_right) = (Vec::new(), Vec::new());
    let mut changelogs = Vec::new();
    for (left_batch, right_batch) in left_epochs.iter().zip(&right_epochs) {
        history_left.extend(left_batch.iter().copied());
        history_right.extend(right_batch.iter().copied());
        changelogs.push(join.apply(&left(left_batch), &right(right_batch)).unwrap());
    }

    assert_eq!(
        consolidated(&changelogs),
        recompute_join(&history_left, &history_right)
    );
    assert!(recompute_join(&history_left, &history_right).is_empty());
}

#[test]
fn retracting_entire_side_empties_join() {
    let mut join = Join::new(&[0], &[0]);
    let first = join
        .apply(&left(&[(1, "a", 1)]), &right(&[(1, "x", 1)]))
        .unwrap();
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

type PairRow = (i64, i64, i64);
type PairJoined = (i64, i64, i64, i64, i64, i64);

fn pair_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// Builds a Z-set of `(a, b, v, diff)` rows over the two-key schema.
fn pair_zset(rows: &[(i64, i64, i64, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.3).collect();
    let batch = RecordBatch::try_new(pair_schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// Reads a six-column Int64 joined Z-set as sorted `(row, diff)` pairs.
fn pair_joined_rows(zset: &ZSetBatch) -> Vec<(PairJoined, i64)> {
    let columns: Vec<Vec<i64>> = (0..6)
        .map(|column| {
            let array = zset
                .batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len()).map(|index| array.value(index)).collect()
        })
        .collect();
    let diffs = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<(PairJoined, i64)> = (0..zset.len())
        .map(|index| {
            let row = (
                columns[0][index],
                columns[1][index],
                columns[2][index],
                columns[3][index],
                columns[4][index],
                columns[5][index],
            );
            (row, diffs.value(index))
        })
        .collect();
    out.sort();
    out
}

fn consolidated_pairs(changelogs: &[ZSetBatch]) -> Vec<(PairJoined, i64)> {
    let schema = changelogs[0].schema();
    let batches: Vec<&RecordBatch> = changelogs.iter().map(|z| &z.batch).collect();
    let batch = concat_batches(&schema, batches).unwrap();
    let diffs: Vec<&dyn Array> = changelogs.iter().map(|z| z.diff.as_ref()).collect();
    let all = ZSetBatch::new(batch, concat(&diffs).unwrap()).unwrap();
    pair_joined_rows(&consolidate(&all).unwrap())
}

/// Full-recompute oracle joining the net sides on `matches`.
fn recompute_with<F: Fn(&PairRow, &PairRow) -> bool>(
    left: &[(i64, i64, i64, i64)],
    right: &[(i64, i64, i64, i64)],
    matches: F,
) -> Vec<(PairJoined, i64)> {
    let net = |rows: &[(i64, i64, i64, i64)]| {
        let mut map: BTreeMap<PairRow, i64> = BTreeMap::new();
        for &(a, b, v, diff) in rows {
            *map.entry((a, b, v)).or_default() += diff;
        }
        map.retain(|_, diff| *diff != 0);
        map
    };
    let (left, right) = (net(left), net(right));
    let mut out: BTreeMap<PairJoined, i64> = BTreeMap::new();
    for ((la, lb, lv), ld) in &left {
        for ((ra, rb, rv), rd) in &right {
            if matches(&(*la, *lb, *lv), &(*ra, *rb, *rv)) {
                *out.entry((*la, *lb, *lv, *ra, *rb, *rv)).or_default() += ld * rd;
            }
        }
    }
    out.retain(|_, diff| *diff != 0);
    out.into_iter().collect()
}

#[test]
fn composite_key_join_matches_full_recompute() {
    let left_epochs = [
        vec![(1, 1, 10, 1), (1, 2, 11, 1), (2, 1, 12, 1)],
        vec![(1, 2, 11, 1), (2, 1, 12, -1)],
        vec![(1, 1, 10, -1), (3, 3, 13, 1)],
    ];
    let right_epochs = [
        vec![(1, 1, 20, 1), (1, 2, 21, 1)],
        vec![(1, 2, 21, 1), (1, 2, 22, 1)],
        vec![(1, 1, 20, -1), (1, 2, 22, -1)],
    ];

    // Join on the two-column key (a, b) on both sides.
    let mut join = Join::new(&[0, 1], &[0, 1]);
    let (mut history_left, mut history_right) = (Vec::new(), Vec::new());
    let mut changelogs = Vec::new();
    for (left_batch, right_batch) in left_epochs.iter().zip(&right_epochs) {
        history_left.extend(left_batch.iter().copied());
        history_right.extend(right_batch.iter().copied());
        changelogs.push(join.apply(&pair_zset(left_batch), &pair_zset(right_batch)).unwrap());
    }

    let expected =
        recompute_with(&history_left, &history_right, |l, r| l.0 == r.0 && l.1 == r.1);
    assert!(!expected.is_empty());
    assert_eq!(consolidated_pairs(&changelogs), expected);
}

#[test]
fn mismatched_left_right_key_indices_match_recompute() {
    // Left key is column 0, right key is column 1: the positions differ, so a
    // positional match would be wrong.
    let left_epochs = [
        vec![(1, 5, 10, 1), (2, 6, 11, 1)],
        vec![(1, 5, 10, 1), (3, 7, 12, 1)],
        vec![(2, 6, 11, -1)],
    ];
    let right_epochs = [
        vec![(8, 1, 20, 1)],
        vec![(8, 1, 20, 1), (9, 2, 21, 1)],
        vec![(9, 2, 21, -1)],
    ];

    let mut join = Join::new(&[0], &[1]);
    let (mut history_left, mut history_right) = (Vec::new(), Vec::new());
    let mut changelogs = Vec::new();
    for (left_batch, right_batch) in left_epochs.iter().zip(&right_epochs) {
        history_left.extend(left_batch.iter().copied());
        history_right.extend(right_batch.iter().copied());
        changelogs.push(join.apply(&pair_zset(left_batch), &pair_zset(right_batch)).unwrap());
    }

    let expected = recompute_with(&history_left, &history_right, |l, r| l.0 == r.1);
    assert!(!expected.is_empty());
    assert_eq!(consolidated_pairs(&changelogs), expected);
}
