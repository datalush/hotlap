use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_engine::consolidate;
use hotlap_engine::ops::{GroupCount, filter, project};
use hotlap_engine::{KeyConverter, KeyedArrangement, ZSetBatch};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

/// Builds a Z-set of `(key, value, diff)` triples over the shared schema.
fn zset(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn keys() -> KeyConverter {
    KeyConverter::new(schema().as_ref(), &[0]).unwrap()
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

fn diffs(z: &ZSetBatch) -> Vec<i64> {
    let array = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len()).map(|index| array.value(index)).collect()
}

/// Reads a `(key, count, diff)` Z-set as sorted `((key, count), diff)` rows.
fn count_diff_rows(z: &ZSetBatch) -> Vec<((i64, i64), i64)> {
    let (keys, counts, diffs) = (ints(z, 0), ints(z, 1), diffs(z));
    let mut out: Vec<((i64, i64), i64)> = (0..z.len())
        .map(|i| ((keys[i], counts[i]), diffs[i]))
        .collect();
    out.sort();
    out
}

/// Full-recompute oracle for `SELECT key, COUNT(*) GROUP BY key`: sums diffs by key.
fn recompute_counts(rows: &[(i64, &str, i64)]) -> Vec<(i64, i64)> {
    let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
    for (key, _value, diff) in rows {
        *counts.entry(*key).or_default() += diff;
    }
    counts.into_iter().filter(|(_, c)| *c != 0).collect()
}

/// Concatenates the changelogs into one Z-set and consolidates it to net rows.
fn consolidated(changelogs: &[ZSetBatch]) -> Vec<((i64, i64), i64)> {
    let schema = changelogs[0].schema();
    let batches: Vec<&RecordBatch> = changelogs.iter().map(|z| &z.batch).collect();
    let batch = concat_batches(&schema, batches).unwrap();
    let diffs: Vec<&dyn Array> = changelogs.iter().map(|z| z.diff.as_ref()).collect();
    let all = ZSetBatch::new(batch, concat(&diffs).unwrap()).unwrap();
    count_diff_rows(&consolidate(&all).unwrap())
}

#[test]
fn filter_matches_full_recompute_with_retractions() {
    let input = zset(&[(1, "a", 1), (2, "b", 1), (3, "c", -1)]);
    let predicate = BooleanArray::from(vec![false, true, true]);
    let out = filter(&input, &predicate).unwrap();
    assert_eq!(ints(&out, 0), vec![2, 3]);
    assert_eq!(strings(&out, 1), vec!["b".to_string(), "c".to_string()]);
    assert_eq!(diffs(&out), vec![1, -1]);
}

#[test]
fn filter_rejects_predicate_of_wrong_length() {
    let input = zset(&[(1, "a", 1), (2, "b", 1)]);
    assert!(filter(&input, &BooleanArray::from(vec![true])).is_err());
}

#[test]
fn project_selects_and_reorders_columns_keeping_diff() {
    let input = zset(&[(1, "a", 1), (2, "b", -1)]);

    let reversed = project(&input, &[1, 0]).unwrap();
    assert_eq!(reversed.schema().field(0).name(), "v");
    assert_eq!(reversed.schema().field(1).name(), "k");
    assert_eq!(
        strings(&reversed, 0),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(ints(&reversed, 1), vec![1, 2]);
    assert_eq!(diffs(&reversed), vec![1, -1]);
}

#[test]
fn project_rejects_out_of_range_column() {
    assert!(project(&zset(&[(1, "a", 1)]), &[3]).is_err());
}

#[test]
fn group_count_changelog_consolidates_to_final_counts() {
    let mut arrangement = KeyedArrangement::new(schema(), &[0]).unwrap();
    let mut reducer = GroupCount::new();
    let mut history: Vec<(i64, &str, i64)> = Vec::new();
    let mut changelogs: Vec<ZSetBatch> = Vec::new();

    let epochs: Vec<Vec<(i64, &str, i64)>> = vec![
        vec![(1, "a", 1), (1, "b", 1), (2, "a", 1)],
        vec![(3, "c", 1), (2, "a", -1)],
        vec![(1, "a", -1)],
    ];
    for epoch in &epochs {
        arrangement.apply(&zset(epoch), &keys()).unwrap();
        history.extend(epoch.iter().copied());
        changelogs.push(reducer.apply(&arrangement).unwrap());
    }

    // The concatenated changelog consolidates to exactly the final relation.
    assert_eq!(consolidated(&changelogs), vec![((1, 1), 1), ((3, 1), 1)]);
    let got: Vec<(i64, i64)> = consolidated(&changelogs)
        .iter()
        .map(|(kc, _)| *kc)
        .collect();
    assert_eq!(got, recompute_counts(&history));
    assert_eq!(got, vec![(1, 1), (3, 1)]);
}

#[test]
fn group_count_delta_retracts_old_and_inserts_new_count() {
    let mut arrangement = KeyedArrangement::new(schema(), &[0]).unwrap();
    let mut reducer = GroupCount::new();

    arrangement.apply(&zset(&[(1, "a", 1)]), &keys()).unwrap();
    let first = reducer.apply(&arrangement).unwrap();
    assert_eq!(count_diff_rows(&first), vec![((1, 1), 1)]);

    arrangement.apply(&zset(&[(1, "b", 1)]), &keys()).unwrap();
    let second = reducer.apply(&arrangement).unwrap();
    assert_eq!(count_diff_rows(&second), vec![((1, 1), -1), ((1, 2), 1)]);
}

#[test]
fn group_count_retracts_key_that_crosses_to_zero() {
    let mut arrangement = KeyedArrangement::new(schema(), &[0]).unwrap();
    arrangement
        .apply(&zset(&[(1, "a", 1), (1, "b", 1)]), &keys())
        .unwrap();
    let mut reducer = GroupCount::new();
    let first = reducer.apply(&arrangement).unwrap();
    assert_eq!(count_diff_rows(&first), vec![((1, 2), 1)]);

    arrangement
        .retract(&zset(&[(1, "a", 1), (1, "b", 1)]), &keys())
        .unwrap();
    let second = reducer.apply(&arrangement).unwrap();
    assert_eq!(count_diff_rows(&second), vec![((1, 2), -1)]);
    assert!(consolidated(&[first, second]).is_empty());
}

#[test]
fn group_count_empty_arrangement_is_empty() {
    let mut reducer = GroupCount::new();
    let out = reducer
        .apply(&KeyedArrangement::new(schema(), &[0]).unwrap())
        .unwrap();
    assert!(out.is_empty());
    assert_eq!(out.schema().field(1).name(), "count");
}
