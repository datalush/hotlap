use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_engine::ops::{filter, group_count, project};
use hotlap_engine::{KeyConverter, KeyedArrangement, ZSetBatch};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

/// Builds a Z-set of `(key, value, diff)` triples over the shared schema.
fn zset(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let keys: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let values: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let diffs: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(StringArray::from(values)),
    ];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn keys() -> KeyConverter {
    KeyConverter::new(schema().as_ref(), &[0]).unwrap()
}

/// Reads a two-column `(int, string)` Z-set as sorted `(int, string, diff)` rows.
fn int_str_rows(z: &ZSetBatch) -> Vec<(i64, String, i64)> {
    let ints = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let strings = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|i| (ints.value(i), strings.value(i).to_string(), diffs.value(i)))
        .collect()
}

/// Full-recompute oracle for `SELECT key, COUNT(*) GROUP BY key`: sums diffs by key.
fn recompute_counts(rows: &[(i64, &str, i64)]) -> Vec<(i64, i64)> {
    let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
    for (key, _value, diff) in rows {
        *counts.entry(*key).or_default() += diff;
    }
    counts.into_iter().filter(|(_, c)| *c != 0).collect()
}

/// Reads a `(key, count)` group-count Z-set as sorted pairs.
fn count_rows(z: &ZSetBatch) -> Vec<(i64, i64)> {
    let keys = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..z.len())
        .map(|i| (keys.value(i), counts.value(i)))
        .collect()
}

#[test]
fn filter_matches_full_recompute_with_retractions() {
    let input = zset(&[(1, "a", 1), (2, "b", 1), (3, "c", -1)]);
    let predicate = BooleanArray::from(vec![false, true, true]);

    let out = filter(&input, &predicate).unwrap();
    let mut got = int_str_rows(&out);
    got.sort();

    // Full recompute: keep only rows whose predicate holds, diffs unchanged.
    let expected = vec![(2, "b".to_string(), 1), (3, "c".to_string(), -1)];
    assert_eq!(got, expected);
}

#[test]
fn filter_rejects_predicate_of_wrong_length() {
    let input = zset(&[(1, "a", 1), (2, "b", 1)]);
    let predicate = BooleanArray::from(vec![true]);
    assert!(filter(&input, &predicate).is_err());
}

#[test]
fn project_selects_and_reorders_columns_keeping_diff() {
    let input = zset(&[(1, "a", 1), (2, "b", -1)]);

    let reversed = project(&input, &[1, 0]).unwrap();
    assert_eq!(reversed.schema().field(0).name(), "v");
    assert_eq!(reversed.schema().field(1).name(), "k");

    let values = reversed
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let keys = reversed
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = reversed.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut got: Vec<(String, i64, i64)> = (0..reversed.len())
        .map(|i| (values.value(i).to_string(), keys.value(i), diffs.value(i)))
        .collect();
    got.sort();
    assert_eq!(got, vec![("a".to_string(), 1, 1), ("b".to_string(), 2, -1)]);
}

#[test]
fn project_rejects_out_of_range_column() {
    let input = zset(&[(1, "a", 1)]);
    assert!(project(&input, &[3]).is_err());
}

#[test]
fn group_count_matches_recompute_across_epochs() {
    let mut arrangement = KeyedArrangement::new(schema(), &[0]).unwrap();
    let mut history: Vec<(i64, &str, i64)> = Vec::new();

    let epochs: Vec<Vec<(i64, &str, i64)>> = vec![
        vec![(1, "a", 1), (1, "b", 1), (2, "a", 1)],
        vec![(3, "c", 1), (2, "a", -1)],
        vec![(1, "a", -1)],
    ];

    for epoch in &epochs {
        arrangement.apply(&zset(epoch), &keys()).unwrap();
        history.extend(epoch.iter().copied());
        let out = group_count(&arrangement).unwrap();
        assert_eq!(count_rows(&out), recompute_counts(&history));
    }

    assert_eq!(
        count_rows(&group_count(&arrangement).unwrap()),
        vec![(1, 1), (3, 1)]
    );
}

#[test]
fn group_count_drops_key_crossing_to_zero() {
    let mut arrangement = KeyedArrangement::new(schema(), &[0]).unwrap();
    arrangement
        .apply(&zset(&[(1, "a", 1), (1, "b", 1)]), &keys())
        .unwrap();
    assert_eq!(
        count_rows(&group_count(&arrangement).unwrap()),
        vec![(1, 2)]
    );

    arrangement
        .retract(&zset(&[(1, "a", 1), (1, "b", 1)]), &keys())
        .unwrap();
    assert!(group_count(&arrangement).unwrap().is_empty());
}

#[test]
fn group_count_empty_arrangement_is_empty() {
    let arrangement = KeyedArrangement::new(schema(), &[0]).unwrap();
    let out = group_count(&arrangement).unwrap();
    assert!(out.is_empty());
    // Output still exposes the key schema plus a `count` column.
    assert_eq!(out.schema().field(1).name(), "count");
}
