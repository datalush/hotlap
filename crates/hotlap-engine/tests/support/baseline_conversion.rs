//! Isolated Arrow batch conversion phases for the benchmark harness.

use std::collections::BTreeMap;

use arrow::array::{Array, Int64Array};
use hotlap_core::ZSetBatch;

use crate::baseline_workloads::{batch, metric, schema};

pub(super) fn run(n: usize) {
    let input_schema = schema(&["k", "v"]);
    let fixture = vec![
        (1, 10, 2),
        (1, 10, -1),
        (1, 10, 1),
        (2, 20, -3),
        (2, 20, 1),
        (3, 30, 5),
        (3, 30, -5),
        (4, 40, 0),
    ];
    let expected = BTreeMap::from([(vec![1, 10], 2), (vec![2, 20], -2)]);
    let setup_started = std::time::Instant::now();
    let input = row_records_to_zset(input_schema, &fixture);
    let fixture_setup_ns = setup_started.elapsed().as_nanos();
    let distinct = fixture
        .iter()
        .map(|row| (row.0, row.1))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let converted = metric(
        "zset_arrow_input_records",
        "arrow_to_signed_rows",
        fixture.len(),
        distinct,
        0,
        fixture_setup_ns,
        || zset_records(&input),
    );
    let actual = consolidate_records(converted);
    assert_eq!(actual, expected);
    assert_eq!(
        input.len(),
        fixture.len(),
        "input Arrow batch preserves source records"
    );
    assert_eq!(distinct, 4);
    let _ = n;
}

pub(super) fn assert_signed_conversion_fixture() {
    let input_schema = schema(&["k", "v"]);
    let fixture = vec![
        (1, 10, 2),
        (1, 10, -1),
        (1, 10, 1),
        (2, 20, -3),
        (2, 20, 1),
        (3, 30, 5),
        (3, 30, -5),
        (4, 40, 0),
    ];
    let input = row_records_to_zset(input_schema, &fixture);
    let expected = BTreeMap::from([(vec![1, 10], 2), (vec![2, 20], -2)]);
    assert_eq!(input.len(), 8, "raw Arrow zset preserves source records");
    let raw = zset_records(&input);
    assert!(
        raw.contains(&(vec![4, 40], 0)),
        "zero-weight source record is retained by this batch representation"
    );
    assert_eq!(consolidate_records(zset_records(&input)), expected);
    assert!(
        std::panic::catch_unwind(|| {
            consolidate_records(vec![(vec![9, 90], i64::MAX), (vec![9, 90], 1)])
        })
        .is_err()
    );
}

fn consolidate_records(records: Vec<(Vec<i64>, i64)>) -> BTreeMap<Vec<i64>, i64> {
    let mut consolidated = BTreeMap::new();
    for (row, diff) in records {
        let weight = consolidated.entry(row.clone()).or_insert(0i64);
        *weight = weight
            .checked_add(diff)
            .expect("oracle multiplicity overflow");
        if *weight == 0 {
            consolidated.remove(&row);
        }
    }
    consolidated
}

fn row_records_to_zset(
    schema: arrow::datatypes::SchemaRef,
    records: &[(i64, i64, i64)],
) -> ZSetBatch {
    batch(
        schema,
        vec![
            records.iter().map(|row| row.0).collect(),
            records.iter().map(|row| row.1).collect(),
        ],
        records.iter().map(|row| row.2).collect(),
    )
}

fn zset_records(zset: &ZSetBatch) -> Vec<(Vec<i64>, i64)> {
    let columns: Vec<_> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let diffs = zset.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..zset.len())
        .map(|row| {
            (
                columns.iter().map(|column| column.value(row)).collect(),
                diffs.value(row),
            )
        })
        .collect()
}

#[cfg(test)]
mod test_oracle_sensitivity {
    use super::consolidate_records;
    use std::collections::BTreeMap;

    #[test]
    #[should_panic]
    fn incorrect_expected_weight_is_detected() {
        let actual = consolidate_records(vec![(vec![1, 10], 2), (vec![1, 10], -1)]);
        assert_eq!(actual, BTreeMap::from([(vec![1, 10], 0)]));
    }
}
