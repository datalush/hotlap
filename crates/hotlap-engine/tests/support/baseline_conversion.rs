//! Isolated Arrow batch conversion phases for the benchmark harness.

use std::collections::BTreeMap;

use crate::baseline_views::materialize_rows;
use crate::baseline_workloads::{batch, int_rows, metric, schema};

pub(super) fn run(n: usize) {
    let input_schema = schema(&["k", "v"]);
    let keys: Vec<i64> = (0..n).map(|i| (i % 16) as i64).collect();
    let values: Vec<i64> = (0..n).map(|i| i as i64).collect();
    let diffs = vec![1; n];
    let expected = keys
        .iter()
        .zip(&values)
        .map(|(&key, &value)| (vec![key, value], 1))
        .collect::<BTreeMap<_, _>>();
    let converted = metric("conversion", "rows_to_arrow_zset", n, n, 0, 0, || {
        batch(
            input_schema,
            vec![keys.clone(), values.clone()],
            diffs.clone(),
        )
    });
    assert_eq!(int_rows(&converted), expected);
    let materialized = metric("conversion", "arrow_zset_to_rows", n, n, 0, 0, || {
        materialize_rows(&converted)
    });
    assert_eq!(materialized, expected);
}
