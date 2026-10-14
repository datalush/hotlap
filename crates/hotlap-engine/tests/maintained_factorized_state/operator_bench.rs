use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::i256;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::plan::AggSpec;
use hotlap_engine::ZSetBatch;
use hotlap_engine::ops::{GroupAggregate, Join};

use crate::join::{JoinGate, JoinView};

const STEPS: usize = 128;
const WARMUP: usize = 1;
const TRIALS: usize = 5;

#[derive(Clone, Copy)]
struct Case {
    keys: usize,
    fanout: usize,
    changed_keys: usize,
}

#[derive(Debug, serde::Serialize)]
struct Measurement {
    case: String,
    maintained_median_ns: u128,
    dense_median_ns: u128,
    ratio: f64,
    maintained_certified_updates: usize,
    output_rows: usize,
    memory: &'static str,
    caveats: &'static str,
}

fn input_schema(left: bool) -> std::sync::Arc<Schema> {
    let mut fields = vec![Field::new("key", DataType::Int64, false)];
    if left {
        fields.push(Field::new("group", DataType::Int64, false));
        fields.push(Field::new("payload", DataType::Int64, false));
    }
    fields.push(Field::new("value", DataType::Int64, true));
    std::sync::Arc::new(Schema::new(fields))
}

fn batch(left: bool, changes: &[(usize, usize, Option<i64>, i64)]) -> ZSetBatch {
    let keys = changes.iter().map(|x| x.0 as i64).collect::<Vec<_>>();
    let groups = changes.iter().map(|x| (x.0 % 8) as i64).collect::<Vec<_>>();
    let payloads = changes.iter().map(|x| x.1 as i64).collect::<Vec<_>>();
    let values = changes.iter().map(|x| x.2).collect::<Vec<_>>();
    let diffs = changes.iter().map(|x| x.3).collect::<Vec<_>>();
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys))];
    if left {
        columns.push(Arc::new(Int64Array::from(groups)));
        columns.push(Arc::new(Int64Array::from(payloads)));
    }
    columns.push(Arc::new(Int64Array::from(values)));
    ZSetBatch::new(
        RecordBatch::try_new(input_schema(left), columns).unwrap(),
        Arc::new(Int64Array::from(diffs)),
    )
    .unwrap()
}

fn initial(case: Case, left: bool) -> ZSetBatch {
    let rows = (0..case.keys)
        .flat_map(|key| {
            (0..(case.fanout + usize::from(left) * 4)).map(move |row| {
                let value = if row % 7 != 0 {
                    Some(((key * 17 + row) as i64).max(0))
                } else {
                    Some(0)
                };
                let tag = if left && row >= case.fanout {
                    case.fanout + row - case.fanout
                } else {
                    row
                };
                (key, tag, value, 1)
            })
        })
        .collect::<Vec<_>>();
    batch(left, &rows)
}

fn seed_view(view: &mut JoinView, case: Case) {
    super::join::set_group_mapping((0..case.keys).map(|key| (key as i64, (key % 8) as i64)));
    view.update_batch(&initial(case, true), &initial(case, false))
        .unwrap();
}

fn oracle_state(case: Case) -> (Join, GroupAggregate) {
    let mut group = GroupAggregate::new(
        &[1],
        vec![
            AggSpec::count(),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(3),
            },
            AggSpec::sum(3),
        ],
    );
    let mut seeded_join = Join::new(&[0], &[0]);
    let joined = seeded_join
        .apply(&initial(case, true), &initial(case, false))
        .unwrap();
    group.apply(&joined).unwrap();
    (seeded_join, group)
}

fn dense_step(join: &mut Join, group: &mut GroupAggregate, delta: &ZSetBatch) -> ZSetBatch {
    let empty_right = batch(false, &[]);
    let joined = join.apply(delta, &empty_right).unwrap();
    group.apply(&joined).unwrap()
}

fn dense_seed(case: Case) -> (Join, GroupAggregate) {
    oracle_state(case)
}

fn snapshot(view: &JoinView) -> Vec<(i64, i64, i64, Option<i64>)> {
    let mut rows = view
        .by_group
        .iter()
        .map(|(&group, summary)| {
            let count = summary.count.to_string().parse::<i64>().unwrap();
            let left_count = summary.left_nonnull.to_string().parse::<i64>().unwrap();
            let left_sum =
                (left_count != 0).then(|| summary.left_sum.to_string().parse::<i64>().unwrap());
            (group, count, left_count, left_sum)
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| row.0);
    rows
}

fn dense_snapshot(batch: &ZSetBatch) -> Vec<(i64, i64, i64, Option<i64>)> {
    use arrow::array::Array;
    let keys = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let count = batch
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left_count = batch
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left_sum = batch
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows = (0..batch.len())
        .map(|i| {
            (
                keys.value(i),
                count.value(i),
                left_count.value(i),
                (!left_sum.is_null(i)).then(|| left_sum.value(i)),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| row.0);
    rows
}

fn exact_snapshot_matches(
    actual: &[(i64, i64, i64, Option<i64>)],
    expected: &[(i64, i64, i64, Option<i64>)],
) -> bool {
    let mut actual = actual.to_vec();
    let mut expected = expected.to_vec();
    actual.sort_unstable_by_key(|row| row.0);
    expected.sort_unstable_by_key(|row| row.0);
    actual == expected
}

#[test]
fn fpzb_bench_oracle_rejects_missing_or_extra_groups() {
    let full = vec![(1, 3, 2, Some(0)), (2, 4, 4, None)];
    assert!(!exact_snapshot_matches(&full[..1], &full));
    assert!(!exact_snapshot_matches(
        &[(1, 3, 2, Some(0)), (2, 4, 4, Some(0))],
        &full
    ));
    assert!(!exact_snapshot_matches(
        &[(1, 3, 2, Some(0)), (2, 4, 4, None), (3, 1, 1, None)],
        &full
    ));
}

fn maintained_step(view: &mut JoinView, delta: &ZSetBatch) -> usize {
    view.update_batch(delta, &batch(false, &[])).unwrap();
    assert_eq!(
        view.gate,
        JoinGate::Certified,
        "gate failed for delta len {}, summary={:?}",
        delta.len(),
        view.by_group.values().next()
    );
    for (&group, summary) in &view.by_group {
        assert!(summary.count >= i256::ZERO);
        assert!(summary.count <= i256::from_i128(i128::from(i64::MAX)));
        assert!(summary.left_nonnull >= i256::ZERO);
        assert!(summary.right_nonnull >= i256::ZERO);
        assert!(i64::try_from(summary.left_sum.to_string().parse::<i128>().unwrap()).is_ok());
        let _ = group;
    }
    let mut rows = Vec::new();
    for (&group, summary) in &view.by_group {
        rows.push((
            group,
            summary.count.to_string().parse::<i64>().unwrap(),
            summary.left_nonnull.to_string().parse::<i64>().unwrap(),
            summary.left_sum.to_string().parse::<i64>().unwrap_or(0),
        ));
    }
    black_box(rows.len())
}

fn median(mut values: Vec<u128>) -> u128 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn run_case(case: Case) -> Measurement {
    let mut dense_ns = Vec::new();
    let mut maintained_ns = Vec::new();
    let mut certified = 0;
    let mut output_rows = 0;
    for trial in 0..(WARMUP + TRIALS) {
        // Both implementations receive identical, prebuilt Arrow updates; all state seeding is untimed.
        let mut deltas = Vec::with_capacity(STEPS);
        let mut next_tags = vec![case.fanout + 4; case.keys];
        for step in 0..STEPS {
            let start = (step * case.changed_keys) % case.keys;
            let rows = (0..case.changed_keys)
                .map(|i| {
                    let key = (start + i) % case.keys;
                    let tag = next_tags[key];
                    next_tags[key] += 1;
                    (key, tag, Some(10 + (step % 17) as i64), 1)
                })
                .collect::<Vec<_>>();
            deltas.push(batch(true, &rows));
        }
        let mut view = JoinView::default();
        seed_view(&mut view, case);
        let (mut join, mut group) = oracle_state(case);
        let start = Instant::now();
        for delta in &deltas {
            let _ = dense_step(&mut join, &mut group, delta);
        }
        let dense_elapsed = start.elapsed().as_nanos() / STEPS as u128;

        let start = Instant::now();
        for delta in &deltas {
            output_rows += maintained_step(&mut view, delta);
            certified += 1;
        }
        let maintained_elapsed = start.elapsed().as_nanos() / STEPS as u128;
        // Fresh-state differential oracle is outside both timed sections.
        let mut check_view = JoinView::default();
        seed_view(&mut check_view, case);
        let (mut check_join, mut check_group) = dense_seed(case);
        let initial_join = Join::new(&[0], &[0]);
        let mut initial_group = GroupAggregate::new(
            &[1],
            vec![
                AggSpec::count(),
                AggSpec {
                    func: hotlap_core::plan::AggFunc::Count,
                    input: Some(3),
                },
                AggSpec::sum(3),
            ],
        );
        let initial_joined = {
            let mut seeded = Join::new(&[0], &[0]);
            seeded
                .apply(&initial(case, true), &initial(case, false))
                .unwrap()
        };
        let initial_rows = initial_group.apply(&initial_joined).unwrap();
        let old_snapshot = dense_snapshot(&initial_rows);
        let mut aggregate_state = std::collections::HashMap::<i64, (i64, i64, Option<i64>)>::new();
        for (key, count, nonnull, sum) in &old_snapshot {
            aggregate_state.insert(*key, (*count, *nonnull, *sum));
        }
        let _ = initial_join;
        for delta in &deltas {
            check_view.update_batch(delta, &batch(false, &[])).unwrap();
            assert_eq!(check_view.gate, JoinGate::Certified);
            let dense_delta = dense_step(&mut check_join, &mut check_group, delta);
            let dense_rows = dense_snapshot(&dense_delta);
            for (key, count, nonnull, sum) in dense_rows {
                if count == 0 {
                    aggregate_state.remove(&key);
                } else {
                    aggregate_state.insert(key, (count, nonnull, sum));
                }
            }
            let mut expected = aggregate_state
                .iter()
                .map(|(&key, &(count, nonnull, sum))| (key, count, nonnull, sum))
                .collect::<Vec<_>>();
            expected.sort_unstable_by_key(|row| row.0);
            let current = snapshot(&check_view);
            assert!(
                exact_snapshot_matches(&current, &expected),
                "full group state mismatch after changelog update"
            );
        }
        if trial > 0 {
            dense_ns.push(dense_elapsed);
            maintained_ns.push(maintained_elapsed);
        }
    }
    let dense = median(dense_ns);
    let maintained = median(maintained_ns);
    Measurement {
        case: format!(
            "keys={} fanout={} changed_keys={}",
            case.keys, case.fanout, case.changed_keys
        ),
        maintained_median_ns: maintained,
        dense_median_ns: dense,
        ratio: maintained as f64 / dense as f64,
        maintained_certified_updates: certified,
        output_rows,
        memory: "NOT_MEASURED",
        caveats: "toy HashMap row identity vs Arrow canonical arrangements; prototype emits scalar snapshot rows, not Arrow changelog; dense output materialization costs are included; not a causal whole-kernel comparison",
    }
}

#[test]
#[ignore = "exploratory paired operator measurement; release-only opt-in"]
fn fpzb_exploratory_paired_operator_measurement() {
    let cases = [
        Case {
            keys: 128,
            fanout: 4,
            changed_keys: 1,
        },
        Case {
            keys: 128,
            fanout: 16,
            changed_keys: 1,
        },
        Case {
            keys: 128,
            fanout: 64,
            changed_keys: 1,
        },
        Case {
            keys: 128,
            fanout: 256,
            changed_keys: 1,
        },
        Case {
            keys: 128,
            fanout: 4,
            changed_keys: 16,
        },
        Case {
            keys: 128,
            fanout: 16,
            changed_keys: 16,
        },
        Case {
            keys: 128,
            fanout: 64,
            changed_keys: 16,
        },
        Case {
            keys: 128,
            fanout: 256,
            changed_keys: 16,
        },
    ];
    let results = cases.into_iter().map(run_case).collect::<Vec<_>>();
    println!(
        "FPZB_EXPLORATORY {}",
        serde_json::to_string_pretty(&results).unwrap()
    );
}
