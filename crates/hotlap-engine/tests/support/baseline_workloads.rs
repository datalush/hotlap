//! Join, aggregate, and filter/project benchmark workloads.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

use crate::baseline_alloc::{self, Stats};

const SEED: u64 = 7;
static REP: AtomicUsize = AtomicUsize::new(0);
static REPEATS: AtomicUsize = AtomicUsize::new(0);
const PROFILE: &str = if cfg!(debug_assertions) {
    "debug"
} else {
    "release"
};

pub(super) fn schema(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn batch(schema: SchemaRef, columns: Vec<Vec<i64>>, diffs: Vec<i64>) -> ZSetBatch {
    let arrays = columns
        .into_iter()
        .map(|column| Arc::new(Int64Array::from(column)) as ArrayRef)
        .collect();
    ZSetBatch::new(
        RecordBatch::try_new(schema, arrays).unwrap(),
        Arc::new(Int64Array::from(diffs)),
    )
    .unwrap()
}

fn rows_batch(rows: &[(i64, i64, i64, i64)], input_schema: SchemaRef) -> ZSetBatch {
    batch(
        input_schema,
        vec![
            rows.iter().map(|r| r.0).collect(),
            rows.iter().map(|r| r.1).collect(),
            rows.iter().map(|r| r.2).collect(),
        ],
        rows.iter().map(|r| r.3).collect(),
    )
}

pub(super) fn metric<T>(
    case: &str,
    phase: &str,
    input_rows: usize,
    delta_rows: usize,
    setup_ns: u128,
    fixture_setup_ns: u128,
    operation: impl FnOnce() -> T,
) -> T {
    let (head, before) = baseline_alloc::capture_baseline(|| {
        std::env::var("HOTLAP_BASELINE_HEAD").unwrap_or_else(|_| "unknown".into())
    });
    let start = Instant::now();
    let output = operation();
    let elapsed_ns = start.elapsed().as_nanos();
    let after = Stats::read();
    println!();
    println!(
        r#"{{"case":"{case}","phase":"{phase}","seed":{SEED},"rep":{},"repeats":{},"head":"{head}","profile":"{PROFILE}","arch":"{}","os":"{}","input_rows":{input_rows},"delta_rows":{delta_rows},"setup_ns":{setup_ns},"fixture_setup_ns":{fixture_setup_ns},"setup_timing_semantics":"measured only when explicitly populated; zero denotes not measured, setup excluded from elapsed interval","elapsed_ns":{elapsed_ns},"elapsed_semantics":"wall_clock_not_cpu","allocated_live_bytes_before":{before},"peak_allocated_live_bytes":{},"allocated_live_bytes_after":{},"logical_retained_bytes":null,"logical_retained_bytes_status":"unavailable"}}"#,
        REP.load(Ordering::Relaxed),
        REPEATS.load(Ordering::Relaxed),
        std::env::consts::ARCH,
        std::env::consts::OS,
        after.peak,
        after.live
    );
    output
}

fn nrows() -> usize {
    std::env::var("HOTLAP_BASELINE_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(128)
}

pub(super) fn rep() -> usize {
    REP.load(Ordering::Relaxed)
}

pub(super) fn repeats() -> usize {
    REPEATS.load(Ordering::Relaxed)
}

fn join_workload(n: usize, case: &str, skewed: bool, duplicates: bool) {
    let setup_start = Instant::now();
    let input_schema = schema(&["k", "v", "ts"]);
    let mut core = EngineCore::new();
    for id in [InputId(0), InputId(1)] {
        core.register_input(id).unwrap();
    }
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
    let memory = core.memory_report().unwrap();
    assert!(!memory.complete);
    assert!(memory.covered_categories.contains(&"joins".to_string()));
    assert!(!memory.unknown_categories.contains(&"joins".to_string()));
    let setup_ns = setup_start.elapsed().as_nanos();
    let fixture_start = Instant::now();
    let left: Vec<_> = (0..n)
        .map(|i| {
            let key = if skewed {
                i64::from(i % 8 == 0)
            } else {
                (i % 8) as i64
            };
            let value = if duplicates { (i / 4) as i64 } else { i as i64 };
            (key, value, 0, 1)
        })
        .collect();
    let mut right: Vec<_> = (0..n)
        .map(|i| {
            let key = if skewed {
                i64::from(i % 8 == 0)
            } else {
                (i % 8) as i64
            };
            let value = if duplicates {
                ((i + n) / 4) as i64
            } else {
                (i + n) as i64
            };
            (key, value, 0, 1)
        })
        .collect();
    let left_batch = rows_batch(&left, input_schema.clone());
    let right_batch = rows_batch(&right, input_schema.clone());
    let insert = [(0, (n * 2) as i64, 0, 1)];
    let retract = [(0, (n * 2) as i64, 0, -1)];
    let insert_batch = rows_batch(&insert, input_schema.clone());
    let retract_batch = rows_batch(&retract, input_schema);
    let fixture_setup_ns = fixture_start.elapsed().as_nanos();
    core.push(InputId(0), &left_batch).unwrap();
    assert!(int_rows(&core.take_changes(ViewId(0)).unwrap()).is_empty());
    core.push(InputId(1), &right_batch).unwrap();
    let seeded_output = join_oracle(&left, &right);
    assert_eq!(
        int_rows(&core.take_changes(ViewId(0)).unwrap()),
        seeded_output
    );
    assert_eq!(int_rows(&core.snapshot(ViewId(0)).unwrap()), seeded_output);
    for (phase, update, history) in [
        ("insert", &insert_batch, insert[0]),
        ("retract", &retract_batch, retract[0]),
    ] {
        let before = join_oracle(&left, &right);
        metric(case, phase, n * 2, 1, setup_ns, fixture_setup_ns, || {
            core.push(InputId(1), update).unwrap();
        });
        right.push(history);
        let after = join_oracle(&left, &right);
        assert_eq!(
            int_rows(&core.take_changes(ViewId(0)).unwrap()),
            delta_rows(&after, &before),
            "{case} {phase} changelog"
        );
        assert_eq!(
            int_rows(&core.snapshot(ViewId(0)).unwrap()),
            after,
            "{case} {phase} state"
        );
    }
}

fn join_oracle(
    left: &[(i64, i64, i64, i64)],
    right: &[(i64, i64, i64, i64)],
) -> BTreeMap<Vec<i64>, i64> {
    let mut rows = BTreeMap::new();
    for l in left {
        for r in right {
            if l.0 == r.0 {
                *rows.entry(vec![l.0, l.1, l.2, r.0, r.1, r.2]).or_default() += l.3 * r.3;
            }
        }
    }
    rows.retain(|_, diff| *diff != 0);
    rows
}

fn delta_rows(
    after: &BTreeMap<Vec<i64>, i64>,
    before: &BTreeMap<Vec<i64>, i64>,
) -> BTreeMap<Vec<i64>, i64> {
    let mut delta = after.clone();
    for (row, diff) in before {
        *delta.entry(row.clone()).or_default() -= diff;
    }
    delta.retain(|_, diff| *diff != 0);
    delta
}

pub(super) fn int_rows(zset: &ZSetBatch) -> BTreeMap<Vec<i64>, i64> {
    let mut rows = BTreeMap::new();
    for row in 0..zset.len() {
        let values = zset
            .batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row)
            })
            .collect::<Vec<_>>();
        let diff = zset
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row);
        *rows.entry(values).or_default() += diff;
    }
    rows.retain(|_, diff| *diff != 0);
    rows
}

fn aggregate_workload(n: usize) {
    let setup_start = Instant::now();
    let input_schema = schema(&["k", "v", "ts"]);
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![
                AggSpec::count(),
                AggSpec::sum(1),
                AggSpec::avg(1),
                AggSpec::min(1),
                AggSpec::max(1),
            ],
        },
    )
    .unwrap();
    let setup_ns = setup_start.elapsed().as_nanos();
    let fixture_start = Instant::now();
    let values: Vec<i64> = (0..n).map(|i| ((i + SEED as usize) % n) as i64).collect();
    let seed = batch(
        input_schema.clone(),
        vec![vec![1; n], values.clone(), vec![0; n]],
        vec![1; n],
    );
    let max_index = values
        .iter()
        .enumerate()
        .max_by_key(|(_, value)| *value)
        .unwrap()
        .0;
    let min_index = values
        .iter()
        .enumerate()
        .min_by_key(|(_, value)| *value)
        .unwrap()
        .0;
    let retract_max = batch(
        input_schema.clone(),
        vec![vec![1], vec![values[max_index]], vec![0]],
        vec![-1],
    );
    let retract_min = batch(
        input_schema,
        vec![vec![1], vec![values[min_index]], vec![0]],
        vec![-1],
    );
    let fixture_setup_ns = fixture_start.elapsed().as_nanos();
    metric(
        "aggregates_count_sum_avg_min_max",
        "seed",
        n,
        n,
        setup_ns,
        fixture_setup_ns,
        || {
            core.push(InputId(0), &seed).unwrap();
        },
    );
    assert_aggregates(&core.snapshot(ViewId(0)).unwrap(), &values);
    metric(
        "aggregates_count_sum_avg_min_max",
        "retract_current_max",
        n,
        1,
        setup_ns,
        fixture_setup_ns,
        || {
            core.push(InputId(0), &retract_max).unwrap();
        },
    );
    let after_max = values
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != max_index)
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    assert_aggregates(&core.snapshot(ViewId(0)).unwrap(), &after_max);
    metric(
        "aggregates_count_sum_avg_min_max",
        "retract_current_min",
        n,
        1,
        setup_ns,
        fixture_setup_ns,
        || {
            core.push(InputId(0), &retract_min).unwrap();
        },
    );
    let after_extremes = values
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != max_index && *index != min_index)
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    assert_aggregates(&core.snapshot(ViewId(0)).unwrap(), &after_extremes);
}

fn assert_aggregates(snapshot: &ZSetBatch, values: &[i64]) {
    assert_eq!(snapshot.len(), 1);
    let sum: i64 = values.iter().sum();
    assert_eq!(
        snapshot
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        values.len() as i64
    );
    assert_eq!(
        snapshot
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        sum
    );
    assert_eq!(
        snapshot
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        sum as f64 / values.len() as f64
    );
    assert_eq!(
        snapshot
            .batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        *values.iter().min().unwrap()
    );
    assert_eq!(
        snapshot
            .batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        *values.iter().max().unwrap()
    );
}

fn filter_project_workload(n: usize) {
    let setup_start = Instant::now();
    let input_schema = schema(&["k", "v", "ts"]);
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Project {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::Source(InputId(0))),
                pred: hotlap_core::Predicate::Cmp {
                    op: hotlap_core::CmpOp::Ge,
                    col: 1,
                    scalar: hotlap_core::Scalar::I64(50),
                },
            }),
            cols: vec![1, 0],
        },
    )
    .unwrap();
    let setup_ns = setup_start.elapsed().as_nanos();
    let fixture_start = Instant::now();
    let rows: Vec<_> = (0..n)
        .map(|i| (i as i64, ((i * 17 + SEED as usize) % 101) as i64, 0, 1))
        .collect();
    let seed = rows_batch(&rows, input_schema.clone());
    let retract_index = filter_retract_index(&rows);
    let (key, value, _, _) = rows[retract_index];
    let retract = rows_batch(&[(key, value, 0, -1)], input_schema);
    let fixture_setup_ns = fixture_start.elapsed().as_nanos();
    metric(
        "filter_project",
        "seed",
        n,
        n,
        setup_ns,
        fixture_setup_ns,
        || {
            core.push(InputId(0), &seed).unwrap();
        },
    );
    let expected = rows.iter().filter(|row| row.1 >= 50).count();
    let expected_rows = rows
        .iter()
        .filter(|row| row.1 >= 50)
        .map(|row| (vec![row.1, row.0], 1))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(int_rows(&core.snapshot(ViewId(0)).unwrap()), expected_rows);
    metric(
        "filter_project",
        if value >= 50 {
            "retract_selected"
        } else {
            "retract_filtered_out"
        },
        n,
        1,
        setup_ns,
        fixture_setup_ns,
        || {
            core.push(InputId(0), &retract).unwrap();
        },
    );
    let expected_after = rows
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != retract_index)
        .map(|(_, row)| row)
        .filter(|row| row.1 >= 50)
        .map(|row| (vec![row.1, row.0], 1))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(int_rows(&core.snapshot(ViewId(0)).unwrap()), expected_after);
    assert_eq!(expected_after.len(), expected - usize::from(value >= 50));
}

pub(super) fn filter_retract_index(rows: &[(i64, i64, i64, i64)]) -> usize {
    rows.iter().position(|row| row.1 >= 50).unwrap_or(0)
}

pub fn run() {
    let rows = nrows();
    let repeats = std::env::var("HOTLAP_BASELINE_REPEATS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3usize);
    assert!((3..=512).contains(&rows));
    assert!((1..=10).contains(&repeats));
    REPEATS.store(repeats, Ordering::Relaxed);
    for rep in 0..repeats {
        REP.store(rep, Ordering::Relaxed);
        join_workload(rows, "join_uniform_fanout_unique", false, false);
        join_workload(rows, "join_skewed_fanout_duplicates", true, true);
        aggregate_workload(rows);
        filter_project_workload(rows);
        crate::baseline_conversion::run(rows);
        crate::baseline_views::run(rows);
        crate::baseline_memory::run(rows);
        crate::baseline_lifecycle::window_workload(rows);
        crate::baseline_lifecycle::recovery_multiview_workload(rows);
    }
}
