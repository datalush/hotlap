//! Bounded current-path join cost fixture: skew, update batch size, inserts,
//! and exact retractions. The independent weighted nested-loop oracle runs
//! outside the timed EngineCore::push region.

#[cfg(feature = "bench-allocation-observer")]
#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

type InputRow = (i64, i64, i64);
type OutputRow = (i64, i64, i64, i64);
type WeightedRows<K> = BTreeMap<K, i64>;

struct PushCheck<'a> {
    output_before: &'a WeightedRows<OutputRow>,
    full_after: &'a WeightedRows<OutputRow>,
    accumulated: &'a mut WeightedRows<OutputRow>,
    case_id: &'a str,
    step: &'a str,
    rows_per_hot_key: usize,
    batch_size: usize,
    operation: &'a str,
}

const LEFT: InputId = InputId(0);
const RIGHT: InputId = InputId(1);
const VIEW: ViewId = ViewId(0);
const SEED_ROWS_PER_SIDE: usize = 128;

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]))
}

fn input_batch(rows: &[InputRow]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(input_schema(), columns).unwrap();
    let diffs = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn seed_rows(hot_key_rows: usize) -> Vec<InputRow> {
    if hot_key_rows == 1 {
        (0..SEED_ROWS_PER_SIDE)
            .map(|payload| (payload as i64, payload as i64, 1))
            .collect()
    } else {
        (0..2)
            .flat_map(|key| {
                (0..hot_key_rows)
                    .map(move |payload| (key as i64, (key * hot_key_rows + payload) as i64, 1))
            })
            .collect()
    }
}

fn change_batch(
    seed: &[InputRow],
    hot_key_rows: usize,
    batch_size: usize,
    side: i64,
    insert: bool,
) -> Vec<InputRow> {
    if insert {
        (0..batch_size)
            .map(|index| {
                let key = if hot_key_rows == 1 {
                    index as i64
                } else {
                    (index % 2) as i64
                };
                let payload = 10_000 + side * 1_000 + index as i64;
                (key, payload, 1)
            })
            .collect()
    } else {
        if hot_key_rows == 1 {
            seed.iter()
                .take(batch_size)
                .map(|&(key, payload, _)| (key, payload, -1))
                .collect()
        } else {
            (0..batch_size)
                .map(|index| {
                    let key = index % 2;
                    let payload = key * hot_key_rows + index / 2;
                    (key as i64, payload as i64, -1)
                })
                .collect()
        }
    }
}

fn apply_input(state: &mut Vec<InputRow>, changes: &[InputRow]) {
    for &(key, payload, diff) in changes {
        let current = state
            .iter()
            .find(|row| row.0 == key && row.1 == payload)
            .map_or(0, |row| row.2);
        let next = current.checked_add(diff).expect("source weight overflow");
        state.retain(|row| row.0 != key || row.1 != payload);
        if next != 0 {
            state.push((key, payload, next));
        }
    }
}

fn source_weights(rows: &[InputRow]) -> WeightedRows<(i64, i64)> {
    rows.iter()
        .map(|&(key, payload, diff)| ((key, payload), diff))
        .collect()
}

/// Independent reference: nested-loop equality join with signed products.
fn full_join(left: &[InputRow], right: &[InputRow]) -> WeightedRows<OutputRow> {
    let mut output = WeightedRows::new();
    for &(left_key, left_payload, left_diff) in left {
        for &(right_key, right_payload, right_diff) in right {
            if left_key == right_key {
                let row = (left_key, left_payload, right_key, right_payload);
                let weight = output.entry(row).or_default();
                *weight = weight
                    .checked_add(left_diff.checked_mul(right_diff).unwrap())
                    .unwrap();
            }
        }
    }
    output.retain(|_, weight| *weight != 0);
    output
}

fn engine_rows(batch: &ZSetBatch) -> WeightedRows<OutputRow> {
    if batch.is_empty() {
        return WeightedRows::new();
    }
    let arrays = (0..4)
        .map(|column| {
            batch
                .batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let diffs = batch.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut output = WeightedRows::new();
    for index in 0..batch.len() {
        let row = (
            arrays[0].value(index),
            arrays[1].value(index),
            arrays[2].value(index),
            arrays[3].value(index),
        );
        *output.entry(row).or_default() += diffs.value(index);
    }
    output.retain(|_, weight| *weight != 0);
    output
}

fn map_delta(
    next: &WeightedRows<OutputRow>,
    previous: &WeightedRows<OutputRow>,
) -> WeightedRows<OutputRow> {
    let mut delta = next.clone();
    for (&row, &weight) in previous {
        *delta.entry(row).or_default() -= weight;
    }
    delta.retain(|_, weight| *weight != 0);
    delta
}

fn add_rows(target: &mut WeightedRows<OutputRow>, changes: &WeightedRows<OutputRow>) {
    for (&row, &weight) in changes {
        *target.entry(row).or_default() += weight;
    }
    target.retain(|_, weight| *weight != 0);
}

fn abs_weight(rows: &WeightedRows<OutputRow>) -> i64 {
    rows.values().map(|weight| weight.abs()).sum()
}

fn push_and_check(core: &mut EngineCore, input: InputId, batch: &ZSetBatch, check: PushCheck<'_>) {
    #[cfg(feature = "bench-allocation-observer")]
    let live_before = baseline_alloc::Stats::read().live;
    #[cfg(feature = "bench-allocation-observer")]
    baseline_alloc::reset_peak();

    let started = Instant::now();
    #[cfg(feature = "bench-allocation-observer")]
    let allocation_events = baseline_alloc::count_events(|| core.push(input, batch).unwrap());
    #[cfg(not(feature = "bench-allocation-observer"))]
    core.push(input, batch).unwrap();
    let elapsed_ns = started.elapsed().as_nanos();

    #[cfg(feature = "bench-allocation-observer")]
    let stats = baseline_alloc::Stats::read();

    // All oracle, snapshot, drain, and serialization work is outside push timing.
    let expected_delta = map_delta(check.full_after, check.output_before);
    let actual_delta = engine_rows(&core.take_changes(VIEW).unwrap());
    assert_eq!(
        actual_delta, expected_delta,
        "{}/{}/delta",
        check.case_id, check.step
    );
    add_rows(check.accumulated, &actual_delta);
    assert_eq!(
        *check.accumulated, *check.full_after,
        "{}/{}/accumulated delta",
        check.case_id, check.step
    );
    assert_eq!(
        engine_rows(&core.snapshot(VIEW).unwrap()),
        *check.full_after,
        "{}/{}/snapshot",
        check.case_id,
        check.step
    );

    #[allow(unused_mut)]
    let mut record = serde_json::json!({
        "case": check.case_id,
        "step": check.step,
        "distribution": if check.rows_per_hot_key == 1 { "uniform-128" } else { "two-hot-keys" },
        "rows_per_hot_key": check.rows_per_hot_key,
        "seed_rows_per_side": SEED_ROWS_PER_SIDE,
        "batch_size": check.batch_size,
        "operation": check.operation,
        "push_wall_ns": elapsed_ns,
        "delta_rows": actual_delta.len(),
        "delta_absolute_weighted_pairs": abs_weight(&actual_delta),
        "snapshot_rows": check.full_after.len(),
        "snapshot_absolute_weighted_pairs": abs_weight(check.full_after),
        "allocator_observer_enabled": cfg!(feature = "bench-allocation-observer"),
        "wall_time_comparable": !cfg!(feature = "bench-allocation-observer"),
    });
    #[cfg(feature = "bench-allocation-observer")]
    {
        record["allocator_live_bytes_before"] = live_before.into();
        record["allocator_live_bytes_after"] = stats.live.into();
        record["allocator_peak_bytes_after_reset"] = stats.peak.into();
        record["allocator_events_during_push"] = allocation_events.into();
    }
    println!("CSFY_JOIN_COST {}", record);
}

fn run_case(hot_key_rows: usize, batch_size: usize, operation: &str) {
    let distribution = if hot_key_rows == 1 {
        "uniform-128"
    } else {
        "two-hot-keys"
    };
    let case_id = format!("{distribution}-batch-{batch_size}-{operation}");
    let mut core = EngineCore::new();
    core.register_input(LEFT).unwrap();
    core.register_input(RIGHT).unwrap();
    core.build_view(
        VIEW,
        &Plan::Join {
            left: Box::new(Plan::Source(LEFT)),
            right: Box::new(Plan::Source(RIGHT)),
            left_key: vec![0],
            right_key: vec![0],
        },
    )
    .unwrap();
    core.tap_view(VIEW).unwrap();

    let left_seed = seed_rows(hot_key_rows);
    let right_seed = seed_rows(hot_key_rows);
    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut accumulated = WeightedRows::new();
    let left_seed_batch = input_batch(&left_seed);
    let right_seed_batch = input_batch(&right_seed);

    let full_before = full_join(&left, &right);
    core.push(LEFT, &left_seed_batch).unwrap();
    apply_input(&mut left, &left_seed);
    let full_after_left_seed = full_join(&left, &right);
    let left_seed_delta = engine_rows(&core.take_changes(VIEW).unwrap());
    assert_eq!(
        left_seed_delta,
        map_delta(&full_after_left_seed, &full_before)
    );
    add_rows(&mut accumulated, &left_seed_delta);
    assert_eq!(accumulated, full_after_left_seed);
    assert_eq!(
        engine_rows(&core.snapshot(VIEW).unwrap()),
        full_after_left_seed
    );

    core.push(RIGHT, &right_seed_batch).unwrap();
    apply_input(&mut right, &right_seed);
    let seeded = full_join(&left, &right);
    let right_seed_delta = engine_rows(&core.take_changes(VIEW).unwrap());
    assert_eq!(right_seed_delta, map_delta(&seeded, &full_after_left_seed));
    add_rows(&mut accumulated, &right_seed_delta);
    assert_eq!(accumulated, seeded);
    assert_eq!(engine_rows(&core.snapshot(VIEW).unwrap()), seeded);

    let left_change = change_batch(&left, hot_key_rows, batch_size, 0, operation == "insert");
    let right_change = change_batch(&right, hot_key_rows, batch_size, 1, operation == "insert");
    let changes = [
        (LEFT, "left_delta", left_change.clone()),
        (RIGHT, "right_delta", right_change.clone()),
        (
            LEFT,
            "left_inverse",
            left_change
                .iter()
                .map(|&(key, payload, diff)| (key, payload, -diff))
                .collect(),
        ),
        (
            RIGHT,
            "right_inverse",
            right_change
                .iter()
                .map(|&(key, payload, diff)| (key, payload, -diff))
                .collect(),
        ),
    ];

    for (input, step, changes) in changes {
        let previous = full_join(&left, &right);
        let next_left = if input == LEFT {
            let mut next = left.clone();
            apply_input(&mut next, &changes);
            next
        } else {
            left.clone()
        };
        let next_right = if input == RIGHT {
            let mut next = right.clone();
            apply_input(&mut next, &changes);
            next
        } else {
            right.clone()
        };
        let next = full_join(&next_left, &next_right);
        let batch = input_batch(&changes);
        push_and_check(
            &mut core,
            input,
            &batch,
            PushCheck {
                output_before: &previous,
                full_after: &next,
                accumulated: &mut accumulated,
                case_id: &case_id,
                step,
                rows_per_hot_key: hot_key_rows,
                batch_size,
                operation,
            },
        );
        left = next_left;
        right = next_right;
    }

    assert_eq!(
        source_weights(&left),
        source_weights(&seed_rows(hot_key_rows)),
        "{case_id} left restoration"
    );
    assert_eq!(
        source_weights(&right),
        source_weights(&seed_rows(hot_key_rows)),
        "{case_id} right restoration"
    );
    assert_eq!(accumulated, seeded, "{case_id} output restoration");
}

#[test]
fn bounded_hotkey_batch_retraction_cost_matrix() {
    #[cfg(feature = "bench-allocation-observer")]
    let (_observer_baseline_label, _observer_baseline_live_bytes) =
        baseline_alloc::capture_baseline(|| "join-cost-matrix setup complete".to_owned());
    for hot_key_rows in [1, 64] {
        for batch_size in [1, 8, 64] {
            for operation in ["insert", "retract"] {
                run_case(hot_key_rows, batch_size, operation);
            }
        }
    }
}
