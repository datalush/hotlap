#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;
#[path = "support/baseline_conversion.rs"]
mod baseline_conversion;
#[path = "support/baseline_lifecycle.rs"]
mod baseline_lifecycle;
#[path = "support/baseline_memory.rs"]
mod baseline_memory;
#[path = "support/baseline_views.rs"]
mod baseline_views;
#[path = "support/baseline_workloads.rs"]
mod baseline_workloads;

#[test]
fn arrangement_update_allocation_window_compares_original_and_borrowed_lookup() {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::DataType;
    use arrow::row::{OwnedRow, RowConverter};
    use hotlap_engine::arrange::state::RowState;
    use std::sync::Arc;

    fn row(converter: &RowConverter, value: i64) -> OwnedRow {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![value]))];
        converter.convert_columns(&columns).unwrap().row(0).owned()
    }

    fn original_add(
        entries: &mut std::collections::HashMap<Vec<u8>, std::collections::HashMap<OwnedRow, i64>>,
        key: OwnedRow,
        payload: OwnedRow,
        diff: i64,
    ) {
        let payloads = entries.entry(key.as_ref().to_vec()).or_default();
        let sum = payloads.entry(payload.clone()).or_insert(0);
        *sum += diff;
        if *sum == 0 {
            payloads.remove(&payload);
        }
        if payloads.is_empty() {
            entries.remove(key.as_ref());
        }
    }

    let converter = RowConverter::new(vec![arrow::row::SortField::new(DataType::Int64)]).unwrap();
    for cardinality in [128i64, 512] {
        let mut optimized = RowState::default();
        let mut original = std::collections::HashMap::new();
        for value in 0..cardinality {
            optimized.add(
                row(&converter, value),
                row(&converter, value + cardinality),
                8,
            );
            original_add(
                &mut original,
                row(&converter, value),
                row(&converter, value + cardinality),
                8,
            );
        }
        let mut optimized_events = 0;
        let mut original_events = 0;
        let mut optimized_ns = 0u128;
        let mut original_ns = 0u128;
        let mut allocated_inputs: Vec<_> = (0..3)
            .map(|_| {
                (
                    row(&converter, cardinality / 2),
                    row(&converter, cardinality / 2 + cardinality),
                )
            })
            .collect();
        let mut original_inputs: Vec<_> = (0..3)
            .map(|_| {
                (
                    row(&converter, cardinality / 2),
                    row(&converter, cardinality / 2 + cardinality),
                )
            })
            .collect();
        for _ in 0..3 {
            let start = std::time::Instant::now();
            optimized_events += crate::baseline_alloc::count_events(|| {
                let (key, payload) = allocated_inputs.remove(0);
                optimized.add(key, payload, -1);
            });
            optimized_ns += start.elapsed().as_nanos();

            let start = std::time::Instant::now();
            original_events += crate::baseline_alloc::count_events(|| {
                let (key, payload) = original_inputs.remove(0);
                original_add(&mut original, key, payload, -1);
            });
            original_ns += start.elapsed().as_nanos();
        }
        assert_eq!(optimized_events, 0, "cardinality={cardinality}");
        assert!(original_events > 0, "cardinality={cardinality}");
        let optimized_values: Vec<_> = optimized
            .sorted()
            .into_iter()
            .map(|(key, payload, diff)| (key.clone(), payload.as_ref().to_vec(), diff))
            .collect();
        let mut original_values: Vec<_> = original
            .iter()
            .flat_map(|(key, payloads)| {
                payloads
                    .iter()
                    .map(move |(payload, diff)| (key.clone(), payload.as_ref().to_vec(), *diff))
            })
            .collect();
        original_values.sort();
        assert_eq!(optimized_values, original_values);
        println!(
            "arrangement_alloc_compare,cardinality={cardinality},repeats=3,optimized_events={optimized_events},original_events={original_events},optimized_elapsed_ns={optimized_ns},original_elapsed_ns={original_ns},seed=7,setup_outside_window=true"
        );
    }
}

#[test]
#[ignore = "matched release wall-clock profile; run with rows/seed/repeats pinned"]
fn arrangement_join_update_benchmark() {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::row::{RowConverter, SortField};
    use hotlap_engine::arrange::state::RowState;
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::Instant;

    let cardinality: i64 = std::env::var("HOTLAP_ARRANGE_ROWS")
        .unwrap()
        .parse()
        .unwrap();
    let repeats: usize = std::env::var("HOTLAP_ARRANGE_REPEATS")
        .unwrap()
        .parse()
        .unwrap();
    let seed: u64 = std::env::var("HOTLAP_ARRANGE_SEED")
        .unwrap()
        .parse()
        .unwrap();
    assert!([128, 512].contains(&cardinality));
    assert_eq!(repeats, 3);
    assert_eq!(seed, 7);
    let converter =
        RowConverter::new(vec![SortField::new(arrow::datatypes::DataType::Int64)]).unwrap();
    let to_row = |value| {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![value]))];
        converter.convert_columns(&columns).unwrap().row(0).owned()
    };
    let mut optimized = RowState::default();
    for key in 0..cardinality {
        optimized.add(to_row(key), to_row(key), 8);
    }
    let started = Instant::now();
    for index in 0..repeats * 4096 {
        let value = (index as i64 * 31 + seed as i64) % cardinality;
        optimized.add(
            to_row(value),
            to_row(value),
            if index % 2 == 0 { -1 } else { 1 },
        );
    }
    let elapsed_ns = started.elapsed().as_nanos();
    let expected = run_original_fixture(cardinality, repeats, seed, &to_row);
    let actual: Vec<_> = optimized
        .sorted()
        .into_iter()
        .map(|(key, payload, diff)| (key.clone(), payload.as_ref().to_vec(), diff))
        .collect();
    assert_eq!(actual, expected);
    black_box(&optimized);
    println!(
        "arrangement_join_update,cardinality={cardinality},repeats={repeats},updates={},elapsed_ns={elapsed_ns},seed={seed},source=borrowed_RowState_add",
        repeats * 4096
    );
}

fn run_original_fixture(
    cardinality: i64,
    repeats: usize,
    seed: u64,
    to_row: &impl Fn(i64) -> arrow::row::OwnedRow,
) -> Vec<(Vec<u8>, Vec<u8>, i64)> {
    use arrow::row::OwnedRow;
    use std::collections::HashMap;
    let mut original = HashMap::<Vec<u8>, HashMap<OwnedRow, i64>>::new();
    for key in 0..cardinality {
        let payload = to_row(key);
        let key = to_row(key);
        original
            .entry(key.as_ref().to_vec())
            .or_default()
            .insert(payload, 8);
    }
    for index in 0..repeats * 4096 {
        let value = (index as i64 * 31 + seed as i64) % cardinality;
        let key = to_row(value);
        let payload = to_row(value);
        let payloads = original.entry(key.as_ref().to_vec()).or_default();
        let sum = payloads.entry(payload.clone()).or_insert(0);
        *sum += if index % 2 == 0 { -1 } else { 1 };
        if *sum == 0 {
            payloads.remove(&payload);
        }
        if payloads.is_empty() {
            original.remove(key.as_ref());
        }
    }
    let mut values: Vec<_> = original
        .into_iter()
        .flat_map(|(key, payloads)| {
            payloads
                .into_iter()
                .map(move |(payload, diff)| (key.clone(), payload.as_ref().to_vec(), diff))
        })
        .collect();
    values.sort();
    values
}

#[test]
#[ignore = "matched release wall-clock profile; run with rows/seed/repeats pinned"]
fn arrangement_join_update_original_benchmark() {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::row::{OwnedRow, RowConverter, SortField};
    use std::collections::HashMap;
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::Instant;

    let cardinality: i64 = std::env::var("HOTLAP_ARRANGE_ROWS")
        .unwrap()
        .parse()
        .unwrap();
    let repeats: usize = std::env::var("HOTLAP_ARRANGE_REPEATS")
        .unwrap()
        .parse()
        .unwrap();
    let seed: u64 = std::env::var("HOTLAP_ARRANGE_SEED")
        .unwrap()
        .parse()
        .unwrap();
    assert!([128, 512].contains(&cardinality));
    assert_eq!(repeats, 3);
    assert_eq!(seed, 7);
    let converter =
        RowConverter::new(vec![SortField::new(arrow::datatypes::DataType::Int64)]).unwrap();
    let to_row = |value| {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![value]))];
        converter.convert_columns(&columns).unwrap().row(0).owned()
    };
    fn original_add(
        entries: &mut HashMap<Vec<u8>, HashMap<OwnedRow, i64>>,
        key: OwnedRow,
        payload: OwnedRow,
        diff: i64,
    ) {
        let payloads = entries.entry(key.as_ref().to_vec()).or_default();
        let sum = payloads.entry(payload.clone()).or_insert(0);
        *sum += diff;
        if *sum == 0 {
            payloads.remove(&payload);
        }
        if payloads.is_empty() {
            entries.remove(key.as_ref());
        }
    }
    let mut original = HashMap::<Vec<u8>, HashMap<OwnedRow, i64>>::new();
    for key in 0..cardinality {
        let payload = to_row(key);
        let key = to_row(key);
        original
            .entry(key.as_ref().to_vec())
            .or_default()
            .insert(payload, 8);
    }
    let started = Instant::now();
    for index in 0..repeats * 4096 {
        let value = ((index as i64 * 31 + seed as i64) % cardinality) % cardinality;
        let key = to_row(value);
        let payload = to_row(value);
        original_add(
            &mut original,
            key,
            payload,
            if index % 2 == 0 { -1 } else { 1 },
        );
    }
    let elapsed_ns = started.elapsed().as_nanos();
    black_box(&original);
    let expected = run_original_fixture(cardinality, repeats, seed, &to_row);
    let mut actual: Vec<_> = original
        .into_iter()
        .flat_map(|(key, payloads)| {
            payloads
                .into_iter()
                .map(move |(payload, diff)| (key.clone(), payload.as_ref().to_vec(), diff))
        })
        .collect();
    actual.sort();
    assert_eq!(actual, expected);
    println!(
        "arrangement_join_update_original,cardinality={cardinality},repeats={repeats},updates={},elapsed_ns={elapsed_ns},seed={seed},source=a3de194-parent-RowState_add",
        repeats * 4096
    );
}

#[test]
fn minimum_three_row_fixture_can_retract_a_filtered_out_row() {
    let rows = [(0, 7, 0, 1), (1, 24, 0, 1), (2, 41, 0, 1)];
    let selected = baseline_workloads::filter_retract_index(&rows);
    assert_eq!(selected, 0);
    assert!(rows[selected].1 < 50);
}

#[test]
fn matched_multiview_outputs_equal_independent_view_cores() {
    baseline_views::assert_matched_multiview_correctness(8);
}

#[test]
fn multiview_deltas_and_recovery_match_dense_signed_oracle() {
    baseline_views::assert_dense_multiview_correctness();
}

#[test]
fn signed_arrow_row_conversion_preserves_duplicate_and_negative_weights() {
    baseline_conversion::assert_signed_conversion_fixture();
}

#[test]
fn checkpoint_encoding_reports_nonzero_serialized_payload_size() {
    assert!(baseline_views::serialized_snapshot_size(8) > 0);
}

#[test]
fn memory_observability_baseline_separates_live_categories_and_serialized_size() {
    use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId};
    use hotlap_engine::EngineCore;

    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            aggs: vec![AggSpec::count()],
        },
    )
    .unwrap();
    let report = core.memory_report().unwrap();
    assert!(!report.complete);
    assert!(report.known_backing_capacity_bytes > 0);
    assert!(
        report
            .unknown_categories
            .contains(&"core_registries_schemas_and_watermarks".to_string())
    );
    assert!(
        !report
            .opaque_external_categories
            .contains(&"arrow_buffers".to_string())
    );
    assert!(
        !report
            .unknown_categories
            .iter()
            .any(|category| category == "joins")
    );
}

#[test]
#[ignore = "manual CPU/allocation baseline; run with --ignored --nocapture"]
fn kernel_families_cpu_and_allocation_baseline() {
    baseline_workloads::run();
}

#[test]
#[ignore = "manual memory coverage probe; run with --ignored --nocapture"]
fn kernel_memory_coverage_probe() {
    let rows = std::env::var("HOTLAP_BASELINE_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128);
    let repeats = std::env::var("HOTLAP_BASELINE_REPEATS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    baseline_memory::run(rows);
    for rep in 0..repeats {
        baseline_memory::run_family_coverage(rows, rep, repeats);
    }
}
