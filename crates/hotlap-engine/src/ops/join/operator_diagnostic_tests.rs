use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::keys::full_converter;
use crate::ops::join::test_support::{self, LegacyDenseJoinGuard};

const SIDE_ROWS: usize = 128;
const ITERATIONS: usize = 16;

fn side(name: &str, offset: i64) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new(name, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64; SIDE_ROWS])) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(
                (0..SIDE_ROWS).map(|row| offset + row as i64),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1_i64; SIDE_ROWS]))).unwrap()
}

fn assert_output(output: &ZSetBatch) {
    assert_eq!(output.len(), SIDE_ROWS * SIDE_ROWS);
    assert_eq!(output.schema().fields().len(), 4);
    for field in output.schema().fields() {
        assert_eq!(field.data_type(), &DataType::Int64);
    }
    let diffs = output.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(diffs.null_count(), 0);
    assert!(diffs.values().iter().all(|&diff| diff == 1));
}

#[test]
#[ignore = "operator-only 16,384-pair diagnostic; one-shot smoke, not a performance acceptance test"]
fn high_fanout_equi_join_clock_smoke() {
    let left = side("left_payload", 0);
    let right = side("right_payload", 10_000);
    let left_conv = KeyConverter::new(left.schema().as_ref(), &[0]).unwrap();
    let right_conv = KeyConverter::new(right.schema().as_ref(), &[0]).unwrap();
    let output_schema = joined_schema(&left.schema(), &right.schema());
    let output_conv = full_converter(output_schema.as_ref()).unwrap();

    let run = |legacy: bool| {
        let _legacy = legacy.then(LegacyDenseJoinGuard::force);
        let warmup = equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap();
        assert_eq!(warmup.1, SIDE_ROWS * SIDE_ROWS);
        assert_output(&warmup.0);
        drop(warmup);
        test_support::reset_counts();
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            let (output, _) =
                equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap();
            black_box(output);
        }
        let elapsed = started.elapsed().as_nanos();
        (elapsed, test_support::counts())
    };
    let legacy_first = std::env::var_os("W3HW_LEGACY_FIRST").is_some();
    let (direct_result, legacy_result) = if legacy_first {
        let legacy = run(true);
        let direct = run(false);
        (direct, legacy)
    } else {
        (run(false), run(true))
    };
    let (direct_ns, direct_routes) = direct_result;
    let (legacy_ns, legacy_routes) = legacy_result;
    assert_eq!(direct_routes.direct, ITERATIONS);
    assert_eq!(direct_routes.forced_legacy, 0);
    assert_eq!(
        direct_routes.direct_pairs,
        ITERATIONS * SIDE_ROWS * SIDE_ROWS
    );

    assert_eq!(legacy_routes.forced_legacy, ITERATIONS);
    assert_eq!(legacy_routes.direct, 0);
    assert_eq!(
        legacy_routes.forced_legacy_pairs,
        ITERATIONS * SIDE_ROWS * SIDE_ROWS
    );
    println!(
        "operator_high_fanout legacy_first={} pairs_per_call={} iterations={} direct_total_ns={} direct_ns_per_call={} legacy_total_ns={} legacy_ns_per_call={} direct_actual_pairs={} legacy_actual_pairs={}",
        legacy_first,
        SIDE_ROWS * SIDE_ROWS,
        ITERATIONS,
        direct_ns,
        direct_ns / ITERATIONS as u128,
        legacy_ns,
        legacy_ns / ITERATIONS as u128,
        direct_routes.direct_pairs,
        legacy_routes.forced_legacy_pairs,
    );
}
