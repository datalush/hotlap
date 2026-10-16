use std::collections::BTreeMap;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::DataType;
use hotlap_core::snapshot::{AggValue, OperatorState};

use super::*;

const LARGE: f64 = (1_u64 << 53) as f64;

fn engine_with_source_taps() -> EngineCore {
    let mut engine = float_aggregate_core();
    for (view, input) in [(ViewId(1), InputId(0)), (ViewId(2), InputId(1))] {
        engine.build_view(view, &Plan::Source(input)).unwrap();
        engine.tap_view(view).unwrap();
    }
    engine
}

#[derive(Clone, Copy)]
enum Route {
    Direct,
    Legacy,
}

fn push_on_route(
    engine: &mut EngineCore,
    input: InputId,
    delta: &ZSetBatch,
    route: Route,
    expect_dense_materialization: bool,
) {
    use crate::ops::join::test_support::{self, LegacyDenseJoinGuard};

    test_support::reset_counts();
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|count| count.set(0));
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    match route {
        Route::Direct => engine.push(input, delta).unwrap(),
        Route::Legacy => {
            let _legacy = LegacyDenseJoinGuard::force();
            engine.push(input, delta).unwrap();
        }
    }
    let calls = test_support::counts();
    if expect_dense_materialization {
        match route {
            Route::Direct => {
                assert!(
                    calls.direct > 0,
                    "eligible Dense helper must use direct indices"
                );
                assert_eq!(calls.forced_legacy, 0);
            }
            Route::Legacy => {
                assert_eq!(calls.direct, 0);
                assert!(calls.forced_legacy > 0, "reference must use legacy tuples");
            }
        }
    } else {
        assert_eq!(calls.direct, 0);
        assert_eq!(calls.forced_legacy, 0);
    }
    assert_eq!(calls.wide_fallback, 0, "fixture indices fit u32");
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    if expect_dense_materialization {
        crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|count| assert!(count.get() > 0));
    }
}

type FloatOutputRow = (i64, u64, u64, u64, u64, i64);

fn float_rows(batch: &ZSetBatch) -> Vec<FloatOutputRow> {
    if batch.is_empty() {
        return Vec::new();
    }
    assert_eq!(batch.schema().fields().len(), 5);
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
    assert!(!batch.schema().field(0).is_nullable());
    for column in 1..5 {
        assert_eq!(batch.schema().field(column).data_type(), &DataType::Float64);
    }
    let key = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let floats = (1..5)
        .map(|column| {
            batch
                .batch
                .column(column)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let diff = batch.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(key.null_count(), 0);
    assert!(floats.iter().all(|values| values.null_count() == 0));
    assert_eq!(diff.null_count(), 0);
    let mut rows = (0..batch.len())
        .map(|row| {
            (
                key.value(row),
                floats[0].value(row).to_bits(),
                floats[1].value(row).to_bits(),
                floats[2].value(row).to_bits(),
                floats[3].value(row).to_bits(),
                diff.value(row),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

fn source_weights(engine: &mut EngineCore, view: ViewId) -> BTreeMap<(i64, u64), i64> {
    let snapshot = engine.snapshot(view).unwrap();
    if snapshot.is_empty() {
        return BTreeMap::new();
    }
    let keys = snapshot
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = snapshot
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let diffs = snapshot
        .diff()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..snapshot.len())
        .map(|row| {
            (
                (keys.value(row), values.value(row).to_bits()),
                diffs.value(row),
            )
        })
        .collect()
}

fn avg_sum_history(engine: &EngineCore) -> Option<(u64, i64, i64)> {
    let checkpoint = engine.checkpoint().unwrap();
    let group =
        checkpoint.views[0]
            .operators
            .iter()
            .flatten()
            .find_map(|operator| match operator {
                OperatorState::Group(group) => Some(group),
                _ => None,
            })?;
    let entry = &group.groups.first()?.1;
    let (avg_sum, avg_count) = match &entry.values[0] {
        AggValue::Avg { sum, count, .. } => (sum.to_bits(), *count),
        _ => panic!("first aggregate is AVG"),
    };
    let (sum, sum_count) = match &entry.values[1] {
        AggValue::SumFloat { sum, count, .. } => (sum.to_bits(), *count),
        _ => panic!("second aggregate is Float SUM"),
    };
    assert_eq!(avg_sum, sum);
    assert_eq!(avg_count, sum_count);
    Some((avg_sum, avg_count, entry.rows))
}

fn drain_changes(engine: &mut EngineCore) -> Vec<ZSetBatch> {
    [ViewId(0), ViewId(1), ViewId(2)]
        .into_iter()
        .map(|view| engine.take_changes(view).unwrap())
        .collect()
}

struct ExpectedPush<'a> {
    output: &'a [FloatOutputRow],
    snapshot: &'a [FloatOutputRow],
    left_changed: bool,
    right_changed: bool,
    left: &'a BTreeMap<(i64, u64), i64>,
    right: &'a BTreeMap<(i64, u64), i64>,
    history: Option<(u64, i64, i64)>,
}

fn assert_direct_reference(
    direct_live: &mut EngineCore,
    legacy_live: &mut EngineCore,
    direct_restored: Option<&mut EngineCore>,
    legacy_restored: Option<&mut EngineCore>,
    expected: ExpectedPush<'_>,
) {
    let direct_changes = drain_changes(direct_live);
    let legacy_changes = drain_changes(legacy_live);
    assert_eq!(direct_changes.len(), legacy_changes.len());
    for (direct, legacy) in direct_changes.iter().zip(&legacy_changes) {
        same_batches_bitwise(direct, legacy);
    }
    let mut expected_output = expected.output.to_vec();
    expected_output.sort_unstable();
    assert_eq!(float_rows(&direct_changes[0]), expected_output);
    assert_eq!(
        float_rows(&direct_live.snapshot(ViewId(0)).unwrap()),
        expected.snapshot
    );
    assert_eq!(!direct_changes[1].is_empty(), expected.left_changed);
    assert_eq!(!direct_changes[2].is_empty(), expected.right_changed);
    assert_eq!(source_weights(direct_live, ViewId(1)), *expected.left);
    assert_eq!(source_weights(direct_live, ViewId(2)), *expected.right);
    assert_eq!(source_weights(legacy_live, ViewId(1)), *expected.left);
    assert_eq!(source_weights(legacy_live, ViewId(2)), *expected.right);
    if let Some((bits, count, rows)) = expected.history {
        assert_eq!(avg_sum_history(direct_live), Some((bits, count, rows)));
        assert_eq!(avg_sum_history(legacy_live), Some((bits, count, rows)));
    } else {
        assert_eq!(avg_sum_history(direct_live), None);
        assert_eq!(avg_sum_history(legacy_live), None);
    }
    assert_eq!(
        direct_live.checkpoint().unwrap(),
        legacy_live.checkpoint().unwrap()
    );

    if let (Some(direct), Some(legacy)) = (direct_restored, legacy_restored) {
        let direct_changes = drain_changes(direct);
        let legacy_changes = drain_changes(legacy);
        for (direct, legacy) in direct_changes.iter().zip(&legacy_changes) {
            same_batches_bitwise(direct, legacy);
        }
        assert_eq!(float_rows(&direct_changes[0]), expected.output);
        assert_eq!(
            float_rows(&direct.snapshot(ViewId(0)).unwrap()),
            expected.snapshot
        );
        assert_eq!(source_weights(direct, ViewId(1)), *expected.left);
        assert_eq!(source_weights(direct, ViewId(2)), *expected.right);
        assert_eq!(!direct_changes[1].is_empty(), expected.left_changed);
        assert_eq!(!direct_changes[2].is_empty(), expected.right_changed);
        assert_eq!(avg_sum_history(direct), expected.history);
        assert_eq!(avg_sum_history(legacy), expected.history);
        assert_eq!(direct.checkpoint().unwrap(), legacy.checkpoint().unwrap());
        assert_eq!(
            direct.checkpoint().unwrap(),
            direct_live.checkpoint().unwrap()
        );
        assert_eq!(
            legacy.checkpoint().unwrap(),
            legacy_live.checkpoint().unwrap()
        );
    }
}

#[test]
fn direct_dense_indices_match_legacy_float_history_through_restore_and_retractions() {
    let mut direct = engine_with_source_taps();
    let mut legacy = engine_with_source_taps();
    let mut expected_left = BTreeMap::new();
    let mut expected_right = BTreeMap::new();

    let right_seed = float_pairs(&[(1, 9.0, 1)]);
    push_on_route(&mut direct, InputId(1), &right_seed, Route::Direct, false);
    push_on_route(&mut legacy, InputId(1), &right_seed, Route::Legacy, false);
    expected_right.insert((1, 9.0_f64.to_bits()), 1);
    assert_direct_reference(
        &mut direct,
        &mut legacy,
        None,
        None,
        ExpectedPush {
            output: &[],
            snapshot: &[],
            left_changed: false,
            right_changed: true,
            left: &expected_left,
            right: &expected_right,
            history: None,
        },
    );

    let mut previous_state: Option<FloatOutputRow> = None;
    for (value, sum, count, avg, min, max) in [
        (LARGE, LARGE, 1_i64, LARGE, LARGE, LARGE),
        (1.0, LARGE, 2, LARGE / 2.0, 1.0, LARGE),
        (-LARGE, 0.0, 3, 0.0, -LARGE, LARGE),
    ] {
        let delta = float_pairs(&[(1, value, 1)]);
        push_on_route(&mut direct, InputId(0), &delta, Route::Direct, true);
        push_on_route(&mut legacy, InputId(0), &delta, Route::Legacy, true);
        *expected_left.entry((1, value.to_bits())).or_insert(0) += 1;

        let current = (
            1,
            avg.to_bits(),
            sum.to_bits(),
            min.to_bits(),
            max.to_bits(),
            1,
        );
        let expected_changes = previous_state
            .map(|previous| {
                vec![
                    (
                        previous.0, previous.1, previous.2, previous.3, previous.4, -1,
                    ),
                    current,
                ]
            })
            .unwrap_or_else(|| vec![current]);
        assert_direct_reference(
            &mut direct,
            &mut legacy,
            None,
            None,
            ExpectedPush {
                output: &expected_changes,
                snapshot: &[current],
                left_changed: true,
                right_changed: false,
                left: &expected_left,
                right: &expected_right,
                history: Some((sum.to_bits(), count, count)),
            },
        );
        previous_state = Some(current);
    }

    let direct_checkpoint = direct.checkpoint().unwrap();
    let legacy_checkpoint = legacy.checkpoint().unwrap();
    assert_eq!(direct_checkpoint, legacy_checkpoint);
    let mut direct_restored = engine_with_source_taps();
    direct_restored.restore(&direct_checkpoint).unwrap();
    let mut legacy_restored = engine_with_source_taps();
    legacy_restored.restore(&legacy_checkpoint).unwrap();

    for (
        delta,
        retract_value,
        insert_value,
        previous_avg,
        previous_sum,
        previous_min,
        previous_max,
        expected_sum,
        expected_avg,
        expected_min,
        expected_max,
    ) in [
        (
            float_pairs(&[(1, 1.0, -1), (1, 2.0, 1)]),
            1.0_f64,
            2.0_f64,
            0.0_f64,
            0.0_f64,
            -LARGE,
            LARGE,
            1.0_f64,
            1.0_f64 / 3.0,
            -LARGE,
            LARGE,
        ),
        (
            float_pairs(&[(1, LARGE, -1), (1, 0.5, 1)]),
            LARGE,
            0.5_f64,
            1.0_f64 / 3.0,
            1.0_f64,
            -LARGE,
            LARGE,
            -LARGE + 2.0,
            (-LARGE + 2.0) / 3.0,
            -LARGE,
            2.0,
        ),
    ] {
        push_on_route(&mut direct, InputId(0), &delta, Route::Direct, true);
        push_on_route(&mut legacy, InputId(0), &delta, Route::Legacy, true);
        push_on_route(
            &mut direct_restored,
            InputId(0),
            &delta,
            Route::Direct,
            true,
        );
        push_on_route(
            &mut legacy_restored,
            InputId(0),
            &delta,
            Route::Legacy,
            true,
        );
        let weight = expected_left
            .get_mut(&(1, retract_value.to_bits()))
            .unwrap();
        *weight -= 1;
        if *weight == 0 {
            expected_left.remove(&(1, retract_value.to_bits()));
        }
        *expected_left
            .entry((1, insert_value.to_bits()))
            .or_insert(0) += 1;
        let expected_changes = vec![
            (
                1,
                previous_avg.to_bits(),
                previous_sum.to_bits(),
                previous_min.to_bits(),
                previous_max.to_bits(),
                -1,
            ),
            (
                1,
                expected_avg.to_bits(),
                expected_sum.to_bits(),
                expected_min.to_bits(),
                expected_max.to_bits(),
                1,
            ),
        ];
        let expected_snapshot = vec![(
            1,
            expected_avg.to_bits(),
            expected_sum.to_bits(),
            expected_min.to_bits(),
            expected_max.to_bits(),
            1,
        )];
        assert_direct_reference(
            &mut direct,
            &mut legacy,
            Some(&mut direct_restored),
            Some(&mut legacy_restored),
            ExpectedPush {
                output: &expected_changes,
                snapshot: &expected_snapshot,
                left_changed: true,
                right_changed: false,
                left: &expected_left,
                right: &expected_right,
                history: Some((expected_sum.to_bits(), 3, 3)),
            },
        );
    }
}

#[test]
fn direct_dense_route_reaches_zero_pair_empty_inputs() {
    use crate::ops::join::test_support;

    let mut engine = engine_with_source_taps();
    let left = float_pairs(&[(7, 3.0, 1)]);
    let right = float_pairs(&[(7, 8.0, 1)]);
    engine.push(InputId(0), &left).unwrap();
    engine.push(InputId(1), &right).unwrap();
    test_support::reset_direct_count();

    let no_match = float_pairs(&[(99, 11.0, 1)]);
    push_on_route(&mut engine, InputId(0), &no_match, Route::Direct, true);
    assert_eq!(test_support::direct_count(), 3);
    let no_match_delta = float_pairs(&[(99, 11.0, -1)]);
    push_on_route(
        &mut engine,
        InputId(0),
        &no_match_delta,
        Route::Direct,
        true,
    );
    assert_eq!(test_support::direct_count(), 3);

    let empty = ZSetBatch::empty(no_match.schema());
    push_on_route(&mut engine, InputId(0), &empty, Route::Direct, false);
    assert_eq!(test_support::direct_count(), 0);
}
