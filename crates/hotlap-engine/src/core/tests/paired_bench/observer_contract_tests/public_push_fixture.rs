use super::super::*;
use std::collections::{BTreeMap, BTreeSet};

type SourceBag = BTreeMap<(i64, i64, Option<i64>), i64>;
type AggregateRow = (
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    i64,
);

fn legacy_first() -> bool {
    std::env::var_os("W3HW_LEGACY_FIRST").is_some()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AggregateState {
    count: i64,
    count_right: i64,
    sum_left: i64,
    sum_right: Option<i64>,
}

fn source_bag(batch: &ZSetBatch) -> SourceBag {
    let hi = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lo = batch
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let value = batch
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diff = batch.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    let mut bag = SourceBag::new();
    for row in 0..batch.len() {
        let payload = (!value.is_null(row)).then(|| value.value(row));
        *bag.entry((hi.value(row), lo.value(row), payload))
            .or_default() += diff.value(row);
    }
    bag.retain(|_, weight| *weight != 0);
    bag
}

fn apply_source_delta(bag: &mut SourceBag, delta: &ZSetBatch) {
    for (key, weight) in source_bag(delta) {
        *bag.entry(key).or_default() += weight;
    }
    bag.retain(|_, weight| *weight != 0);
}

fn canonical_row_count(batch: &ZSetBatch) -> usize {
    source_bag(batch).len()
}

fn source_weight_total(batch: &ZSetBatch) -> i64 {
    source_bag(batch).values().sum()
}

fn expected_dense_pairs(fixture: Fixture, input: InputId, delta: &ZSetBatch) -> usize {
    let changed = source_bag(delta);
    let other_batch = if input == InputId(0) {
        fixture.initial_right()
    } else {
        fixture.initial_left()
    };
    let other = source_bag(&other_batch);
    let mut other_rows = BTreeMap::<(i64, i64), usize>::new();
    for ((hi, lo, _), _) in other {
        *other_rows.entry((hi, lo)).or_default() += 1;
    }
    changed
        .iter()
        .map(|((hi, lo, _), _)| other_rows.get(&(*hi, *lo)).copied().unwrap_or(0))
        .sum()
}

fn aggregate_state(left: &SourceBag, right: &SourceBag) -> BTreeMap<i64, AggregateState> {
    let mut groups = BTreeMap::<i64, AggregateState>::new();
    for ((left_hi, left_lo, left_value), left_weight) in left {
        for ((right_hi, right_lo, right_value), right_weight) in right {
            if left_hi != right_hi || left_lo != right_lo {
                continue;
            }
            let weight = left_weight * right_weight;
            let state = groups.entry(*left_hi).or_insert(AggregateState {
                count: 0,
                count_right: 0,
                sum_left: 0,
                sum_right: None,
            });
            state.count += weight;
            if right_value.is_some() {
                state.count_right += weight;
            }
            state.sum_left += left_value.expect("fixture left values are non-null") * weight;
            if let Some(value) = right_value {
                *state.sum_right.get_or_insert(0) += value * weight;
            }
        }
    }
    groups
}

fn state_row(key: i64, state: &AggregateState, diff: i64) -> AggregateRow {
    (
        Some(key),
        Some(state.count),
        Some(state.count_right),
        Some(state.sum_left),
        state.sum_right,
        diff,
    )
}

fn expected_group_outputs(
    fixture: Fixture,
    input: InputId,
    delta: &ZSetBatch,
) -> (Vec<AggregateRow>, Vec<AggregateRow>) {
    let mut left = source_bag(&fixture.initial_left());
    let mut right = source_bag(&fixture.initial_right());
    let before = aggregate_state(&left, &right);
    if input == InputId(0) {
        apply_source_delta(&mut left, delta);
    } else {
        apply_source_delta(&mut right, delta);
    }
    let after = aggregate_state(&left, &right);
    let mut changes = Vec::new();
    for key in before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        match (before.get(&key), after.get(&key)) {
            (Some(old), Some(new)) if old == new => {}
            (old, new) => {
                if let Some(old) = old {
                    changes.push(state_row(key, old, -1));
                }
                if let Some(new) = new {
                    changes.push(state_row(key, new, 1));
                }
            }
        }
    }
    let mut snapshot = after
        .iter()
        .map(|(&key, state)| state_row(key, state, 1))
        .collect::<Vec<_>>();
    changes.sort_unstable();
    snapshot.sort_unstable();
    (changes, snapshot)
}

fn expected_group_outputs_from(
    mut left: SourceBag,
    mut right: SourceBag,
    input: InputId,
    delta: &ZSetBatch,
) -> (Vec<AggregateRow>, Vec<AggregateRow>) {
    let before = aggregate_state(&left, &right);
    if input == InputId(0) {
        apply_source_delta(&mut left, delta);
    } else {
        apply_source_delta(&mut right, delta);
    }
    let after = aggregate_state(&left, &right);
    let mut changes = Vec::new();
    for key in before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        match (before.get(&key), after.get(&key)) {
            (Some(old), Some(new)) if old == new => {}
            (old, new) => {
                if let Some(old) = old {
                    changes.push(state_row(key, old, -1));
                }
                if let Some(new) = new {
                    changes.push(state_row(key, new, 1));
                }
            }
        }
    }
    let mut snapshot = after
        .iter()
        .map(|(&key, state)| state_row(key, state, 1))
        .collect::<Vec<_>>();
    changes.sort_unstable();
    snapshot.sort_unstable();
    (changes, snapshot)
}

fn typed_aggregate_rows(batch: &ZSetBatch, case: &str) -> Vec<AggregateRow> {
    assert_eq!(batch.schema().fields().len(), 5, "{case} output width");
    for field in batch.schema().fields() {
        assert_eq!(field.data_type(), &DataType::Int64, "{case} output type");
    }
    let arrays = batch
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect::<Vec<_>>();
    let diff = batch.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    let mut rows = (0..batch.len())
        .map(|row| {
            (
                (!arrays[0].is_null(row)).then(|| arrays[0].value(row)),
                (!arrays[1].is_null(row)).then(|| arrays[1].value(row)),
                (!arrays[2].is_null(row)).then(|| arrays[2].value(row)),
                (!arrays[3].is_null(row)).then(|| arrays[3].value(row)),
                (!arrays[4].is_null(row)).then(|| arrays[4].value(row)),
                diff.value(row),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

fn assert_typed_aggregate_rows(batch: &ZSetBatch, expected: &[AggregateRow], case: &str) {
    assert_eq!(
        typed_aggregate_rows(batch, case),
        expected,
        "{case} complete typed rows/diffs"
    );
}

#[test]
#[ignore = "smoke-only direct/legacy EngineCore push fixtures; no timing claim"]
fn dense_join_direct_index_public_push_fixture_smoke() {
    let cases = [
        Fixture {
            name: "narrow-1x1",
            keys: 1,
            fanout: 1,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendLeft,
        },
        Fixture {
            name: "moderate-4x8",
            keys: 4,
            fanout: 8,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendLeft,
        },
        Fixture {
            name: "duplicates-2x8",
            keys: 2,
            fanout: 8,
            duplicate_payloads: true,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendLeft,
        },
        Fixture {
            name: "fanout-1x128",
            keys: 1,
            fanout: 128,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::AppendLeft,
        },
        Fixture {
            name: "skewed-4x8",
            keys: 4,
            fanout: 8,
            duplicate_payloads: false,
            skewed: true,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::ReplaceLeft,
        },
        Fixture {
            name: "no-match-4x8",
            keys: 4,
            fanout: 8,
            duplicate_payloads: false,
            skewed: false,
            mixed_flat_view: false,
            unsafe_avg_history: false,
            subset_group: false,
            update: Update::NoMatchLeft,
        },
    ];
    for fixture in cases {
        let (input, update) = fixture.update();
        let expected_pairs = expected_dense_pairs(fixture, input, &update);
        let (expected_change, expected_snapshot) = expected_group_outputs(fixture, input, &update);
        let _mode = ModeGuard::new(true);

        let mut direct = build_seeded(fixture, true);
        crate::ops::join::test_support::reset_counts();
        crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|c| c.set(0));
        let direct_started = Instant::now();
        let direct_result = direct.push(input, &update);
        let direct_ns = direct_started.elapsed().as_nanos();
        direct_result.unwrap();
        let direct_routes = crate::ops::join::test_support::counts();
        let direct_calls = crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get);

        let (mut legacy, legacy_ns, legacy_routes, legacy_calls) = {
            let _legacy = crate::ops::join::test_support::LegacyDenseJoinGuard::force();
            crate::ops::join::test_support::reset_counts();
            let mut legacy = build_seeded(fixture, true);
            let seed_routes = crate::ops::join::test_support::counts();
            assert_eq!(
                seed_routes.direct, 0,
                "{} reference seed stayed legacy",
                fixture.name
            );
            assert!(
                seed_routes.forced_legacy > 0,
                "{} reference seed used legacy",
                fixture.name
            );
            crate::ops::join::test_support::reset_counts();
            crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|c| c.set(0));
            let started = Instant::now();
            let result = legacy.push(input, &update);
            let elapsed = started.elapsed().as_nanos();
            result.unwrap();
            let routes = crate::ops::join::test_support::counts();
            let calls = crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get);
            (legacy, elapsed, routes, calls)
        };
        assert!(direct_routes.direct > 0, "{} direct path", fixture.name);
        assert_eq!(
            direct_routes.direct_pairs, expected_pairs,
            "{} direct matched pairs",
            fixture.name
        );
        assert_eq!(
            direct_routes.forced_legacy, 0,
            "{} candidate did not use legacy",
            fixture.name
        );
        assert_eq!(
            legacy_routes.direct, 0,
            "{} reference did not use direct",
            fixture.name
        );
        assert!(
            legacy_routes.forced_legacy > 0,
            "{} reference legacy path",
            fixture.name
        );
        assert_eq!(
            legacy_routes.forced_legacy_pairs, expected_pairs,
            "{} legacy matched pairs",
            fixture.name
        );
        assert_eq!(
            direct_routes.wide_fallback, 0,
            "{} candidate width",
            fixture.name
        );
        assert!(
            direct_calls > 0 && legacy_calls > 0,
            "{} Dense materialization",
            fixture.name
        );

        let direct_change = direct.take_changes(ViewId(0)).unwrap();
        let legacy_change = legacy.take_changes(ViewId(0)).unwrap();
        assert_same_batch(&direct_change, &legacy_change);
        assert_typed_aggregate_rows(&direct_change, &expected_change, fixture.name);
        let direct_snapshot = direct.snapshot(ViewId(0)).unwrap();
        let legacy_snapshot = legacy.snapshot(ViewId(0)).unwrap();
        assert_same_batch(&direct_snapshot, &legacy_snapshot);
        assert_typed_aggregate_rows(&direct_snapshot, &expected_snapshot, fixture.name);
        assert_eq!(direct.checkpoint().unwrap(), legacy.checkpoint().unwrap());
        println!(
            "push_fixture name={} raw_left={} raw_right={} canonical_left={} canonical_right={} left_weight_total={} right_weight_total={} update_rows={} expected_pairs={} direct_calls={} legacy_calls={} direct_ns={} legacy_ns={} output_delta_rows={} snapshot_rows={}",
            fixture.name,
            fixture.initial_left().len(),
            fixture.initial_right().len(),
            canonical_row_count(&fixture.initial_left()),
            canonical_row_count(&fixture.initial_right()),
            source_weight_total(&fixture.initial_left()),
            source_weight_total(&fixture.initial_right()),
            update.len(),
            expected_pairs,
            direct_routes.direct,
            legacy_routes.forced_legacy,
            direct_ns,
            legacy_ns,
            direct_change.len(),
            direct_snapshot.len(),
        );
    }
}

#[test]
#[ignore = "smoke-only actual 16,384-pair public EngineCore push; not a measurement matrix"]
fn dense_join_direct_index_public_push_16384_pair_smoke() {
    let fixture = Fixture {
        name: "one-key-128x128-distinct",
        keys: 1,
        fanout: 128,
        duplicate_payloads: false,
        skewed: false,
        mixed_flat_view: false,
        unsafe_avg_history: false,
        subset_group: false,
        update: Update::AppendRight,
    };
    let right_delta = zset(
        vec![0; 128],
        (0..128).map(|row| Some(1_000_000 + row)).collect(),
        vec![1; 128],
    );
    let empty_right = ZSetBatch::empty(right_delta.schema());
    let left_seed = fixture.initial_left();
    let expected_pairs = expected_dense_pairs(fixture, InputId(1), &right_delta);
    let (expected_change, expected_snapshot) = expected_group_outputs_from(
        source_bag(&left_seed),
        SourceBag::new(),
        InputId(1),
        &right_delta,
    );
    assert_eq!(expected_pairs, 16_384);
    assert_eq!(expected_change.len(), 1);
    assert_eq!(expected_snapshot.len(), 1);
    assert_eq!(
        expected_snapshot,
        vec![(
            Some(0),
            Some(16_384),
            Some(16_384),
            Some(1_040_384),
            Some(16_385_040_384),
            1,
        )]
    );

    let _mode = ModeGuard::new(true);
    let legacy_first = legacy_first();
    let run = |legacy: bool| {
        let _old = legacy.then(crate::ops::join::test_support::LegacyDenseJoinGuard::force);
        crate::ops::join::test_support::reset_counts();
        let mut engine = build_empty(fixture);
        engine.push(InputId(0), &left_seed).unwrap();
        engine.push(InputId(1), &empty_right).unwrap();
        let seed_routes = crate::ops::join::test_support::counts();
        if legacy {
            assert_eq!(seed_routes.direct, 0, "all legacy seed pushes use old path");
            assert!(seed_routes.forced_legacy > 0);
        } else {
            assert_eq!(seed_routes.forced_legacy, 0);
            assert!(seed_routes.direct > 0);
        }
        let _ = engine.take_changes(ViewId(0)).unwrap();
        crate::ops::join::test_support::reset_counts();
        crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|count| count.set(0));
        let started = Instant::now();
        let result = engine.push(InputId(1), &right_delta);
        let elapsed = started.elapsed().as_nanos();
        result.unwrap();
        let routes = crate::ops::join::test_support::counts();
        let dense_calls = crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get);
        (engine, elapsed, routes, dense_calls)
    };
    let (direct_result, legacy_result) = if legacy_first {
        let legacy = run(true);
        let direct = run(false);
        (direct, legacy)
    } else {
        (run(false), run(true))
    };
    let (mut direct, direct_ns, direct_routes, direct_dense_calls) = direct_result;
    let (mut legacy, legacy_ns, legacy_routes, legacy_dense_calls) = legacy_result;

    assert_eq!(direct_routes.direct_pairs, expected_pairs);
    assert_eq!(legacy_routes.forced_legacy_pairs, expected_pairs);
    assert_eq!(direct_routes.forced_legacy, 0);
    assert_eq!(legacy_routes.direct, 0);
    assert_eq!(direct_dense_calls, 1);
    assert_eq!(legacy_dense_calls, 1);
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    let direct_change = direct.take_changes(ViewId(0)).unwrap();
    let legacy_change = legacy.take_changes(ViewId(0)).unwrap();
    assert_same_batch(&direct_change, &legacy_change);
    assert_typed_aggregate_rows(&direct_change, &expected_change, fixture.name);
    let direct_snapshot = direct.snapshot(ViewId(0)).unwrap();
    let legacy_snapshot = legacy.snapshot(ViewId(0)).unwrap();
    assert_same_batch(&direct_snapshot, &legacy_snapshot);
    assert_typed_aggregate_rows(&direct_snapshot, &expected_snapshot, fixture.name);
    assert_eq!(direct.checkpoint().unwrap(), legacy.checkpoint().unwrap());
    println!(
        "public_high_fanout_smoke legacy_first={} pairs={} left_rows={} measured_right_rows={} calls_direct={} calls_legacy={} direct_ns={} legacy_ns={} delta_rows={} snapshot_rows={}",
        legacy_first,
        expected_pairs,
        canonical_row_count(&left_seed),
        canonical_row_count(&right_delta),
        direct_routes.direct,
        legacy_routes.forced_legacy,
        direct_ns,
        legacy_ns,
        direct_change.len(),
        direct_snapshot.len(),
    );
}
