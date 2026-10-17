//! Bounded public-engine MIN/MAX characterization; the timer covers one push.

use std::time::Instant;
use std::{collections::BTreeMap, sync::Arc};

use super::*;
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::snapshot::{AggValue, EngineSnapshot, OperatorState, OrderedMultiset};
use serde::Deserialize;

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    cardinality: usize,
    value: i64,
    diff: i64,
    changes_extreme: bool,
}

const CARDINALITIES: [usize; 2] = [8, 8192];

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for cardinality in CARDINALITIES {
        cases.extend([
            Case {
                name: "insert_new_min",
                cardinality,
                value: -1,
                diff: 1,
                changes_extreme: true,
            },
            Case {
                name: "duplicate_min",
                cardinality,
                value: 0,
                diff: 1,
                changes_extreme: false,
            },
            Case {
                name: "retract_interior",
                cardinality,
                value: (cardinality / 2) as i64,
                diff: -1,
                changes_extreme: false,
            },
            Case {
                name: "retract_current_min",
                cardinality,
                value: 0,
                diff: -1,
                changes_extreme: true,
            },
            Case {
                name: "retract_current_max",
                cardinality,
                value: cardinality as i64 - 1,
                diff: -1,
                changes_extreme: true,
            },
        ]);
    }
    cases
}

fn batch(values: &[i64], diffs: &[i64]) -> ZSetBatch {
    assert_eq!(values.len(), diffs.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("group_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1; values.len()])) as ArrayRef,
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(input, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

fn seed(case: Case) -> ZSetBatch {
    let values = (0..case.cardinality as i64).collect::<Vec<_>>();
    batch(&values, &vec![1; values.len()])
}

fn delta(case: Case) -> ZSetBatch {
    batch(&[case.value], &[case.diff])
}

fn engine() -> EngineCore {
    let mut engine = EngineCore::new();
    engine.register_input(InputId(0)).unwrap();
    engine
        .build_view(ViewId(1), &Plan::Source(InputId(0)))
        .unwrap();
    engine
        .build_view(
            ViewId(0),
            &Plan::GroupAggregate {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                aggs: vec![AggSpec::min(1), AggSpec::max(1)],
            },
        )
        .unwrap();
    engine.tap_view(ViewId(0)).unwrap();
    engine
}

fn assert_same_zset(actual: &ZSetBatch, expected: &ZSetBatch) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.batch.columns().iter().zip(expected.batch.columns()) {
        assert_eq!(actual.to_data(), expected.to_data());
    }
    assert_eq!(actual.diff().to_data(), expected.diff().to_data());
}

fn expected_changelog_rows(case: Case) -> Vec<(i64, i64, i64, i64)> {
    let before = (0, case.cardinality as i64 - 1);
    let after = expected_extrema(case);
    if case.changes_extreme {
        vec![(1, before.0, before.1, -1), (1, after.0, after.1, 1)]
    } else {
        Vec::new()
    }
}

fn assert_expected_changelog(actual: &ZSetBatch, case: Case) {
    let expected_schema = Schema::new(vec![
        Field::new("group_key", DataType::Int64, false),
        Field::new("min", DataType::Int64, true),
        Field::new("max", DataType::Int64, true),
    ]);
    assert_eq!(actual.schema().as_ref(), &expected_schema);
    let key = actual
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let min = actual
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let max = actual
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diff = actual.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(key.null_count(), 0);
    assert_eq!(min.null_count(), 0);
    assert_eq!(max.null_count(), 0);
    let mut actual_rows = (0..actual.len())
        .map(|row| {
            (
                key.value(row),
                min.value(row),
                max.value(row),
                diff.value(row),
            )
        })
        .collect::<Vec<_>>();
    let mut expected_rows = expected_changelog_rows(case);
    actual_rows.sort_unstable();
    expected_rows.sort_unstable();
    assert_eq!(actual_rows, expected_rows, "typed changelog: {}", case.name);
}

fn source_counts(case: Case) -> BTreeMap<i64, i64> {
    let mut counts = (0..case.cardinality as i64)
        .map(|value| (value, 1))
        .collect::<BTreeMap<_, _>>();
    apply_source_delta(&mut counts, case);
    counts
}

fn apply_source_delta(counts: &mut BTreeMap<i64, i64>, case: Case) {
    let count = counts.get(&case.value).copied().unwrap_or(0) + case.diff;
    if count == 0 {
        counts.remove(&case.value);
    } else {
        counts.insert(case.value, count);
    }
}

fn assert_source_counts(source: &ZSetBatch, expected: &BTreeMap<i64, i64>) {
    let expected_schema = Schema::new(vec![
        Field::new("group_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]);
    assert_eq!(source.schema().as_ref(), &expected_schema);
    let keys = source
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = source
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = source.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(keys.null_count(), 0);
    assert_eq!(values.null_count(), 0);
    let actual = (0..source.len())
        .map(|row| {
            assert_eq!(keys.value(row), 1);
            (values.value(row), diffs.value(row))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(&actual, expected, "source multiplicities");
}

// Test-only mirror of the v1 multiset wire shape. Float payload bits use u64;
// these fixtures contain only integer keys, so no float interpretation occurs.
#[derive(Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
enum V1ExtremeKey {
    Int(i128),
    Float(u64),
}

#[derive(Deserialize)]
struct V1Multiset {
    counts: BTreeMap<V1ExtremeKey, i64>,
}

fn exact_multiset(multiset: &OrderedMultiset) -> BTreeMap<i64, i64> {
    let bytes = bincode::serialize(multiset).unwrap();
    let decoded: V1Multiset = bincode::deserialize(&bytes).unwrap();
    decoded
        .counts
        .into_iter()
        .map(|(key, count)| match key {
            V1ExtremeKey::Int(value) => (i64::try_from(value).unwrap(), count),
            V1ExtremeKey::Float(_) => panic!("integer fixture has float multiset value"),
        })
        .collect()
}

fn assert_group_state(snapshot: &EngineSnapshot, expected: &BTreeMap<i64, i64>) {
    let group = snapshot.views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|state| match state {
            OperatorState::Group(group) => Some(group),
            _ => None,
        })
        .unwrap();
    assert_eq!(group.groups.len(), 1, "one source group expected");
    let entry = &group.groups[0];
    assert_eq!(entry.rows, expected.values().sum::<i64>());
    let min = match &entry.values[0] {
        AggValue::Min(values) => values,
        _ => panic!("first aggregate is not MIN"),
    };
    let max = match &entry.values[1] {
        AggValue::Max(values) => values,
        _ => panic!("second aggregate is not MAX"),
    };
    assert_eq!(&exact_multiset(min), expected, "MIN multiplicities");
    assert_eq!(&exact_multiset(max), expected, "MAX multiplicities");
}

fn expected_extrema(case: Case) -> (i64, i64) {
    let max = case.cardinality as i64 - 1;
    match case.name {
        "insert_new_min" => (-1, max),
        "duplicate_min" | "retract_interior" => (0, max),
        "retract_current_min" => (1, max),
        "retract_current_max" => (0, max - 1),
        _ => unreachable!("unknown fixture action {}", case.name),
    }
}

fn assert_snapshot_extrema(snapshot: &ZSetBatch, case: Case) {
    let expected_schema = Schema::new(vec![
        Field::new("group_key", DataType::Int64, false),
        Field::new("min", DataType::Int64, true),
        Field::new("max", DataType::Int64, true),
    ]);
    assert_eq!(snapshot.schema().as_ref(), &expected_schema);
    let key = snapshot
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let min = snapshot
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let max = snapshot
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(key.null_count(), 0);
    assert_eq!(key.value(0), 1);
    assert_eq!(min.null_count(), 0);
    assert_eq!(max.null_count(), 0);
    assert_eq!(min.value(0), expected_extrema(case).0, "{}", case.name);
    assert_eq!(max.value(0), expected_extrema(case).1, "{}", case.name);
}

fn assert_case(case: Case) {
    let mut original = engine();
    let source_seed = seed(case);
    original.push(InputId(0), &source_seed).unwrap();
    let seed_changes = original.take_changes(ViewId(0)).unwrap();
    assert_eq!(seed_changes.len(), 1, "seed creates one aggregate row");
    let source_before = original.snapshot(ViewId(1)).unwrap();
    let checkpoint_before = original.checkpoint().unwrap();
    let mut expected_source = source_counts(Case { diff: 0, ..case });
    assert_group_state(&checkpoint_before, &expected_source);
    assert_source_counts(&source_before, &expected_source);

    let mut restored = EngineCore::new();
    restored.restore(&checkpoint_before).unwrap();
    let input_delta = delta(case);

    original.push(InputId(0), &input_delta).unwrap();
    restored.push(InputId(0), &input_delta).unwrap();
    let original_changes = original.take_changes(ViewId(0)).unwrap();
    let restored_changes = restored.take_changes(ViewId(0)).unwrap();
    assert_expected_changelog(&original_changes, case);
    assert_expected_changelog(&restored_changes, case);
    let original_snapshot = original.snapshot(ViewId(0)).unwrap();
    let restored_snapshot = restored.snapshot(ViewId(0)).unwrap();
    assert_same_zset(&original_snapshot, &restored_snapshot);
    assert_snapshot_extrema(&original_snapshot, case);
    apply_source_delta(&mut expected_source, case);
    let original_source = original.snapshot(ViewId(1)).unwrap();
    let restored_source = restored.snapshot(ViewId(1)).unwrap();
    assert_same_zset(&original_source, &restored_source);
    assert_source_counts(&original_source, &expected_source);
    assert_source_counts(&restored_source, &expected_source);
    let original_checkpoint = original.checkpoint().unwrap();
    let restored_checkpoint = restored.checkpoint().unwrap();
    assert_group_state(&original_checkpoint, &expected_source);
    assert_group_state(&restored_checkpoint, &expected_source);
    assert_eq!(original_checkpoint, restored_checkpoint);
}

#[test]
fn minmax_public_engine_contracts_match_live_and_restored_continuation() {
    for case in cases() {
        assert_case(case);
    }
}

#[test]
fn duplicate_extremum_retractions_are_exact_across_live_and_restored_pushes() {
    let template = Case {
        name: "duplicate_min",
        cardinality: 8,
        value: 0,
        diff: 0,
        changes_extreme: false,
    };
    let mut live = engine();
    live.push(InputId(0), &seed(template)).unwrap();
    live.take_changes(ViewId(0)).unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&live.checkpoint().unwrap()).unwrap();
    let mut expected_source = source_counts(template);

    for case in [
        Case {
            diff: 1,
            ..template
        },
        Case {
            diff: -1,
            ..template
        },
        Case {
            name: "retract_current_min",
            diff: -1,
            changes_extreme: true,
            ..template
        },
    ] {
        let input_delta = delta(case);
        live.push(InputId(0), &input_delta).unwrap();
        restored.push(InputId(0), &input_delta).unwrap();
        let live_changes = live.take_changes(ViewId(0)).unwrap();
        let restored_changes = restored.take_changes(ViewId(0)).unwrap();
        assert_expected_changelog(&live_changes, case);
        assert_expected_changelog(&restored_changes, case);

        apply_source_delta(&mut expected_source, case);
        assert_source_counts(&live.snapshot(ViewId(1)).unwrap(), &expected_source);
        assert_source_counts(&restored.snapshot(ViewId(1)).unwrap(), &expected_source);
        let live_checkpoint = live.checkpoint().unwrap();
        let restored_checkpoint = restored.checkpoint().unwrap();
        assert_group_state(&live_checkpoint, &expected_source);
        assert_group_state(&restored_checkpoint, &expected_source);
        assert_eq!(live_checkpoint, restored_checkpoint);
    }
}

/// One externally selected fixture. Full timing matrices wait for independent review.
#[test]
#[ignore = "r41f smoke/measurement entry; select one case with R41F_MINMAX_CASE"]
fn minmax_public_push_fixture() {
    let selected = std::env::var("R41F_MINMAX_CASE").expect("select a named fixture case");
    let case = cases()
        .into_iter()
        .find(|case| format!("{}-distinct-{}", case.name, case.cardinality) == selected)
        .unwrap_or_else(|| panic!("unknown R41F_MINMAX_CASE={selected}"));

    let mut engine = engine();
    let source_seed = seed(case);
    engine.push(InputId(0), &source_seed).unwrap();
    let seed_changes = engine.take_changes(ViewId(0)).unwrap();
    assert_eq!(seed_changes.len(), 1);
    let input_delta = delta(case);
    let started = Instant::now();
    engine.push(InputId(0), &input_delta).unwrap();
    let push_wall_ns = started.elapsed().as_nanos();
    let changes = engine.take_changes(ViewId(0)).unwrap();
    assert_expected_changelog(&changes, case);
    let expected_source = source_counts(case);
    assert_source_counts(&engine.snapshot(ViewId(1)).unwrap(), &expected_source);
    assert_group_state(&engine.checkpoint().unwrap(), &expected_source);

    println!(
        "{}",
        serde_json::json!({
            "record": "r41f_minmax_smoke",
            "fixture": selected,
            "distinct_values_seeded": case.cardinality,
            "seed_rows": source_seed.len(),
            "delta_rows": input_delta.len(),
            "delta_value": case.value,
            "delta_weight": case.diff,
            "aggregate_changelog_rows": changes.len(),
            "public_push_wall_ns": push_wall_ns,
            "timed_scope": "one public EngineCore::push only; seed/setup/drain/assertions excluded",
            "production_candidate_comparison": false,
            "authoritative_timing": false
        })
    );
}
