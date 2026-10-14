use super::*;
use crate::ops::join::Join;
use crate::ops::join::factorized::flat::{FlatRowsCursor, prepare_flat_join_update};
use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};

#[test]
fn flat_rows_prepare_is_eligible_and_matches_current_dense_join() {
    let left_schema = schema(&[("lk", DataType::Int64), ("label", DataType::Utf8)]);
    let right_schema = schema(&[("rk", DataType::Int64), ("value", DataType::Int32)]);
    let left_batch = |keys: &[i64], labels: &[Option<&str>], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
            diffs,
        )
    };
    let right_batch = |keys: &[i64], values: &[i32], diffs: &[i64]| {
        zset(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_left_batch = left_batch(&[1, 1], &[Some("old"), None], &[2, 1]);
    let old_right_batch = right_batch(&[1, 2], &[7, 10], &[1, 1]);
    let left = arrangement(&old_left_batch, &[0]);
    let right = arrangement(&old_right_batch, &[0]);
    let delta_left = left_batch(&[1, 2], &[None, Some("new")], &[1, 1]);
    let delta_right = right_batch(&[1, 2], &[8, 10], &[1, -1]);
    let recognition = recognition(vec![], vec![]);
    let old_groups = empty_groups();
    let decision = prepare(PrepareInput {
        recognition: &recognition,
        aggregates: &[],
        inputs: DeltaPair::new(
            1,
            Arc::new(delta_left.clone()),
            Arc::new(delta_right.clone()),
        ),
        state: OldState {
            left: &left,
            right: &right,
            groups: &old_groups,
        },
        consumer: ConsumerCaps::flat_rows(2),
    });
    let prepared = match decision {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("valid flat rows cut rejected: {reason:?}"),
    };
    assert!(prepared.groups.is_empty());
    let actual = prepared.flat_rows.as_ref().expect("flat output prepared");

    let mut dense = Join::new(&[0], &[0]);
    dense.apply(&old_left_batch, &old_right_batch).unwrap();
    let expected = dense.apply(&delta_left, &delta_right).unwrap();
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(typed_nullable_rows(actual), typed_nullable_rows(&expected));
}

#[test]
fn canonical_flat_update_matches_dense_and_commits_rows_and_stats_only_after_materialization() {
    let left_schema = schema(&[("lk", DataType::Int64), ("label", DataType::Utf8)]);
    let right_schema = schema(&[("rk", DataType::Int64), ("value", DataType::Int32)]);
    let left_batch = |keys: &[i64], labels: &[Option<&str>], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
            diffs,
        )
    };
    let right_batch = |keys: &[i64], values: &[i32], diffs: &[i64]| {
        zset(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_left = left_batch(&[1, 1], &[Some("old"), None], &[2, 1]);
    let old_right = right_batch(&[1, 2], &[7, 10], &[1, 1]);
    let delta_left = left_batch(&[1, 2], &[None, Some("new")], &[1, 1]);
    let delta_right = right_batch(&[1, 2], &[8, 10], &[1, -1]);
    let demand = crate::ops::join::factorized::demand::Demand {
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![],
        left_columns: vec![1],
        right_columns: vec![1],
        left_column_types: vec![DataType::Utf8],
        right_column_types: vec![DataType::Int32],
        left_column_sums: vec![false],
        right_column_sums: vec![true],
    };
    let mut actual = Join::new(&[0], &[0]);
    let mut dense = Join::new(&[0], &[0]);
    actual.apply(&old_left, &old_right).unwrap();
    dense.apply(&old_left, &old_right).unwrap();
    actual.register_demand(demand.clone()).unwrap();
    dense.register_demand(demand).unwrap();
    let old_actual_left: Vec<_> = actual.left_for_cut().unwrap().iter().collect();
    let old_actual_right: Vec<_> = actual.right_for_cut().unwrap().iter().collect();
    let old_left_stats: Vec<_> = actual
        .left_for_cut()
        .unwrap()
        .iter_key_bytes()
        .map(|key| {
            (
                key.to_vec(),
                actual
                    .left_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone(),
            )
        })
        .collect();
    let old_right_stats: Vec<_> = actual
        .right_for_cut()
        .unwrap()
        .iter_key_bytes()
        .map(|key| {
            (
                key.to_vec(),
                actual
                    .right_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone(),
            )
        })
        .collect();

    let prepared = prepare_flat_join_update(&mut actual, &delta_left, &delta_right, 2).unwrap();
    let expected = dense.apply(&delta_left, &delta_right).unwrap();
    assert_eq!(prepared.output().schema(), expected.schema());
    assert_eq!(
        typed_nullable_rows(prepared.output()),
        typed_nullable_rows(&expected)
    );
    assert_eq!(
        actual.left_for_cut().unwrap().iter().collect::<Vec<_>>(),
        old_actual_left
    );
    assert_eq!(
        actual.right_for_cut().unwrap().iter().collect::<Vec<_>>(),
        old_actual_right
    );
    assert_eq!(
        actual
            .left_for_cut()
            .unwrap()
            .iter_key_bytes()
            .map(|key| (
                key.to_vec(),
                actual
                    .left_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone()
            ))
            .collect::<Vec<_>>(),
        old_left_stats
    );
    assert_eq!(
        actual
            .right_for_cut()
            .unwrap()
            .iter_key_bytes()
            .map(|key| (
                key.to_vec(),
                actual
                    .right_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone()
            ))
            .collect::<Vec<_>>(),
        old_right_stats
    );

    prepared.commit(&mut actual);
    for (actual_side, dense_side) in [
        (
            actual.left_for_cut().unwrap(),
            dense.left_for_cut().unwrap(),
        ),
        (
            actual.right_for_cut().unwrap(),
            dense.right_for_cut().unwrap(),
        ),
    ] {
        assert_eq!(
            actual_side.iter().collect::<Vec<_>>(),
            dense_side.iter().collect::<Vec<_>>()
        );
        for key in actual_side.iter_key_bytes() {
            assert_eq!(actual_side.source_stats(key), dense_side.source_stats(key));
        }
    }
}

#[test]
fn canonical_flat_key_only_duplicate_rows_and_retractions_match_dense_state() {
    let key_schema = schema(&[("k", DataType::Int64)]);
    let batch = |keys: &[i64], diffs: &[i64]| {
        zset(
            key_schema.clone(),
            vec![Arc::new(Int64Array::from(keys.to_vec()))],
            diffs,
        )
    };
    let initial_left = batch(&[7], &[1]);
    let initial_right = batch(&[7], &[1]);
    let demand = crate::ops::join::factorized::demand::Demand {
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![],
        left_columns: vec![],
        right_columns: vec![],
        left_column_types: vec![],
        right_column_types: vec![],
        left_column_sums: vec![],
        right_column_sums: vec![],
    };
    let mut actual = Join::new(&[0], &[0]);
    let mut dense = Join::new(&[0], &[0]);
    actual.apply(&initial_left, &initial_right).unwrap();
    dense.apply(&initial_left, &initial_right).unwrap();
    actual.register_demand(demand.clone()).unwrap();
    dense.register_demand(demand).unwrap();

    for (left_delta, right_delta) in [
        (batch(&[7, 7], &[1, 1]), batch(&[7, 7], &[1, 1])),
        (batch(&[7, 7], &[-1, -1]), batch(&[7, 7], &[-1, -1])),
    ] {
        let prepared = prepare_flat_join_update(&mut actual, &left_delta, &right_delta, 1).unwrap();
        let expected = dense.apply(&left_delta, &right_delta).unwrap();
        assert_eq!(prepared.output().schema(), expected.schema());
        assert_eq!(typed_rows(prepared.output()), typed_rows(&expected));
        prepared.commit(&mut actual);
        for (actual_side, dense_side) in [
            (
                actual.left_for_cut().unwrap(),
                dense.left_for_cut().unwrap(),
            ),
            (
                actual.right_for_cut().unwrap(),
                dense.right_for_cut().unwrap(),
            ),
        ] {
            assert_eq!(
                actual_side.iter().collect::<Vec<_>>(),
                dense_side.iter().collect::<Vec<_>>()
            );
            for key in actual_side.iter_key_bytes() {
                assert_eq!(actual_side.source_stats(key), dense_side.source_stats(key));
            }
        }
    }
}

#[test]
fn canonical_flat_late_pair_and_chunk_sum_errors_leave_rows_and_stats_unchanged() {
    let schema = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let batch = |weights: &[i64]| {
        zset(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7; weights.len()])),
                Arc::new(Int64Array::from(vec![1; weights.len()])),
            ],
            weights,
        )
    };
    let demand = crate::ops::join::factorized::demand::Demand {
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![],
        left_columns: vec![1],
        right_columns: vec![1],
        left_column_types: vec![DataType::Int64],
        right_column_types: vec![DataType::Int64],
        left_column_sums: vec![true],
        right_column_sums: vec![true],
    };
    let mut actual = Join::new(&[0], &[0]);
    let mut dense = Join::new(&[0], &[0]);
    actual.apply(&batch(&[1]), &batch(&[i64::MAX])).unwrap();
    dense.apply(&batch(&[1]), &batch(&[i64::MAX])).unwrap();
    actual.register_demand(demand.clone()).unwrap();
    dense.register_demand(demand).unwrap();
    let before = |join: &Join| {
        [join.left_for_cut().unwrap(), join.right_for_cut().unwrap()].map(|side| {
            (
                side.iter().collect::<Vec<_>>(),
                side.iter_key_bytes()
                    .map(|key| (key.to_vec(), side.source_stats(key).unwrap().clone()))
                    .collect::<Vec<_>>(),
            )
        })
    };
    let old_state = before(&actual);

    let overflow_delta = batch(&[2]);
    let actual_error = match prepare_flat_join_update(&mut actual, &overflow_delta, &batch(&[]), 1)
    {
        Ok(_) => panic!("flat preparation must reject the overflowing pair product"),
        Err(error) => error,
    };
    let dense_error = match dense.apply(&overflow_delta, &batch(&[])) {
        Ok(_) => panic!("Dense join must reject the overflowing pair product"),
        Err(error) => error,
    };
    assert!(matches!(
        actual_error,
        crate::error::EngineError::NumericOverflow("join diff product")
    ));
    assert_eq!(actual_error.to_string(), dense_error.to_string());
    assert_eq!(before(&actual), old_state);

    // Every pair product fits, but consolidating the three formula terms for
    // the same joined row overflows after the first one-row materialization.
    let mut actual = Join::new(&[0], &[0]);
    let mut dense = Join::new(&[0], &[0]);
    actual.apply(&batch(&[1]), &batch(&[1])).unwrap();
    dense.apply(&batch(&[1]), &batch(&[1])).unwrap();
    actual
        .register_demand(crate::ops::join::factorized::demand::Demand {
            left_join_key: vec![0],
            right_join_key: vec![0],
            group_key: vec![],
            left_columns: vec![],
            right_columns: vec![],
            left_column_types: vec![],
            right_column_types: vec![],
            left_column_sums: vec![],
            right_column_sums: vec![],
        })
        .unwrap();
    let old_state = before(&actual);
    let left_delta = batch(&[i64::MAX - 1]);
    let right_delta = batch(&[1]);
    let actual_error = match prepare_flat_join_update(&mut actual, &left_delta, &right_delta, 1) {
        Ok(_) => panic!("flat collector must reject the overflowing consolidated diff"),
        Err(error) => error,
    };
    let dense_error = match dense.apply(&left_delta, &right_delta) {
        Ok(_) => panic!("Dense join must reject the overflowing consolidated diff"),
        Err(error) => error,
    };
    assert!(matches!(
        actual_error,
        crate::error::EngineError::Infrastructure(ref message)
            if message == "consolidated diff sum overflowed i64"
    ));
    assert_eq!(actual_error.to_string(), dense_error.to_string());
    assert_eq!(before(&actual), old_state);
}

#[test]
fn canonical_flat_float_nullable_payloads_match_dense_across_bilateral_updates() {
    let left_schema = schema(&[("lk", DataType::Int64), ("value", DataType::Float64)]);
    let right_schema = schema(&[("rk", DataType::Int64), ("label", DataType::Utf8)]);
    let left = |values: &[Option<f64>], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4; values.len()])),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let right = |values: &[Option<&str>], diffs: &[i64]| {
        zset(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4; values.len()])),
                Arc::new(StringArray::from(values.to_vec())),
            ],
            diffs,
        )
    };
    let old_left = left(&[Some(1.5), None], &[1, -1]);
    let old_right = right(&[None, Some("old")], &[2, 1]);
    let delta_left = left(&[None, Some(3.0)], &[1, -1]);
    let delta_right = right(&[Some("new"), None], &[1, -1]);
    let mut actual = Join::new(&[0], &[0]);
    let mut dense = Join::new(&[0], &[0]);
    actual.apply(&old_left, &old_right).unwrap();
    dense.apply(&old_left, &old_right).unwrap();
    let prepared = prepare_flat_join_update(&mut actual, &delta_left, &delta_right, 1).unwrap();
    let expected = dense.apply(&delta_left, &delta_right).unwrap();
    assert_eq!(prepared.output().schema(), expected.schema());
    assert_eq!(canonical_rows(prepared.output()), canonical_rows(&expected));
    prepared.commit(&mut actual);
    for (actual_side, dense_side) in [
        (
            actual.left_for_cut().unwrap(),
            dense.left_for_cut().unwrap(),
        ),
        (
            actual.right_for_cut().unwrap(),
            dense.right_for_cut().unwrap(),
        ),
    ] {
        assert_eq!(
            actual_side.iter().collect::<Vec<_>>(),
            dense_side.iter().collect::<Vec<_>>()
        );
    }
}

#[test]
fn canonical_flat_keeps_dense_per_term_consolidation_boundaries() {
    let schema = schema(&[("k", DataType::Int64)]);
    let batch = |weights: &[i64]| {
        zset(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1; weights.len()]))],
            weights,
        )
    };

    // Each Dense term consolidates independently: its cross term cancels to
    // zero, so the overall delta is MAX-1. Combining raw rows across terms
    // instead overflows before reaching the cancelling rows.
    let left_delta = batch(&[i64::MAX - 1]);
    let right_delta = batch(&[1, -1]);
    let mut dense = Join::new(&[0], &[0]);
    let mut flat = Join::new(&[0], &[0]);
    dense.apply(&batch(&[1]), &batch(&[1])).unwrap();
    flat.apply(&batch(&[1]), &batch(&[1])).unwrap();
    let expected = dense.apply(&left_delta, &right_delta).unwrap();
    let prepared = prepare_flat_join_update(&mut flat, &left_delta, &right_delta, 1)
        .expect("flat must preserve Dense term consolidation boundaries");
    assert_eq!(typed_rows(prepared.output()), typed_rows(&expected));
    assert_eq!(
        prepared
            .output()
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[i64::MAX - 1]
    );
    prepared.commit(&mut flat);

    // Term two overflows on its own: old_left * (+1,+1) consolidates to
    // 2K > MAX, although combining it with the first term's -K would fit.
    let old_left_weight = i64::MAX / 2 + 1;
    let left_delta = batch(&[-old_left_weight]);
    let right_delta = batch(&[1, 1]);
    let mut dense = Join::new(&[0], &[0]);
    let mut flat = Join::new(&[0], &[0]);
    dense
        .apply(&batch(&[old_left_weight]), &batch(&[1]))
        .unwrap();
    flat.apply(&batch(&[old_left_weight]), &batch(&[1]))
        .unwrap();
    let dense_result = dense.apply(&left_delta, &right_delta);
    let flat_result = prepare_flat_join_update(&mut flat, &left_delta, &right_delta, 1);
    assert_eq!(
        flat_result.err().map(|err| err.to_string()),
        dense_result.as_ref().err().map(|err| err.to_string())
    );
    assert!(matches!(
        dense_result,
        Err(crate::error::EngineError::Infrastructure(_))
    ));
}

#[test]
fn canonical_flat_checks_key_result_before_materializing_the_next_key() {
    let schema = schema(&[("k", DataType::Int64)]);
    let batch = |keys: &[i64], weights: &[i64]| {
        zset(
            schema.clone(),
            vec![Arc::new(Int64Array::from(keys.to_vec()))],
            weights,
        )
    };
    let old_left = batch(&[1, 2], &[1, 1]);
    let old_right = batch(&[1, 2], &[1, i64::MAX]);
    let delta_left = batch(&[1, 2], &[i64::MAX - 1, 2]);
    let delta_right = batch(&[1], &[1]);
    let mut dense = Join::new(&[0], &[0]);
    let mut flat = Join::new(&[0], &[0]);
    dense.apply(&old_left, &old_right).unwrap();
    flat.apply(&old_left, &old_right).unwrap();

    let dense_error = match dense.apply(&delta_left, &delta_right) {
        Ok(_) => panic!("Dense is expected to fail at the earlier key consolidation"),
        Err(error) => error,
    };
    let flat_error = prepare_flat_join_update(&mut flat, &delta_left, &delta_right, 1024)
        .err()
        .expect("flat must stop at Dense's earlier key-level consolidation error");
    assert_eq!(flat_error.to_string(), dense_error.to_string());
    assert!(matches!(
        dense_error,
        crate::error::EngineError::Infrastructure(ref message)
            if message == "consolidated diff sum overflowed i64"
    ));
}

#[test]
fn canonical_flat_checks_all_products_in_a_term_before_consolidating_that_term() {
    let schema = schema(&[("k", DataType::Int64)]);
    let batch = |keys: &[i64], weights: &[i64]| {
        zset(
            schema.clone(),
            vec![Arc::new(Int64Array::from(keys.to_vec()))],
            weights,
        )
    };
    let k = i64::MAX / 2 + 1;
    let old_left = batch(&[1], &[1]);
    let old_right = batch(&[1], &[k]);
    let mut delta_left_weights = vec![1; 1026];
    delta_left_weights.push(2);
    let delta_left = batch(&vec![1; delta_left_weights.len()], &delta_left_weights);
    let delta_right = batch(&[], &[]);
    let mut dense = Join::new(&[0], &[0]);
    let mut flat = Join::new(&[0], &[0]);
    dense.apply(&old_left, &old_right).unwrap();
    flat.apply(&old_left, &old_right).unwrap();

    // The first 1026 products are individually valid, but the second same-row
    // product makes their eventual consolidation overflow. The final pair
    // product overflows too; Dense computes all products before consolidating.
    let dense_error = match dense.apply(&delta_left, &delta_right) {
        Ok(_) => panic!("Dense is expected to fail on the later pair product"),
        Err(error) => error,
    };
    let flat_error = prepare_flat_join_update(&mut flat, &delta_left, &delta_right, 1024)
        .err()
        .expect("flat must evaluate every product in the term before term consolidation");
    assert_eq!(flat_error.to_string(), dense_error.to_string());
    assert!(matches!(
        dense_error,
        crate::error::EngineError::NumericOverflow("join diff product")
    ));
}

fn canonical_rows(z: &ZSetBatch) -> Vec<(Vec<u8>, i64)> {
    let converter = crate::keys::full_converter(z.schema().as_ref()).unwrap();
    let rows = converter.convert_columns(z.batch.columns()).unwrap();
    let diffs = z.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|index| (rows.row(index).as_ref().to_vec(), diffs.value(index)))
        .collect()
}

#[test]
fn flat_rows_unsafe_full_key_bounds_fallback_before_staging_or_publication() {
    let old_left_batch = simple(&[10], &[1]);
    let old_right_batch = simple(&[20], &[1]);
    let left = arrangement(&old_left_batch, &[0]);
    let right = arrangement(&old_right_batch, &[0]);
    let old_left = left.to_zset().unwrap();
    let old_right = right.to_zset().unwrap();
    let recognition = recognition(vec![], vec![]);
    let groups = empty_groups();
    let decision = prepare(PrepareInput {
        recognition: &recognition,
        aggregates: &[],
        inputs: DeltaPair::new(
            2,
            Arc::new(simple(&[10], &[i64::MAX - 2])),
            Arc::new(simple(&[20], &[1])),
        ),
        state: OldState {
            left: &left,
            right: &right,
            groups: &groups,
        },
        consumer: ConsumerCaps::flat_rows(1),
    });
    assert!(matches!(
        decision,
        CutDecision::Dense(DenseReason::NegativeOrOverflow)
    ));
    assert_eq!(
        typed_i64_rows(&left.to_zset().unwrap()),
        typed_i64_rows(&old_left)
    );
    assert_eq!(
        typed_i64_rows(&right.to_zset().unwrap()),
        typed_i64_rows(&old_right)
    );
}

#[test]
fn flat_rows_key_only_and_empty_inputs_return_typed_owned_batches() {
    let key_schema = schema(&[("k", DataType::Int64)]);
    let left_empty = zset(
        key_schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        &[],
    );
    let right_empty = left_empty.clone();
    let left = arrangement(&left_empty, &[0]);
    let right = arrangement(&right_empty, &[0]);
    let mapping = recognition(vec![], vec![]);
    let groups = empty_groups();
    let prepare_flat = |left_delta: ZSetBatch, right_delta: ZSetBatch| {
        prepare(PrepareInput {
            recognition: &mapping,
            aggregates: &[],
            inputs: DeltaPair::new(3, Arc::new(left_delta), Arc::new(right_delta)),
            state: OldState {
                left: &left,
                right: &right,
                groups: &groups,
            },
            consumer: ConsumerCaps::flat_rows(ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS),
        })
    };

    let one_key = zset(
        key_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![7]))],
        &[1],
    );
    let prepared = match prepare_flat(one_key.clone(), one_key) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("key-only flat cut rejected: {reason:?}"),
    };
    let output = prepared.flat_rows.unwrap();
    assert_eq!(output.schema().fields().len(), 2);
    assert_eq!(output.len(), 1);
    assert_eq!(
        output
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );

    let prepared = match prepare_flat(left_empty.clone(), right_empty.clone()) {
        CutDecision::Eligible(prepared) => prepared,
        CutDecision::Dense(reason) => panic!("empty flat cut rejected: {reason:?}"),
    };
    let output = prepared.flat_rows.unwrap();
    assert_eq!(output.len(), 0);
    assert_eq!(output.schema().fields().len(), 2);
}

#[test]
fn flat_rows_prepare_rejects_invalid_chunk_configurations_without_panicking() {
    let empty = simple(&[], &[]);
    let left = arrangement(&empty, &[0]);
    let right = arrangement(&empty, &[0]);
    let mapping = recognition(vec![], vec![]);
    for max_rows in [0, usize::MAX] {
        let decision = prepare(PrepareInput {
            recognition: &mapping,
            aggregates: &[],
            inputs: DeltaPair::new(4, Arc::new(empty.clone()), Arc::new(empty.clone())),
            state: OldState {
                left: &left,
                right: &right,
                groups: &empty_groups(),
            },
            consumer: ConsumerCaps::flat_rows(max_rows),
        });
        assert!(matches!(
            decision,
            CutDecision::Dense(DenseReason::InvalidChunkSize)
        ));
    }
}

#[test]
fn flat_rows_cross_term_cancellation_is_consolidated_to_empty() {
    super::super::FLAT_PAIR_PRODUCTS.with(|count| count.set(0));
    let s = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let row = |weight| {
        zset(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
            ],
            &[weight],
        )
    };
    let old_left_batch = row(1);
    let old_right_batch = zset(
        s.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
        ],
        &[2],
    );
    let left = arrangement(&old_left_batch, &[0]);
    let right = arrangement(&old_right_batch, &[0]);
    let cursor = FlatRowsCursor::from_join_delta(
        &left,
        &right,
        &row(1),
        &zset(
            s,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![20])),
            ],
            &[-1],
        ),
        &[0],
        &[0],
        1,
    )
    .unwrap();
    let output = cursor.collect().unwrap();
    assert!(
        output.is_empty(),
        "the three identical full-row terms sum to zero"
    );
    super::super::FLAT_PAIR_PRODUCTS.with(|count| assert!(count.get() > 0));
}

fn typed_i64_rows(z: &ZSetBatch) -> Vec<(i64, i64, i64)> {
    let key = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let value = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diff = z.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|i| (key.value(i), value.value(i), diff.value(i)))
        .collect()
}

fn typed_nullable_rows(z: &ZSetBatch) -> Vec<(i64, Option<String>, i64, i32, i64)> {
    let lk = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let label = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rk = z
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let value = z
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let diff = z.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|i| {
            (
                lk.value(i),
                label.is_valid(i).then(|| label.value(i).to_owned()),
                rk.value(i),
                value.value(i),
                diff.value(i),
            )
        })
        .collect()
}
