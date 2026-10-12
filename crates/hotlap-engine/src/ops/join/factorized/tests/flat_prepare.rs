use super::*;
use crate::ops::join::Join;
use crate::ops::join::factorized::flat::FlatRowsCursor;
use arrow::array::{Array, Int32Array, Int64Array, StringArray};

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
    let old_groups = [];
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
fn flat_rows_unsafe_full_key_bounds_fallback_before_staging_or_publication() {
    let old_left_batch = simple(&[10], &[1]);
    let old_right_batch = simple(&[20], &[1]);
    let left = arrangement(&old_left_batch, &[0]);
    let right = arrangement(&old_right_batch, &[0]);
    let old_left = left.to_zset().unwrap();
    let old_right = right.to_zset().unwrap();
    let recognition = recognition(vec![], vec![]);
    let groups = [];
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
    let groups = [];
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
                groups: &[],
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
