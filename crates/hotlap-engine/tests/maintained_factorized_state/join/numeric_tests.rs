use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, i256};
use arrow::record_batch::RecordBatch;
use hotlap_core::plan::AggSpec;
use hotlap_engine::ZSetBatch;
use hotlap_engine::ops::{GroupAggregate, Join};

use super::{Change, JoinGate, JoinView};

fn left_batch(rows: &[(i64, Option<i64>, i64)]) -> ZSetBatch {
    make_batch(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
        rows.iter().map(|row| row.2).collect(),
    )
}

fn right_batch(rows: &[(i64, i64)]) -> ZSetBatch {
    make_batch(
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )) as ArrayRef],
        rows.iter().map(|row| row.1).collect(),
    )
}

fn make_batch(schema: SchemaRef, columns: Vec<ArrayRef>, diffs: Vec<i64>) -> ZSetBatch {
    ZSetBatch::new(
        RecordBatch::try_new(schema, columns).unwrap(),
        Arc::new(Int64Array::from(diffs)),
    )
    .unwrap()
}

fn grouped(joined: &ZSetBatch) -> Result<ZSetBatch, hotlap_engine::EngineError> {
    let mut aggregate = GroupAggregate::new(
        &[0],
        vec![
            AggSpec::count(),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(1),
            },
            AggSpec::sum(1),
        ],
    );
    aggregate.apply(joined)
}

#[test]
fn fpzb_join_null_left_count_times_right_weight_needs_dense_evaluation() {
    let mut view = JoinView::default();
    view.update(
        &[Change {
            id: (1, 1),
            value: None,
            delta: i64::MAX,
        }],
        &[Change {
            id: (1, 1),
            value: None,
            delta: 2,
        }],
    )
    .unwrap();
    assert_eq!(
        view.by_group[&1].count,
        i256::from_i128(2 * i128::from(i64::MAX))
    );
    assert_eq!(view.by_group[&1].left_nonnull, i256::ZERO);
    assert_eq!(view.gate, JoinGate::NeedsRowEvaluation);

    let mut join = Join::new(&[0], &[0]);
    assert!(
        join.apply(&left_batch(&[(1, None, i64::MAX)]), &right_batch(&[(1, 2)]))
            .is_err()
    );
}

#[test]
fn fpzb_join_sum_bound_rejects_renderer_overflow_but_keeps_wide_total() {
    let mut view = JoinView::default();
    view.update(
        &[Change {
            id: (2, 1),
            value: Some(i64::MAX),
            delta: 1,
        }],
        &[Change {
            id: (2, 1),
            value: None,
            delta: 2,
        }],
    )
    .unwrap();
    assert_eq!(
        view.by_group[&2].left_sum,
        i256::from_i128(2 * i128::from(i64::MAX))
    );
    assert_eq!(view.gate, JoinGate::NeedsRowEvaluation);

    let mut join = Join::new(&[0], &[0]);
    let joined = join
        .apply(
            &left_batch(&[(2, Some(i64::MAX), 1)]),
            &right_batch(&[(2, 2)]),
        )
        .unwrap();
    assert!(grouped(&joined).is_err());
}

#[test]
fn fpzb_join_raw_abs_delta_bound_rejects_count_prefix_overflow() {
    let mut view = JoinView::default();
    view.update(
        &[],
        &[Change {
            id: (3, 1),
            value: None,
            delta: 1,
        }],
    )
    .unwrap();
    view.update(
        &[Change {
            id: (3, 1),
            value: None,
            delta: i64::MAX,
        }],
        &[],
    )
    .unwrap();
    assert_eq!(view.gate, JoinGate::Certified);
    let raw = [
        Change {
            id: (3, 2),
            value: Some(0),
            delta: 1,
        },
        Change {
            id: (3, 3),
            value: None,
            delta: -1,
        },
    ];
    view.update(&raw, &[]).unwrap();
    assert_eq!(
        view.by_group[&3].count,
        i256::from_i128(i128::from(i64::MAX))
    );
    assert_eq!(view.gate, JoinGate::NeedsRowEvaluation);

    let mut join = Join::new(&[0], &[0]);
    let mut aggregate = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    let joined = join
        .apply(&left_batch(&[(3, None, i64::MAX)]), &right_batch(&[(3, 1)]))
        .unwrap();
    aggregate.apply(&joined).unwrap();
    assert!(
        join.apply(
            &left_batch(&[(3, None, 1), (3, Some(0), -1)]),
            &right_batch(&[]),
        )
        .is_err()
    );

    let mut dense = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    dense.apply(&left_batch(&[(3, None, i64::MAX)])).unwrap();
    assert!(
        dense
            .apply(&left_batch(&[(3, None, 1), (3, Some(0), -1)]))
            .is_err()
    );
}

#[test]
fn fpzb_join_nonnegative_physical_rows_remain_certifiable() {
    let mut view = JoinView::default();
    view.update(
        &[
            Change {
                id: (4, 1),
                value: None,
                delta: 2,
            },
            Change {
                id: (4, 2),
                value: None,
                delta: 1,
            },
        ],
        &[Change {
            id: (4, 1),
            value: None,
            delta: 1,
        }],
    )
    .unwrap();
    assert_eq!(view.left.by_key[&4].weight, 3);
    assert_eq!(view.left.by_key[&4].negative_rows, 0);
    assert_eq!(view.gate, JoinGate::Certified);

    let mut join = Join::new(&[0], &[0]);
    let joined = join
        .apply(
            &left_batch(&[(4, None, 2), (4, None, 1)]),
            &right_batch(&[(4, 1)]),
        )
        .unwrap();
    assert!(grouped(&joined).is_ok());
}

#[test]
fn fpzb_join_abs_sum_oversize_recovers_after_one_keyed_retraction() {
    let mut view = JoinView::default();
    view.update(
        &[
            Change {
                id: (5, 1),
                value: Some(i64::MAX),
                delta: 1,
            },
            Change {
                id: (5, 2),
                value: Some(-i64::MAX),
                delta: 1,
            },
        ],
        &[Change {
            id: (5, 1),
            value: None,
            delta: 1,
        }],
    )
    .unwrap();
    assert_eq!(view.gate, JoinGate::NeedsRowEvaluation);
    let before = &view.by_group[&5];
    assert_eq!(before.count, i256::from_i128(2));
    assert_eq!(before.left_sum, i256::ZERO);

    view.update(
        &[Change {
            id: (5, 2),
            value: Some(-i64::MAX),
            delta: -1,
        }],
        &[],
    )
    .unwrap();
    assert_eq!(view.gate, JoinGate::Certified);
    assert_eq!(view.by_group[&5].count, i256::ONE);
    assert_eq!(
        view.by_group[&5].left_sum,
        i256::from_i128(i128::from(i64::MAX))
    );
    assert_eq!(view.left.work.canonical_row_reads, 1);
    assert_eq!(view.left.work.summary_copies, 1);
}
