use super::*;
use crate::batch::ZSetBatch;
use crate::ops::join::factorized::flat::FlatRowsCursor;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

type TypedFlatRow = (Option<i32>, Option<i64>, Option<i32>, Option<String>, i64);

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn dense_rows(z: &ZSetBatch) -> Vec<TypedFlatRow> {
    let key = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let value = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_key = z
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let text = z
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let diff = z.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|i| {
            (
                key.is_valid(i).then(|| key.value(i)),
                value.is_valid(i).then(|| value.value(i)),
                right_key.is_valid(i).then(|| right_key.value(i)),
                text.is_valid(i).then(|| text.value(i).to_owned()),
                diff.value(i),
            )
        })
        .collect()
}

fn rows_i64(z: &ZSetBatch) -> Vec<(i64, i64, i64, i64, i64)> {
    let columns = (0..4)
        .map(|column| {
            z.batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let diff = z.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|i| {
            (
                columns[0].value(i),
                columns[1].value(i),
                columns[2].value(i),
                columns[3].value(i),
                diff.value(i),
            )
        })
        .collect()
}

#[test]
fn cursor_emits_exact_full_rows_with_signed_checked_products_in_bounded_chunks() {
    let left_schema = schema(&[("lk", DataType::Int32), ("payload", DataType::Int64)]);
    let right_schema = schema(&[("rk", DataType::Int32), ("label", DataType::Utf8)]);
    let left = zset(
        left_schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(7), None])),
            Arc::new(Int64Array::from(vec![Some(11), None])),
        ],
        &[2, -1],
    );
    let right = zset(
        right_schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(7), None])),
            strings(&[Some("a"), None]),
        ],
        &[3, 4],
    );
    let mut cursor = FlatRowsCursor::new(&left, &right, 1).unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = cursor.next_chunk().unwrap() {
        assert!(chunk.len() <= 1);
        assert_eq!(chunk.schema().fields().len(), 4);
        assert_eq!(chunk.schema().field(0).name(), "lk");
        assert_eq!(chunk.schema().field(2).name(), "rk");
        chunks.extend(dense_rows(&chunk));
    }
    assert_eq!(
        chunks,
        vec![
            (Some(7), Some(11), Some(7), Some("a".into()), 6),
            (None, None, Some(7), Some("a".into()), -3),
            (Some(7), Some(11), None, None, 8),
            (None, None, None, None, -4),
        ]
    );
}

#[test]
fn cursor_rejects_zero_chunk_size() {
    let left = crate::ops::join::factorized::tests::simple(&[], &[]);
    assert!(FlatRowsCursor::new(&left, &left, 0).is_err());
    assert!(FlatRowsCursor::new(&left, &left, usize::MAX).is_err());
}

#[test]
fn cursor_returns_dense_typed_error_instead_of_wrapping_pair_weight() {
    let left = simple(&[10], &[i64::MAX]);
    let right = simple(&[20], &[2]);
    let mut cursor = FlatRowsCursor::new(&left, &right, 1).unwrap();
    assert!(matches!(
        cursor.next_chunk(),
        Err(crate::error::EngineError::NumericOverflow(
            "join diff product"
        ))
    ));
}

#[test]
fn factor_terms_match_same_epoch_dense_delta_formula_across_keys_and_chunks() {
    let s = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let batch = |keys: &[i64], values: &[i64], weights: &[i64]| {
        zset(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            weights,
        )
    };
    let old_l = batch(&[1, 2], &[10, 20], &[2, 1]);
    let old_r = batch(&[1, 2], &[100, 200], &[3, 2]);
    let delta_l = batch(&[1, 3], &[11, 30], &[1, 2]);
    let delta_r = batch(&[1, 2, 3], &[101, 201, 301], &[-1, 1, 1]);
    let left = arrangement(&old_l, &[0]);
    let right = arrangement(&old_r, &[0]);
    let expected = vec![
        (1, 11, 1, 100, 3),
        (1, 10, 1, 101, -2),
        (1, 11, 1, 101, -1),
        (3, 30, 3, 301, 2),
        (2, 20, 2, 201, 1),
    ];
    for limit in [1, 2] {
        let mut cursor =
            FlatRowsCursor::from_join_delta(&left, &right, &delta_l, &delta_r, &[0], &[0], limit)
                .unwrap();
        let mut actual = Vec::new();
        while let Some(chunk) = cursor.next_chunk().unwrap() {
            assert!(chunk.len() <= limit);
            let lk = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let lv = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let rk = chunk
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let rv = chunk
                .batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let diff = chunk.diff().as_any().downcast_ref::<Int64Array>().unwrap();
            actual.extend((0..chunk.len()).map(|i| {
                (
                    lk.value(i),
                    lv.value(i),
                    rk.value(i),
                    rv.value(i),
                    diff.value(i),
                )
            }));
        }
        assert_eq!(actual, expected);
    }
}

#[test]
fn factor_terms_handle_empty_sides_and_unmatched_delta_keys() {
    let s = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let batch = |keys: &[i64], values: &[i64], weights: &[i64]| {
        zset(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            weights,
        )
    };
    let old_l = batch(&[1], &[10], &[2]);
    let old_r = batch(&[1], &[100], &[3]);
    let left = arrangement(&old_l, &[0]);
    let right = arrangement(&old_r, &[0]);
    let empty = batch(&[], &[], &[]);

    let mut left_only = FlatRowsCursor::from_join_delta(
        &left,
        &right,
        &batch(&[1, 2], &[11, 20], &[1, 1]),
        &empty,
        &[0],
        &[0],
        2,
    )
    .unwrap();
    let first = left_only.next_chunk().unwrap().unwrap();
    assert_eq!(rows_i64(&first), vec![(1, 11, 1, 100, 3)]);
    assert!(left_only.next_chunk().unwrap().is_none());

    let mut right_only = FlatRowsCursor::from_join_delta(
        &left,
        &right,
        &empty,
        &batch(&[1, 2], &[101, 200], &[-1, 1]),
        &[0],
        &[0],
        1,
    )
    .unwrap();
    let chunk = right_only.next_chunk().unwrap().unwrap();
    assert_eq!(rows_i64(&chunk), vec![(1, 10, 1, 101, -2)]);
    assert!(right_only.next_chunk().unwrap().is_none());

    let mut no_terms =
        FlatRowsCursor::from_join_delta(&left, &right, &empty, &empty, &[0], &[0], 1).unwrap();
    assert!(no_terms.next_chunk().unwrap().is_none());
}

#[test]
fn collector_consolidates_across_chunks_and_discards_partial_result_on_overflow() {
    let left = simple(&[10, 10], &[1, -1]);
    let right = simple(&[20], &[1]);
    let empty = FlatRowsCursor::new(&left, &right, 1)
        .unwrap()
        .collect()
        .unwrap();
    assert!(empty.is_empty(), "cross-chunk cancellation removes the row");

    let overflowing_left = simple(&[10, 10], &[i64::MAX, 1]);
    let err = match FlatRowsCursor::new(&overflowing_left, &right, 1)
        .unwrap()
        .collect()
    {
        Ok(_) => panic!("collector must return no partial result"),
        Err(error) => error,
    };
    assert!(matches!(
        err,
        crate::error::EngineError::Infrastructure(message)
            if message == "consolidated diff sum overflowed i64"
    ));

    let old_left_batch = simple(&[10], &[1]);
    let old_right_batch = simple(&[20], &[1]);
    let old_left = arrangement(&old_left_batch, &[0]);
    let old_right = arrangement(&old_right_batch, &[0]);
    let before_left = typed_rows(&old_left.to_zset().unwrap());
    let before_right = typed_rows(&old_right.to_zset().unwrap());
    let cursor = FlatRowsCursor::from_join_delta(
        &old_left,
        &old_right,
        &simple(&[10], &[i64::MAX - 1]),
        &simple(&[20], &[1]),
        &[0],
        &[0],
        1,
    )
    .unwrap();
    assert!(matches!(
        cursor.collect(),
        Err(crate::error::EngineError::Infrastructure(message))
            if message == "consolidated diff sum overflowed i64"
    ));
    assert_eq!(typed_rows(&old_left.to_zset().unwrap()), before_left);
    assert_eq!(typed_rows(&old_right.to_zset().unwrap()), before_right);
}

fn typed_rows(z: &ZSetBatch) -> Vec<(i64, i64, i64)> {
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
