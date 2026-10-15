use std::sync::Arc;

use arrow::array::FixedSizeListArray;
use arrow::array::{
    Array, BooleanArray, Int16Array, Int64Array, RunArray, StringArray, UnionArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, Field, Schema, UnionFields, UnionMode};
use arrow::record_batch::RecordBatch;

use super::{filter, filter_project, project};
use crate::batch::ZSetBatch;

fn nested_input() -> (Arc<Schema>, ZSetBatch) {
    let nested = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        1,
        Arc::new(Int64Array::from(vec![1, 2])) as _,
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("ok", DataType::Int64, false),
        Field::new("unsupported", nested.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])), nested],
    )
    .unwrap();
    let input = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1, 1]))).unwrap();
    (schema, input)
}

#[test]
fn selected_filter_projection_matches_dense_pipeline() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Int64, true),
        Field::new("wide", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(3), None, Some(5), Some(6)])),
            Arc::new(StringArray::from(vec![
                Some("long-a"),
                Some("long-b"),
                None,
                Some("long-d"),
            ])),
        ],
    )
    .unwrap();
    let input = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![2, -3, 0, 4]))).unwrap();
    let mask = BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]);
    let columns = [1, 0, 0];

    let actual = filter_project(&input, &mask, &columns).unwrap();
    let expected = project(&filter(&input, &mask).unwrap(), &columns).unwrap();
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.batch, expected.batch);
    assert_eq!(actual.diff.as_ref(), expected.diff.as_ref());
    assert_eq!(actual.schema().field(0), expected.schema().field(0));
    assert_eq!(actual.schema().field(1), expected.schema().field(1));
    assert_eq!(actual.batch.column(0).len(), 2);
    assert_eq!(input.batch.column(1).len(), 4);

    for mask in [
        BooleanArray::from(vec![true, true, true, true]),
        BooleanArray::from(vec![false, false, false, false]),
        BooleanArray::from(vec![true, false, true, false]),
    ] {
        let selected = filter_project(&input, &mask, &[0]).unwrap();
        let dense = project(&filter(&input, &mask).unwrap(), &[0]).unwrap();
        assert_eq!(selected.batch, dense.batch);
        assert_eq!(selected.diff.as_ref(), dense.diff.as_ref());
    }
    let empty = ZSetBatch::empty(input.schema());
    let empty_mask = BooleanArray::from(Vec::<bool>::new());
    let selected_empty = filter_project(&empty, &empty_mask, &[0]).unwrap();
    let dense_empty = project(&filter(&empty, &empty_mask).unwrap(), &[0]).unwrap();
    assert_eq!(selected_empty.schema(), dense_empty.schema());
    assert_eq!(selected_empty.batch, dense_empty.batch);
    assert_eq!(selected_empty.diff.as_ref(), dense_empty.diff.as_ref());
}

#[test]
fn repeated_projection_shares_filtered_arrays_like_project() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("pass", DataType::Boolean, false),
        Field::new("wide", DataType::Utf8, true),
        Field::new("discard", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                None,
                Some("d"),
            ])),
            Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30, 40])),
        ],
    )
    .unwrap();
    let input = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![2, -3, 0, 4]))).unwrap();
    let mask = BooleanArray::from(vec![true, false, true, false]);
    let cols = [2, 0, 2, 2];

    let actual = filter_project(&input, &mask, &cols).unwrap();
    let expected = project(&filter(&input, &mask).unwrap(), &cols).unwrap();
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.batch, expected.batch);
    assert_eq!(actual.diff.to_data(), expected.diff.to_data());
    assert!(Arc::ptr_eq(actual.batch.column(0), actual.batch.column(2)));
    assert!(Arc::ptr_eq(actual.batch.column(2), actual.batch.column(3)));
    assert!(Arc::ptr_eq(
        expected.batch.column(0),
        expected.batch.column(2)
    ));
    assert!(Arc::ptr_eq(
        expected.batch.column(2),
        expected.batch.column(3)
    ));
    for (actual, expected) in actual.batch.columns().iter().zip(expected.batch.columns()) {
        assert_eq!(actual.to_data(), expected.to_data());
    }
    assert_eq!(actual.diff.as_ref(), &Int64Array::from(vec![2, 0]));
}

#[test]
fn wide_projection_index_cache_preserves_distinct_and_repeated_columns() {
    const WIDTH: usize = 256;
    let fields = (0..WIDTH)
        .map(|index| Field::new(format!("c{index}"), DataType::Int64, index % 7 == 0))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let arrays = (0..WIDTH)
        .map(|column| {
            Arc::new(Int64Array::from(vec![
                Some(column as i64),
                if column % 7 == 0 {
                    None
                } else {
                    Some(-(column as i64))
                },
                Some((column * 2) as i64),
                Some((column * 3) as i64),
            ])) as _
        })
        .collect();
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    let input = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![3, -2, 0, 5]))).unwrap();
    let mask = BooleanArray::from(vec![true, false, true, false]);
    let mut cols = (0..WIDTH).rev().collect::<Vec<_>>();
    cols.extend_from_slice(&[17, 255, 0, 17, 128]);

    let actual = filter_project(&input, &mask, &cols).unwrap();
    let expected = project(&filter(&input, &mask).unwrap(), &cols).unwrap();
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.batch, expected.batch);
    assert_eq!(actual.diff.to_data(), expected.diff.to_data());
    for (actual, expected) in actual.batch.columns().iter().zip(expected.batch.columns()) {
        assert_eq!(actual.to_data(), expected.to_data());
    }
    for (position, &column) in cols.iter().enumerate() {
        let first = cols
            .iter()
            .position(|&candidate| candidate == column)
            .unwrap();
        assert!(Arc::ptr_eq(
            actual.batch.column(position),
            actual.batch.column(first)
        ));
    }
}

#[test]
fn fused_kernel_counter_only_counts_capability_eligible_fast_path() {
    super::reset_fused_filter_project_kernel_count();
    let (schema, input) = nested_input();
    filter_project(&input, &BooleanArray::from(vec![true, false]), &[0]).unwrap();
    assert_eq!(super::fused_filter_project_kernel_count(), 0);

    let invalid = filter_project(&input, &BooleanArray::from(vec![true, false]), &[9]);
    assert!(invalid.is_err());
    assert_eq!(super::fused_filter_project_kernel_count(), 0);

    let scalar = ZSetBatch::empty(schema);
    assert!(filter_project(&scalar, &BooleanArray::from(vec![true]), &[0]).is_err());
    assert_eq!(super::fused_filter_project_kernel_count(), 0);

    let supported = ZSetBatch::new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![1])),
    )
    .unwrap();
    filter_project(&supported, &BooleanArray::from(vec![true]), &[0]).unwrap();
    assert_eq!(super::fused_filter_project_kernel_count(), 1);
}

#[test]
fn filter_project_falls_back_for_unlisted_nested_types() {
    let nested = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        1,
        Arc::new(Int64Array::from(vec![1, 2])) as _,
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Int64, false),
        Field::new("nested", nested.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![10, 20])), nested],
    )
    .unwrap();
    let input = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![2, -3]))).unwrap();
    let mask = BooleanArray::from(vec![true, false]);
    let actual = filter_project(&input, &mask, &[0]).unwrap();
    let expected = project(&filter(&input, &mask).unwrap(), &[0]).unwrap();
    assert_eq!(actual.batch, expected.batch);
    assert_eq!(actual.diff.as_ref(), expected.diff.as_ref());
}

#[test]
fn invalid_projection_and_unsupported_discarded_type_keep_filter_first_error() {
    let (schema, input) = nested_input();
    let mask = BooleanArray::from(vec![true, false]);
    let expected = match project(&filter(&input, &mask).unwrap(), &[9]) {
        Ok(_) => panic!("invalid projection unexpectedly succeeded"),
        Err(error) => error,
    };
    let actual = match filter_project(&input, &mask, &[9]) {
        Ok(_) => panic!("invalid projection unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(actual.to_string(), expected.to_string());

    let invalid_predicate = BooleanArray::from(vec![true]);
    let expected = match filter(&ZSetBatch::empty(schema.clone()), &invalid_predicate) {
        Ok(_) => panic!("length mismatch unexpectedly succeeded"),
        Err(error) => error,
    };
    let actual = match filter_project(&ZSetBatch::empty(schema), &invalid_predicate, &[9]) {
        Ok(_) => panic!("length mismatch unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(actual.to_string(), expected.to_string());
}

#[test]
fn unsupported_type_fallback_preserves_legacy_success_or_error() {
    let (schema, input) = nested_input();
    let predicate_error = match filter_project(&input, &BooleanArray::from(vec![true, false]), &[0])
    {
        Ok(_) => "success".to_string(),
        Err(error) => error.to_string(),
    };
    let fallback_error = match filter(&input, &BooleanArray::from(vec![true, false])) {
        Ok(_) => "success".to_string(),
        Err(error) => error.to_string(),
    };
    assert_eq!(predicate_error, fallback_error);
    let invalid_projection_error =
        match filter_project(&input, &BooleanArray::from(vec![true, false]), &[9]) {
            Ok(_) => panic!("invalid projection unexpectedly succeeded"),
            Err(error) => error,
        };
    let dense_invalid_projection_error = match project(
        &filter(&input, &BooleanArray::from(vec![true, false])).unwrap(),
        &[9],
    ) {
        Ok(_) => panic!("invalid dense projection unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(
        invalid_projection_error.to_string(),
        dense_invalid_projection_error.to_string()
    );

    let invalid_predicate = BooleanArray::from(vec![true]);
    let legacy_length_error = match filter(&ZSetBatch::empty(schema), &invalid_predicate) {
        Ok(_) => panic!("invalid mask length unexpectedly succeeded"),
        Err(error) => error,
    };
    let actual_length_error =
        match filter_project(&ZSetBatch::empty(input.schema()), &invalid_predicate, &[9]) {
            Ok(_) => panic!("invalid mask length unexpectedly succeeded"),
            Err(error) => error,
        };
    assert_eq!(
        actual_length_error.to_string(),
        legacy_length_error.to_string()
    );
}

#[test]
fn dense_union_run_end_filter_falls_back_without_changing_arrow_result() {
    let rows = 32_769;
    let run = RunArray::<arrow::datatypes::Int16Type>::try_new(
        &Int16Array::from(vec![1]),
        &Int64Array::from(vec![7]),
    )
    .unwrap();
    let fields: UnionFields = vec![(
        0,
        Arc::new(Field::new("run", run.data_type().clone(), false)),
    )]
    .into_iter()
    .collect();
    let union = UnionArray::try_new(
        fields.clone(),
        ScalarBuffer::from(vec![0_i8; rows]),
        Some(ScalarBuffer::from(vec![0_i32; rows])),
        vec![Arc::new(run)],
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Int64, false),
        Field::new("union", DataType::Union(fields, UnionMode::Dense), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![0; rows])), Arc::new(union)],
    )
    .unwrap();
    let input = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1; rows]))).unwrap();
    let mut selected = vec![true; rows];
    selected[rows / 2] = false;
    let mask = BooleanArray::from(selected);

    assert!(!super::filter_kernel_supported(
        input.batch.column(1).data_type()
    ));
    let dense_error = match filter(&input, &mask) {
        Ok(_) => panic!("expected Arrow's dense Union/RunEnd overflow"),
        Err(error) => error.to_string(),
    };
    let candidate_error = match filter_project(&input, &mask, &[0]) {
        Ok(_) => panic!("projection fast path hid Arrow's dense filter error"),
        Err(error) => error.to_string(),
    };
    assert_eq!(candidate_error, dense_error);
}
