use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int64Array, Int64Builder, ListBuilder,
    StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Int8Type, Schema};
use arrow::record_batch::RecordBatch;

#[test]
fn typed_key_capture_compacts_dictionary_value_buffer_to_one_key() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]));
    let values = Arc::new(StringArray::from(
        (0..4096).map(|i| Some(format!("v{i}"))).collect::<Vec<_>>(),
    ));
    let keys = Int8Array::from(vec![Some(120_i8)]);
    let dictionary = DictionaryArray::<Int8Type>::try_new(keys, values.clone()).unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dictionary)]).unwrap();
    let row = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    assert_eq!(row.batch().schema().as_ref(), schema.as_ref());
    let dictionary = row
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(dictionary.values().len(), 1);
    assert!(dictionary.values().get_array_memory_size() < values.get_array_memory_size() / 100);
    let converter = crate::keys::converter_for(&schema, &[0]).unwrap();
    assert_eq!(
        converter.convert_columns(batch.columns()).unwrap().row(0),
        converter
            .convert_columns(row.batch().columns())
            .unwrap()
            .row(0),
    );
}

#[test]
fn typed_key_capture_preserves_float_bits_and_canonical_zero_column_row_count() {
    let float_schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)]));
    let bits = [0x8000_0000_0000_0000_u64, 0x7ff8_0000_0000_0042_u64];
    for bits in bits {
        let batch = RecordBatch::try_new(
            float_schema.clone(),
            vec![Arc::new(arrow::array::Float64Array::from(vec![
                f64::from_bits(bits),
            ]))],
        )
        .unwrap();
        let row = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
        let value = row
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(value.to_bits(), bits);
    }

    let empty_schema = Arc::new(Schema::empty());
    let one = RecordBatch::try_new_with_options(
        empty_schema.clone(),
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let zero = RecordBatch::new_empty(empty_schema.clone());
    let one_ipc = super::typed_keys::encode(&empty_schema, [&one]).unwrap();
    let (decoded_one, rows_one) = super::typed_keys::decode(&one_ipc, &empty_schema).unwrap();
    assert_eq!(rows_one, 1);
    assert_eq!(decoded_one[0].num_rows(), 1);
    let zero_ipc = super::typed_keys::encode(&empty_schema, [&zero]).unwrap();
    let (decoded_zero, rows_zero) = super::typed_keys::decode(&zero_ipc, &empty_schema).unwrap();
    assert_eq!(rows_zero, 0);
    assert_eq!(decoded_zero[0].num_rows(), 0);
}

#[test]
fn native_capture_classifier_is_selective_and_recursive() {
    let scalar = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    assert!(!super::typed_keys::schema_uses_native_capture(&scalar));
    let dictionary = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]));
    assert!(super::typed_keys::schema_uses_native_capture(&dictionary));
    let nested = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::Struct(
            vec![Field::new(
                "nested",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )]
            .into(),
        ),
        true,
    )]));
    assert!(!super::typed_keys::schema_uses_native_capture(&nested));
    let views = Arc::new(Schema::new(vec![
        Field::new("utf8_view", DataType::Utf8View, false),
        Field::new(
            "list_view",
            DataType::ListView(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]));
    assert!(super::typed_keys::schema_uses_native_capture(&views));

    let map_dictionary = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new(
                            "value",
                            DataType::Dictionary(
                                Box::new(DataType::Int8),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                        ),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        true,
    )]));
    assert!(super::typed_keys::schema_uses_native_capture(
        &map_dictionary
    ));
}

#[test]
fn nullable_view_capture_preserves_null_separately_from_empty_value() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8View, true),
        Field::new("b", DataType::BinaryView, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::StringViewArray::from(vec![None, Some("")])) as ArrayRef,
            Arc::new(arrow::array::BinaryViewArray::from(vec![
                None,
                Some(&[][..]),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    for row in 0..2 {
        let captured = super::typed_keys::TypedKeyRow::capture(&batch, row).unwrap();
        assert_eq!(captured.batch().schema().as_ref(), schema.as_ref());
        assert_eq!(captured.batch().column(0).is_null(0), row == 0);
        assert_eq!(captured.batch().column(1).is_null(0), row == 0);
    }
    let null_key = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let empty_key = super::typed_keys::TypedKeyRow::capture(&batch, 1).unwrap();
    let converter = crate::keys::converter_for(&schema, &[0, 1]).unwrap();
    assert_ne!(
        converter
            .convert_columns(null_key.batch().columns())
            .unwrap()
            .row(0),
        converter
            .convert_columns(empty_key.batch().columns())
            .unwrap()
            .row(0),
    );
}

#[test]
fn nested_dictionary_list_capture_releases_unreachable_values_and_keeps_canonical_key() {
    let list_field = Arc::new(Field::new("item", DataType::Int64, true));
    let dict_type = DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::List(list_field.clone())),
    );
    let fields = vec![Field::new("d", dict_type.clone(), true)];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::Struct(fields.clone().into()),
        true,
    )]));
    let mut lists = ListBuilder::new(Int64Builder::new());
    for i in 0..4096_i64 {
        lists.values().append_value(i);
        lists.append(true);
    }
    let values: ArrayRef = Arc::new(lists.finish());
    let dictionary =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(17_i8)]), values.clone())
            .unwrap();
    let structure =
        StructArray::try_new(fields.clone().into(), vec![Arc::new(dictionary)], None).unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(structure)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let captured_struct = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let captured_dict = captured_struct
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(captured_dict.values().len(), 1);

    fn retained_bytes(array: &dyn Array) -> usize {
        array.get_array_memory_size()
            + array
                .to_data()
                .child_data()
                .iter()
                .map(|child| retained_bytes(&arrow::array::make_array(child.clone())))
                .sum::<usize>()
    }
    let source_bytes = retained_bytes(batch.column(0).as_ref());
    let captured_bytes = retained_bytes(captured.batch().column(0).as_ref());
    assert!(source_bytes > 4096 * 8);
    assert!(
        captured_bytes < source_bytes / 100,
        "retained {captured_bytes} of {source_bytes}"
    );
}

#[test]
fn list_view_capture_retains_only_selected_child_span() {
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::ListView(field.clone()),
        true,
    )]));
    let source_values: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(0..100_000));
    let view = arrow::array::ListViewArray::try_new(
        field,
        arrow::buffer::ScalarBuffer::from(vec![99_999_i32]),
        arrow::buffer::ScalarBuffer::from(vec![1_i32]),
        source_values.clone(),
        None,
    )
    .unwrap();
    let source_bytes = source_values.get_array_memory_size();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(view)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let captured_view = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListViewArray>()
        .unwrap();
    assert_eq!(captured_view.value_offsets(), &[0]);
    assert_eq!(captured_view.value_sizes(), &[1]);
    assert_eq!(captured_view.values().len(), 1);
    assert!(captured_view.values().get_array_memory_size() < source_bytes / 100);
}

#[test]
fn empty_list_view_row_compacts_without_a_reachable_child_span() {
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let values: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(0..8));
    let view = arrow::array::ListViewArray::try_new(
        field.clone(),
        arrow::buffer::ScalarBuffer::from(vec![2_i32]),
        arrow::buffer::ScalarBuffer::from(vec![0_i32]),
        values,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        view.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(view)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let view = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListViewArray>()
        .unwrap();
    assert_eq!(view.value_offsets(), &[0]);
    assert_eq!(view.value_sizes(), &[0]);
    assert_eq!(view.values().len(), 0);
}

#[test]
fn recursive_capture_preserves_nullable_struct_and_list_values_canonically() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let list = Arc::new(Field::new("list", DataType::List(item), true));
    let fields = vec![Field::new("nested", list.data_type().clone(), true)];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        DataType::Struct(fields.clone().into()),
        true,
    )]));
    let mut builder = ListBuilder::new(Int64Builder::new());
    builder.values().append_value(4);
    builder.values().append_null();
    builder.append(true);
    builder.append(false);
    let lists = Arc::new(builder.finish());
    let structure = StructArray::try_new(
        fields.into(),
        vec![lists],
        Some(arrow::buffer::NullBuffer::from(vec![true, false])),
    )
    .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(structure)]).unwrap();
    let converter = crate::keys::converter_for(&schema, &[0]).unwrap();
    for row in 0..2 {
        let captured = super::typed_keys::TypedKeyRow::capture(&batch, row).unwrap();
        assert_eq!(
            converter
                .convert_columns(captured.batch().columns())
                .unwrap()
                .row(0),
            converter.convert_columns(batch.columns()).unwrap().row(row),
        );
    }
}

#[test]
fn large_list_fixed_list_and_large_list_view_capture_compact_children() {
    let child_field = Arc::new(Field::new("item", DataType::Int64, true));
    let large_values: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(0..100_000));
    let large_list = arrow::array::LargeListArray::try_new(
        child_field.clone(),
        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![
            99_999_i64, 100_000,
        ])),
        large_values.clone(),
        None,
    )
    .unwrap();
    let fixed_values: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(0..100_000));
    let fixed = arrow::array::FixedSizeListArray::try_new(
        child_field.clone(),
        1,
        fixed_values.clone(),
        None,
    )
    .unwrap();
    let view_values: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(0..100_000));
    let large_view = arrow::array::LargeListViewArray::try_new(
        child_field.clone(),
        arrow::buffer::ScalarBuffer::from(vec![99_999_i64]),
        arrow::buffer::ScalarBuffer::from(vec![1_i64]),
        view_values.clone(),
        None,
    )
    .unwrap();
    for (array, data_type, source_bytes) in [
        (
            Arc::new(large_list) as ArrayRef,
            DataType::LargeList(child_field.clone()),
            large_values.get_array_memory_size(),
        ),
        (
            Arc::new(fixed) as ArrayRef,
            DataType::FixedSizeList(child_field.clone(), 1),
            fixed_values.get_array_memory_size(),
        ),
        (
            Arc::new(large_view) as ArrayRef,
            DataType::LargeListView(child_field.clone()),
            view_values.get_array_memory_size(),
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new("k", data_type, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
        assert_eq!(captured.batch().schema().as_ref(), schema.as_ref());
        assert!(captured.batch().column(0).get_array_memory_size() < source_bytes / 100);
        let original_converter = crate::keys::converter_for(&schema, &[0]);
        let captured_converter = crate::keys::converter_for(&schema, &[0]);
        match (original_converter, captured_converter) {
            (Ok(original), Ok(captured_converter)) => assert_eq!(
                original.convert_columns(batch.columns()).unwrap().row(0),
                captured_converter
                    .convert_columns(captured.batch().columns())
                    .unwrap()
                    .row(0),
            ),
            (Err(_), Err(_)) => {}
            _ => panic!("capture changed RowConverter admission behavior"),
        }
        let bytes = super::typed_keys::encode(&schema, [captured.batch()]).unwrap();
        let (decoded, rows) = super::typed_keys::decode(&bytes, &schema).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(
            decoded[0].column(0).data_type(),
            captured.batch().column(0).data_type()
        );
    }
}

#[test]
fn null_fixed_size_list_preserves_required_typed_null_child_slots() {
    let dictionary_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let child = Arc::new(Field::new("item", dictionary_type.clone(), true));
    let values = Arc::new(arrow::array::new_null_array(&dictionary_type, 4));
    let source = arrow::array::FixedSizeListArray::try_new(
        child.clone(),
        2,
        values,
        Some(arrow::buffer::NullBuffer::from(vec![false, true])),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        source.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(source)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let list = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .unwrap();
    assert_eq!(list.data_type(), batch.column(0).data_type());
    assert_eq!(list.len(), 1);
    assert_eq!(list.values().len(), 2);
    assert_eq!(list.values().null_count(), 2);
    assert_eq!(list.null_count(), 1);
    let encoded = super::typed_keys::encode(&schema, [captured.batch()]).unwrap();
    let (decoded, rows) = super::typed_keys::decode(&encoded, &schema).unwrap();
    assert_eq!(rows, 1);
    let decoded_list = decoded[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .unwrap();
    assert_eq!(decoded_list.values().len(), 2);
    assert_eq!(decoded_list.values().null_count(), 2);
    match (
        crate::keys::converter_for(&schema, &[0]),
        crate::keys::converter_for(&schema, &[0]),
    ) {
        (Ok(source_converter), Ok(captured_converter)) => assert_eq!(
            source_converter
                .convert_columns(batch.columns())
                .unwrap()
                .row(0),
            captured_converter
                .convert_columns(captured.batch().columns())
                .unwrap()
                .row(0),
        ),
        (Err(_), Err(_)) => {} // RowConverter rejects FixedSizeList in the source schema.
        _ => panic!("capture changed RowConverter admission"),
    }

    let empty_child = Arc::new(Field::new("item", DataType::Int64, true));
    let empty_source = arrow::array::FixedSizeListArray::try_new(
        empty_child,
        0,
        Arc::new(Int64Array::new_null(0)),
        Some(arrow::buffer::NullBuffer::from(vec![false, true])),
    )
    .unwrap();
    let empty_schema = Arc::new(Schema::new(vec![Field::new(
        "empty",
        empty_source.data_type().clone(),
        true,
    )]));
    let empty_batch = RecordBatch::try_new(empty_schema, vec![Arc::new(empty_source)]).unwrap();
    let empty_capture = super::typed_keys::TypedKeyRow::capture(&empty_batch, 0).unwrap();
    let empty_list = empty_capture
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeListArray>()
        .unwrap();
    assert_eq!(empty_list.values().len(), 0);
    assert_eq!(empty_list.null_count(), 1);
}

#[test]
fn union_capture_preserves_noncontiguous_ids_and_run_end_stays_encoded() {
    use arrow::datatypes::{UnionFields, UnionMode};

    let union_fields: UnionFields = vec![
        (2, Arc::new(Field::new("unused", DataType::Utf8, true))),
        (7, Arc::new(Field::new("chosen", DataType::Int64, true))),
    ]
    .into_iter()
    .collect();
    let union = arrow::array::UnionArray::try_new(
        union_fields.clone(),
        arrow::buffer::ScalarBuffer::from(vec![7_i8]),
        Some(arrow::buffer::ScalarBuffer::from(vec![99_999_i32])),
        vec![
            Arc::new(StringArray::from(
                (0..100_000)
                    .map(|i| Some(format!("unused-{i}")))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::Int64Array::from_iter_values(0..100_000)),
        ],
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Dense),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(union)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let captured_union = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UnionArray>()
        .unwrap();
    assert_eq!(captured_union.type_id(0), 7);
    assert_eq!(captured_union.offsets().unwrap()[0], 0);
    assert_eq!(captured_union.child(7).len(), 1);
    assert_eq!(captured_union.child(2).len(), 0);
    assert_same_row_converter_admission(&schema, &batch, captured.batch());

    let sparse_fields: UnionFields = vec![
        (2, Arc::new(Field::new("unused", DataType::Utf8, true))),
        (7, Arc::new(Field::new("chosen", DataType::Int64, true))),
    ]
    .into_iter()
    .collect();
    let sparse = arrow::array::UnionArray::try_new(
        sparse_fields.clone(),
        arrow::buffer::ScalarBuffer::from(vec![7_i8]),
        None,
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(arrow::array::Int64Array::from(vec![Some(44)])),
        ],
    )
    .unwrap();
    let sparse_schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(sparse_fields, UnionMode::Sparse),
        false,
    )]));
    let sparse_batch = RecordBatch::try_new(sparse_schema, vec![Arc::new(sparse)]).unwrap();
    let sparse_captured = super::typed_keys::TypedKeyRow::capture(&sparse_batch, 0).unwrap();
    let sparse = sparse_captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UnionArray>()
        .unwrap();
    assert!(!sparse.is_dense());
    assert_eq!(sparse.type_id(0), 7);
    assert_eq!(sparse.child(2).len(), 1);
    assert_eq!(sparse.child(2).null_count(), 1);
    let sparse_schema = sparse_captured.batch().schema();
    let sparse_ipc = super::typed_keys::encode(&sparse_schema, [sparse_captured.batch()]).unwrap();
    let (sparse_decoded, sparse_rows) =
        super::typed_keys::decode(&sparse_ipc, &sparse_schema).unwrap();
    assert_eq!(sparse_rows, 1);
    assert_eq!(sparse_decoded[0].schema(), sparse_captured.batch().schema());

    let run = arrow::array::RunArray::<arrow::datatypes::Int32Type>::try_new(
        &arrow::array::Int32Array::from(vec![100_000_i32]),
        &arrow::array::Int64Array::from(vec![42_i64]),
    )
    .unwrap();
    let run_schema = Arc::new(Schema::new(vec![Field::new(
        "r",
        run.data_type().clone(),
        false,
    )]));
    let run_batch = RecordBatch::try_new(run_schema.clone(), vec![Arc::new(run)]).unwrap();
    let captured_run = super::typed_keys::TypedKeyRow::capture(&run_batch, 50_000).unwrap();
    let run = captured_run
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::RunArray<arrow::datatypes::Int32Type>>()
        .unwrap();
    assert_eq!(run.len(), 1);
    assert_eq!(run.run_ends().values(), &[1]);
    assert_eq!(run.values().len(), 1);
    assert_same_row_converter_admission(&run_schema, &run_batch, captured_run.batch());
    let run_ipc = super::typed_keys::encode(&run_schema, [captured_run.batch()]).unwrap();
    let (run_decoded, run_rows) = super::typed_keys::decode(&run_ipc, &run_schema).unwrap();
    assert_eq!(run_rows, 1);
    assert_eq!(run_decoded[0].schema().as_ref(), run_schema.as_ref());
}

fn assert_same_row_converter_admission(
    schema: &Arc<Schema>,
    source: &RecordBatch,
    captured: &RecordBatch,
) {
    let source_converter = crate::keys::converter_for(schema, &[0]);
    let captured_converter = crate::keys::converter_for(schema, &[0]);
    match (source_converter, captured_converter) {
        (Ok(source_converter), Ok(captured_converter)) => assert_eq!(
            source_converter
                .convert_columns(source.columns())
                .unwrap()
                .row(0),
            captured_converter
                .convert_columns(captured.columns())
                .unwrap()
                .row(0),
        ),
        (Err(_), Err(_)) => {} // Source RowConverter rejects this Arrow type before capture.
        _ => panic!("capture changed source RowConverter admission"),
    }
}

#[test]
fn list_view_run_end_capture_does_not_expand_the_selected_span() {
    let run = arrow::array::RunArray::<arrow::datatypes::Int16Type>::try_new(
        &arrow::array::Int16Array::from(vec![32_767_i16]),
        &arrow::array::Int64Array::from(vec![42_i64]),
    )
    .unwrap();
    let run_ref: ArrayRef = Arc::new(run);
    let field = Arc::new(Field::new("run", run_ref.data_type().clone(), true));
    let view = arrow::array::ListViewArray::try_new(
        field.clone(),
        arrow::buffer::ScalarBuffer::from(vec![32_765_i32]),
        arrow::buffer::ScalarBuffer::from(vec![2_i32]),
        run_ref,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        view.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(view)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let view = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListViewArray>()
        .unwrap();
    let run = view
        .values()
        .as_any()
        .downcast_ref::<arrow::array::RunArray<arrow::datatypes::Int16Type>>()
        .unwrap();
    assert_eq!(view.value_offsets(), &[0]);
    assert_eq!(view.value_sizes(), &[2]);
    assert_eq!(run.len(), 2);
    assert_eq!(run.run_ends().values(), &[2]);
    assert_eq!(run.values().len(), 1);
}

#[test]
fn overlapping_nested_list_views_share_one_compacted_run_end_span() {
    let run = arrow::array::RunArray::<arrow::datatypes::Int16Type>::try_new(
        &arrow::array::Int16Array::from(vec![32_767_i16]),
        &arrow::array::Int64Array::from(vec![9_i64]),
    )
    .unwrap();
    let run: ArrayRef = Arc::new(run);
    let run_field = Arc::new(Field::new("run", run.data_type().clone(), true));
    let inner = arrow::array::ListViewArray::try_new(
        run_field.clone(),
        arrow::buffer::ScalarBuffer::from(vec![0_i32, 32_766_i32]),
        arrow::buffer::ScalarBuffer::from(vec![32_767_i32, 1_i32]),
        run,
        None,
    )
    .unwrap();
    let inner_ref: ArrayRef = Arc::new(inner);
    let inner_field = Arc::new(Field::new("views", inner_ref.data_type().clone(), true));
    let outer = arrow::array::ListViewArray::try_new(
        inner_field.clone(),
        arrow::buffer::ScalarBuffer::from(vec![0_i32]),
        arrow::buffer::ScalarBuffer::from(vec![2_i32]),
        inner_ref,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        outer.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(outer)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let outer = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListViewArray>()
        .unwrap();
    let inner = outer
        .values()
        .as_any()
        .downcast_ref::<arrow::array::ListViewArray>()
        .unwrap();
    assert_eq!(inner.value_offsets(), &[0, 32_766]);
    assert_eq!(inner.values().len(), 32_767);
    let run = inner
        .values()
        .as_any()
        .downcast_ref::<arrow::array::RunArray<arrow::datatypes::Int16Type>>()
        .unwrap();
    assert_eq!(run.run_ends().values(), &[32_767]);
    assert_eq!(run.values().len(), 1);
}

#[test]
fn outer_list_merges_overlapping_large_list_view_run_end_spans() {
    let run = arrow::array::RunArray::<arrow::datatypes::Int16Type>::try_new(
        &arrow::array::Int16Array::from(vec![32_767_i16]),
        &Int64Array::from(vec![9_i64]),
    )
    .unwrap();
    let run: ArrayRef = Arc::new(run);
    let run_field = Arc::new(Field::new("run", run.data_type().clone(), true));
    let inner = arrow::array::LargeListViewArray::try_new(
        run_field.clone(),
        arrow::buffer::ScalarBuffer::from(vec![0_i64, 32_766_i64]),
        arrow::buffer::ScalarBuffer::from(vec![32_767_i64, 1_i64]),
        run,
        None,
    )
    .unwrap();
    let inner: ArrayRef = Arc::new(inner);
    let inner_field = Arc::new(Field::new("views", inner.data_type().clone(), true));
    let outer = arrow::array::ListArray::try_new(
        inner_field.clone(),
        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0_i32, 2_i32])),
        inner,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        outer.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(outer)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let outer = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let views = outer
        .values()
        .as_any()
        .downcast_ref::<arrow::array::LargeListViewArray>()
        .unwrap();
    assert_eq!(views.value_offsets(), &[0_i64, 32_766_i64]);
    assert_eq!(views.values().len(), 32_767);
    let run = views
        .values()
        .as_any()
        .downcast_ref::<arrow::array::RunArray<arrow::datatypes::Int16Type>>()
        .unwrap();
    assert_eq!(run.run_ends().values(), &[32_767]);
    assert_eq!(run.values().len(), 1);
}

#[test]
fn map_capture_compacts_reachable_entries_and_preserves_row_converter_key() {
    let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
    let value_field = Arc::new(Field::new("value", DataType::Int64, true));
    let entry_fields: arrow::datatypes::Fields =
        vec![key_field.clone(), value_field.clone()].into();
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(entry_fields.clone()),
        false,
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "m",
        DataType::Map(entries_field.clone(), false),
        true,
    )]));
    let entries = StructArray::try_new(
        entry_fields,
        vec![
            Arc::new(StringArray::from(
                (0..50_000)
                    .map(|i| Some(format!("k{i}")))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::Int64Array::from_iter_values(0..50_000)),
        ],
        None,
    )
    .unwrap();
    let map = arrow::array::MapArray::try_new(
        entries_field,
        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![
            49_999_i32, 50_000,
        ])),
        entries,
        None,
        false,
    )
    .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(map)]).unwrap();
    let captured = super::typed_keys::TypedKeyRow::capture(&batch, 0).unwrap();
    let captured_map = captured
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    assert_eq!(captured_map.value_offsets(), &[0, 1]);
    assert_eq!(captured_map.entries().len(), 1);
    let converter = crate::keys::converter_for(&schema, &[0]);
    if let Ok(converter) = converter {
        assert_eq!(
            converter.convert_columns(batch.columns()).unwrap().row(0),
            converter
                .convert_columns(captured.batch().columns())
                .unwrap()
                .row(0),
        );
    }
    let encoded = super::typed_keys::encode(&schema, [captured.batch()]).unwrap();
    let (decoded, rows) = super::typed_keys::decode(&encoded, &schema).unwrap();
    assert_eq!(rows, 1);
    assert_eq!(
        decoded[0].column(0).data_type(),
        captured.batch().column(0).data_type()
    );
}

#[test]
fn dropped_complex_key_sources_leave_only_bounded_captured_buffer_capacity() {
    use std::collections::HashSet;

    fn capture_first(source: RecordBatch) -> super::typed_keys::TypedKeyRow {
        let key_schema = Arc::new(Schema::new(vec![source.schema().field(0).clone()]));
        let key_only = RecordBatch::try_new(key_schema, vec![source.column(0).clone()]).unwrap();
        let row = super::typed_keys::TypedKeyRow::capture(&key_only, 0).unwrap();
        drop(key_only);
        drop(source);
        row
    }

    fn retained_unique_buffer_capacity(array: &dyn Array) -> usize {
        fn walk(data: &arrow::array::ArrayData, seen: &mut HashSet<(usize, usize)>) -> usize {
            let mut total = 0;
            for buffer in data.buffers() {
                let identity = (buffer.data_ptr().as_ptr() as usize, buffer.capacity());
                if buffer.capacity() > 0 && seen.insert(identity) {
                    total += buffer.capacity();
                }
            }
            if let Some(nulls) = data.nulls() {
                let buffer = nulls.buffer();
                let identity = (buffer.data_ptr().as_ptr() as usize, buffer.capacity());
                if buffer.capacity() > 0 && seen.insert(identity) {
                    total += buffer.capacity();
                }
            }
            for child in data.child_data() {
                total += walk(child, seen);
            }
            total
        }
        walk(&array.to_data(), &mut HashSet::new())
    }

    let dictionary_type = DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
    let dictionary_field = Field::new("key", dictionary_type.clone(), true);
    let dictionary_values = Arc::new(StringArray::from(
        (0..8192)
            .map(|i| Some(format!("key-{i:04}-{}", "x".repeat(48))))
            .collect::<Vec<_>>(),
    ));
    let dictionary = DictionaryArray::<arrow::datatypes::Int16Type>::try_new(
        arrow::array::Int16Array::from(vec![Some(8191_i16)]),
        dictionary_values,
    )
    .unwrap();
    let source = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            dictionary_field,
            Field::new("unrelated_payload", DataType::Int64, false),
        ])),
        vec![
            Arc::new(dictionary) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(0..1_000_000).slice(0, 1)) as ArrayRef,
        ],
    )
    .unwrap();
    let captured_dictionary = capture_first(source);
    assert_eq!(captured_dictionary.batch().num_columns(), 1);
    let dictionary_capacity =
        retained_unique_buffer_capacity(captured_dictionary.batch().column(0).as_ref());
    eprintln!("retained_capacity_case=dictionary_utf8 bytes={dictionary_capacity}");
    assert!(
        dictionary_capacity <= 4096,
        "retained {dictionary_capacity} bytes"
    );

    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let dict_list_type = DataType::Dictionary(
        Box::new(DataType::Int16),
        Box::new(DataType::List(item.clone())),
    );
    let mut list_builder = ListBuilder::new(Int64Builder::new());
    for i in 0..16_384_i64 {
        list_builder.values().append_value(i);
        list_builder.append(true);
    }
    let dict_list_values: ArrayRef = Arc::new(list_builder.finish());
    let dict_list = DictionaryArray::<arrow::datatypes::Int16Type>::try_new(
        arrow::array::Int16Array::from(vec![Some(16_383_i16)]),
        dict_list_values,
    )
    .unwrap();
    let nested_schema = Arc::new(Schema::new(vec![Field::new(
        "nested_dictionary_list",
        dict_list_type,
        true,
    )]));
    let nested_batch = RecordBatch::try_new(nested_schema, vec![Arc::new(dict_list)]).unwrap();
    let captured_nested = capture_first(nested_batch);
    let nested_capacity =
        retained_unique_buffer_capacity(captured_nested.batch().column(0).as_ref());
    eprintln!("retained_capacity_case=nested_dictionary_list bytes={nested_capacity}");
    assert!(nested_capacity <= 4096, "retained {nested_capacity} bytes");

    let view_field = Arc::new(Field::new("item", DataType::Int64, true));
    let view_values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1_000_000));
    let view = arrow::array::ListViewArray::try_new(
        view_field.clone(),
        arrow::buffer::ScalarBuffer::from(vec![999_999_i32]),
        arrow::buffer::ScalarBuffer::from(vec![1_i32]),
        view_values,
        None,
    )
    .unwrap();
    let view_schema = Arc::new(Schema::new(vec![Field::new(
        "list_view",
        view.data_type().clone(),
        true,
    )]));
    let view_batch = RecordBatch::try_new(view_schema, vec![Arc::new(view)]).unwrap();
    let captured_view = capture_first(view_batch);
    let view_capacity = retained_unique_buffer_capacity(captured_view.batch().column(0).as_ref());
    eprintln!("retained_capacity_case=list_view_int64 bytes={view_capacity}");
    assert!(view_capacity <= 4096, "retained {view_capacity} bytes");
}
