use std::sync::Arc;

use super::typed_keys;

use arrow::array::{Array, ArrayRef, DictionaryArray, Int8Array, StringArray};
use arrow::datatypes::{DataType, Field, Int8Type, Schema};
use arrow::record_batch::RecordBatch;

#[test]
fn typed_key_codec_roundtrips_every_batch_without_dictionary_capacity_loss() {
    let field = Arc::new(Field::new(
        "key",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    ));
    let schema = Arc::new(Schema::new(vec![field]));
    let batches = (0..3)
        .map(|part| {
            let start = part * 50;
            let values = (start..start + 50)
                .map(|i| Some(format!("key-{i}")))
                .collect::<Vec<_>>();
            let array = StringArray::from(values);
            let keys = Int8Array::from((0..50).map(|i| Some(i as i8)).collect::<Vec<_>>());
            let dict =
                DictionaryArray::<Int8Type>::try_new(keys, Arc::new(array) as ArrayRef).unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap()
        })
        .collect::<Vec<_>>();

    let encoded = super::typed_keys::encode(&schema, batches.iter()).unwrap();
    let (decoded, rows) = super::typed_keys::decode(&encoded, &schema).unwrap();
    assert_eq!(rows, 150);
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[2].num_rows(), 50);
    let dict = decoded[2]
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(dict.values().len(), 50);
}

#[test]
fn typed_key_reader_rejects_truncation_trailing_bytes_and_wrong_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![9])) as ArrayRef],
    )
    .unwrap();
    let encoded = super::typed_keys::encode(&schema, [&batch]).unwrap();

    assert!(super::typed_keys::decode(&encoded[..encoded.len() - 1], &schema).is_err());
    let mut trailing = encoded.clone();
    trailing.push(0x7f);
    assert!(super::typed_keys::decode(&trailing, &schema).is_err());
    let mut extra_stream = encoded.clone();
    extra_stream.extend_from_slice(&encoded);
    assert!(super::typed_keys::decode(&extra_stream, &schema).is_err());
    let wrong = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    assert!(super::typed_keys::decode(&encoded, &wrong).is_err());
}

#[test]
fn typed_key_reader_requires_exact_eos_for_empty_and_populated_streams() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let populated = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![9])) as ArrayRef],
    )
    .unwrap();
    let empty = RecordBatch::new_empty(schema.clone());
    for batch in [&populated, &empty] {
        let encoded = super::typed_keys::encode(&schema, [batch]).unwrap();
        assert!(super::typed_keys::decode(&encoded, &schema).is_ok());
        assert!(
            super::typed_keys::decode(&encoded[..encoded.len() - 8], &schema).is_err(),
            "missing complete EOS accepted"
        );
        for partial in 1..8 {
            assert!(
                super::typed_keys::decode(&encoded[..encoded.len() - partial], &schema).is_err(),
                "partial EOS length {partial} accepted"
            );
        }
        let mut suffix = encoded.clone();
        suffix.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
        assert!(
            super::typed_keys::decode(&suffix, &schema).is_err(),
            "EOS appended after complete stream accepted"
        );
    }
}

#[path = "typed_keys_capture_tests.rs"]
mod capture_tests;
