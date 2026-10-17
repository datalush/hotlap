use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, Int8Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Int8Type, Schema};
use arrow::record_batch::RecordBatch;

use crate::batch::ZSetBatch;

use super::TumbleCount;

fn dictionary_delta(start: i64, count: usize) -> ZSetBatch {
    let key_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", key_type.clone(), true),
        Field::new("time", DataType::Int64, false),
    ]));
    let values = Arc::new(StringArray::from(
        (start..start + count as i64)
            .map(|v| Some(format!("k{v}")))
            .collect::<Vec<_>>(),
    ));
    let keys = Int8Array::from((0..count).map(|i| Some(i as i8)).collect::<Vec<_>>());
    let dictionary = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(dictionary) as ArrayRef,
            Arc::new(Int64Array::from(vec![1; count])),
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(vec![1; count])) as ArrayRef,
    )
    .unwrap()
}

fn nullable_string_delta() -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("time", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![None, Some("a"), Some("a")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(vec![-3, 1, -1])) as ArrayRef,
    )
    .unwrap()
}

#[test]
fn native_dictionary_window_checkpoint_preserves_many_keys_and_continues() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    for part in 0..3 {
        window.apply(&dictionary_delta(part * 50, 50), 0).unwrap();
    }
    assert_eq!(window.windows.values().map(|b| b.len()).sum::<usize>(), 150);
    let state = window.export_state().unwrap();
    let mut restored = TumbleCount::new(&[0], 1, 10);
    restored.import_state(&state).unwrap();
    assert_eq!(
        restored.windows.values().map(|b| b.len()).sum::<usize>(),
        150
    );
    restored.apply(&dictionary_delta(0, 1), 0).unwrap();
    assert_eq!(
        restored.windows.values().map(|b| b.len()).sum::<usize>(),
        150
    );
    assert_eq!(
        restored
            .windows
            .values()
            .next()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .1,
        2
    );
}

#[test]
fn initialized_empty_native_key_state_exports_typed_empty_batch() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    window.apply(&dictionary_delta(0, 0), 0).unwrap();
    let state = window.export_state().unwrap();
    let hotlap_core::snapshot::TypedKeyPayload::Ipc(bytes) = state.typed_keys else {
        panic!("initialized state must carry typed IPC");
    };
    let expected = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]));
    let (batches, rows) = crate::core::ipc::typed_keys::decode(&bytes, &expected).unwrap();
    assert_eq!(rows, 0);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
}

#[test]
fn failed_window_import_keeps_populated_target_unchanged() {
    let mut source = TumbleCount::new(&[0], 1, 10);
    source.apply(&dictionary_delta(0, 1), 0).unwrap();
    let mut malformed = source.export_state().unwrap();
    if let hotlap_core::snapshot::TypedKeyPayload::Ipc(bytes) = &mut malformed.typed_keys {
        bytes.pop();
    }
    let mut target = TumbleCount::new(&[0], 1, 10);
    target.apply(&dictionary_delta(50, 1), 0).unwrap();
    let before = target.export_state().unwrap();
    assert!(target.import_state(&malformed).is_err());
    assert_eq!(target.export_state().unwrap(), before);
}

#[test]
fn native_import_rejects_duplicate_canonical_key_with_paired_metadata_atomically() {
    let mut source = TumbleCount::new(&[0], 1, 10);
    source.apply(&dictionary_delta(0, 1), 0).unwrap();
    let mut duplicate = source.export_state().unwrap();
    duplicate.open.push(duplicate.open[0].clone());
    let key_schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]));
    let values = Arc::new(StringArray::from(vec![Some("k0")]));
    let dictionary =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0_i8), Some(0_i8)]), values)
            .unwrap();
    let batch = RecordBatch::try_new(key_schema.clone(), vec![Arc::new(dictionary)]).unwrap();
    duplicate.typed_keys = hotlap_core::snapshot::TypedKeyPayload::Ipc(
        crate::core::ipc::typed_keys::encode(&key_schema, [&batch]).unwrap(),
    );

    let mut target = TumbleCount::new(&[0], 1, 10);
    target.apply(&dictionary_delta(50, 1), 0).unwrap();
    let before = target.export_state().unwrap();
    assert!(target.import_state(&duplicate).is_err());
    assert_eq!(target.export_state().unwrap(), before);
}

#[test]
fn native_import_rejects_wrong_key_schema_and_row_pairing_atomically() {
    let mut source = TumbleCount::new(&[0], 1, 10);
    source.apply(&dictionary_delta(0, 1), 0).unwrap();
    let valid = source.export_state().unwrap();
    let wrong_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some("k0")])) as ArrayRef],
    )
    .unwrap();
    let mut bad_schema = valid.clone();
    bad_schema.typed_keys = hotlap_core::snapshot::TypedKeyPayload::Ipc(
        crate::core::ipc::typed_keys::encode(&wrong_schema, [&wrong_batch]).unwrap(),
    );
    let mut bad_pairing = valid;
    bad_pairing.open.clear();

    let mut target = TumbleCount::new(&[0], 1, 10);
    target.apply(&dictionary_delta(50, 1), 0).unwrap();
    let before = target.export_state().unwrap();
    assert!(target.import_state(&bad_schema).is_err());
    assert_eq!(target.export_state().unwrap(), before);
    assert!(target.import_state(&bad_pairing).is_err());
    assert_eq!(target.export_state().unwrap(), before);
}

#[test]
fn scalar_nullable_utf8_roundtrip_keeps_zero_and_negative_buckets_without_cache() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    window.apply(&nullable_string_delta(), 0).unwrap();
    assert!(window.native_keys.is_none());
    let state = window.export_state().unwrap();
    let mut restored = TumbleCount::new(&[0], 1, 10);
    restored.import_state(&state).unwrap();
    assert_eq!(restored.export_state().unwrap(), state);
    let counts = restored.windows.values().next().unwrap();
    assert_eq!(counts.len(), 2);
    assert!(counts.values().any(|(_, count)| *count == 0));
    assert!(counts.values().any(|(_, count)| *count == -3));
}

#[test]
fn old_window_frame_without_typed_keys_is_rejected_by_normal_frame_decode() {
    use serde::Serialize;

    use hotlap_core::snapshot::WindowState;

    // This is the historical WindowBucket field order from git HEAD:
    // (start, key, count). Keep it test-only; production has no old reader.
    #[derive(Serialize)]
    struct LegacyWindowBucket {
        start: i64,
        key: Vec<u8>,
        count: i64,
    }

    #[derive(Serialize)]
    struct LegacyRawWindowState {
        schema: Option<Vec<u8>>,
        watermark: i64,
        dropped_late: u64,
        dropped_closed: u64,
        open: Vec<LegacyWindowBucket>,
    }

    // Obtain valid old-schema and canonical key bytes from an actual live
    // reducer, so rejection is due to the historical layout, not bad fixture
    // contents.
    let mut source = super::TumbleCount::new(&[0], 1, 10);
    source.apply(&nullable_string_delta(), 0).unwrap();
    let current = source.export_state().unwrap();
    let (&start, buckets) = source.windows.iter().next().unwrap();
    let (key, (_, count)) = buckets.iter().next().unwrap();
    assert!(!key.is_empty());
    let old = LegacyRawWindowState {
        schema: current.schema,
        watermark: current.watermark,
        dropped_late: current.dropped_late,
        dropped_closed: current.dropped_closed,
        open: vec![LegacyWindowBucket {
            start,
            key: key.clone(),
            count: *count,
        }],
    };
    let exact_old_frame = crate::core::ipc::encode_framed(&old).unwrap();
    assert!(matches!(
        crate::core::ipc::decode_framed::<WindowState>(&exact_old_frame),
        Err(crate::error::EngineError::Infrastructure(_))
    ));
}
