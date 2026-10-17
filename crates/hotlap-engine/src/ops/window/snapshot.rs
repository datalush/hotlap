//! Versioned export/import of [`TumbleCount`]'s open windows and progress.

use std::sync::Arc;

use arrow::array::new_empty_array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use hotlap_core::snapshot::{TypedKeyPayload, WindowBucket, WindowState};

use super::TumbleCount;
use crate::core::ipc::{decode_schema, encode_schema, typed_keys};
use crate::error::EngineError;
use crate::keys::converter_for;

impl TumbleCount {
    pub(crate) fn export_state(&self) -> Result<WindowState, EngineError> {
        let schema = self.schema.as_ref().map(encode_schema).transpose()?;
        let mut open = Vec::new();
        let mut key_rows = Vec::new();
        let mut native_batches = Vec::new();
        for (start, buckets) in &self.windows {
            for (key_bytes, (key, count)) in buckets {
                open.push(WindowBucket {
                    start: *start,
                    count: *count,
                });
                if let Some(cache) = &self.native_keys {
                    let native = cache.get(&(*start, key_bytes.clone())).ok_or_else(|| {
                        EngineError::Infrastructure(
                            "window native key representative missing".into(),
                        )
                    })?;
                    native_batches.push(native.batch());
                } else {
                    key_rows.push(key.clone());
                }
            }
        }
        let typed_keys = match &self.schema {
            None => TypedKeyPayload::Uninitialized,
            Some(input_schema) => {
                let converter = self.converter.as_ref().ok_or_else(|| {
                    EngineError::Infrastructure("window converter missing".into())
                })?;
                let key_schema = Arc::new(Schema::new(
                    self.key
                        .iter()
                        .map(|&i| input_schema.field(i).clone())
                        .collect::<Vec<_>>(),
                ));
                if self.native_keys.is_some() {
                    let empty_batch;
                    let batches = if native_batches.is_empty() {
                        empty_batch = RecordBatch::new_empty(key_schema.clone());
                        vec![&empty_batch]
                    } else {
                        native_batches
                    };
                    TypedKeyPayload::Ipc(typed_keys::encode(&key_schema, batches)?)
                } else {
                    let batch = if key_rows.is_empty() {
                        RecordBatch::try_new(
                            key_schema.clone(),
                            self.key
                                .iter()
                                .map(|&i| new_empty_array(input_schema.field(i).data_type()))
                                .collect(),
                        )?
                    } else {
                        let arrays = super::helpers::decode(
                            converter,
                            &key_rows.iter().collect::<Vec<_>>(),
                        )?;
                        RecordBatch::try_new(key_schema.clone(), arrays)?
                    };
                    TypedKeyPayload::Ipc(typed_keys::encode(&key_schema, [&batch])?)
                }
            }
        };
        Ok(WindowState {
            typed_keys,
            schema,
            watermark: self.watermark,
            dropped_late: self.dropped_late,
            dropped_closed: self.dropped_closed,
            open,
        })
    }

    pub(crate) fn import_state(&mut self, state: &WindowState) -> Result<(), EngineError> {
        let Some(bytes) = &state.schema else {
            if !state.open.is_empty() || state.typed_keys != TypedKeyPayload::Uninitialized {
                return Err(EngineError::Infrastructure(
                    "restored window buckets exist without their schema".into(),
                ));
            }
            self.windows.clear();
            self.native_keys = None;
            self.watermark = state.watermark;
            self.dropped_late = state.dropped_late;
            self.dropped_closed = state.dropped_closed;
            self.converter = None;
            self.schema = None;
            return Ok(());
        };
        let schema = decode_schema(bytes)?;
        if self.key.iter().any(|&i| i >= schema.fields().len()) {
            return Err(EngineError::Infrastructure(
                "restored window key column out of range".into(),
            ));
        }
        let key_schema = Arc::new(Schema::new(
            self.key
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect::<Vec<_>>(),
        ));
        let converter = converter_for(&schema, &self.key)?;
        let TypedKeyPayload::Ipc(key_bytes) = &state.typed_keys else {
            return Err(EngineError::Infrastructure(
                "initialized window missing typed keys".into(),
            ));
        };
        let (batches, rows) = typed_keys::decode(key_bytes, &key_schema)?;
        if rows != state.open.len() {
            return Err(EngineError::Infrastructure(
                "typed window key cardinality mismatch".into(),
            ));
        }
        let mut keys = Vec::with_capacity(rows);
        let native_enabled = crate::core::ipc::typed_keys::schema_uses_native_capture(&key_schema);
        let mut native_keys = native_enabled.then(std::collections::BTreeMap::new);
        for batch in batches {
            let rows = converter.convert_columns(batch.columns())?;
            for row in 0..rows.num_rows() {
                let key = rows.row(row).owned();
                if native_keys.is_some() {
                    let representative =
                        crate::core::ipc::typed_keys::TypedKeyRow::capture(&batch, row)?;
                    // Pair representatives with metadata below after canonical keys are known.
                    keys.push((key, Some(representative)));
                } else {
                    keys.push((key, None));
                }
            }
        }
        let mut windows = super::helpers::Windows::new();
        for (bucket, (key, native)) in state.open.iter().zip(keys) {
            let bytes = key.as_ref().to_vec();
            if windows
                .entry(bucket.start)
                .or_default()
                .insert(bytes.clone(), (key, bucket.count))
                .is_some()
            {
                return Err(EngineError::Infrastructure(
                    "duplicate restored window bucket".into(),
                ));
            }
            if let (Some(cache), Some(native)) = (&mut native_keys, native) {
                cache.insert((bucket.start, bytes), native);
            }
        }
        self.windows = windows;
        self.native_keys = native_keys;
        self.watermark = state.watermark;
        self.dropped_late = state.dropped_late;
        self.dropped_closed = state.dropped_closed;
        self.converter = Some(converter);
        self.schema = Some(schema);
        Ok(())
    }
}
