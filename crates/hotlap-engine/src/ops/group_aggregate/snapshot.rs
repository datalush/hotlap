//! Versioned export/import of [`GroupAggregate`]'s retained groups.

use std::sync::Arc;

use arrow::array::{ArrayRef, UInt8Array, new_empty_array};
use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::SortField;
use hotlap_core::snapshot::{GroupEntry, GroupState, TypedKeyPayload};

use super::GroupAggregate;
use super::accumulate::empty_entry;
use crate::core::ipc::{decode_schema, encode_schema, typed_keys};
use crate::error::EngineError;
use crate::keys::converter_for;

impl GroupAggregate {
    /// Exports authoritative entries and key rows in deterministic key order.
    pub(crate) fn export_state(&self) -> Result<GroupState, EngineError> {
        let schema_bytes = self.schema.as_ref().map(encode_schema).transpose()?;
        let mut ordered: Vec<_> = self.groups.iter().collect();
        ordered.sort_by(|a, b| a.0.cmp(b.0));
        let groups: Vec<GroupEntry> = ordered.iter().map(|(_, entry)| (*entry).clone()).collect();
        let typed_keys = match (&self.schema, &self.native_keys) {
            (None, _) => TypedKeyPayload::Uninitialized,
            (Some(input_schema), native) => {
                let key_schema = Arc::new(Schema::new(
                    self.key
                        .iter()
                        .map(|&i| input_schema.field(i).clone())
                        .collect::<Vec<_>>(),
                ));
                if self.key.is_empty() {
                    let batch = RecordBatch::try_new_with_options(
                        key_schema.clone(),
                        vec![],
                        &RecordBatchOptions::new().with_row_count(Some(groups.len())),
                    )?;
                    TypedKeyPayload::Ipc(typed_keys::encode(&key_schema, [&batch])?)
                } else if native.is_some() {
                    let cache = native.as_ref().unwrap();
                    let batches = ordered
                        .iter()
                        .map(|(key, _)| {
                            cache.get(*key).map(|row| row.batch()).ok_or_else(|| {
                                snapshot_shape_error("native key representative missing")
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if batches.is_empty() {
                        let empty = RecordBatch::new_empty(key_schema.clone());
                        TypedKeyPayload::Ipc(typed_keys::encode(&key_schema, [&empty])?)
                    } else {
                        TypedKeyPayload::Ipc(typed_keys::encode(&key_schema, batches)?)
                    }
                } else {
                    let converter = self
                        .converter
                        .as_ref()
                        .ok_or_else(|| snapshot_shape_error("group converter missing"))?;
                    let parser = converter.parser();
                    let rows = ordered
                        .iter()
                        .map(|(key, _)| parser.parse(key))
                        .collect::<Vec<_>>();
                    let arrays = converter.convert_rows(rows)?;
                    let batch = if groups.is_empty() {
                        RecordBatch::try_new(
                            key_schema.clone(),
                            self.key
                                .iter()
                                .map(|&i| new_empty_array(input_schema.field(i).data_type()))
                                .collect(),
                        )?
                    } else {
                        RecordBatch::try_new(key_schema.clone(), arrays)?
                    };
                    TypedKeyPayload::Ipc(typed_keys::encode(&key_schema, [&batch])?)
                }
            }
        };
        Ok(GroupState {
            typed_keys,
            schema: schema_bytes,
            groups,
        })
    }

    /// Restores schema, all key rows and authoritative histories transactionally.
    pub(crate) fn import_state(&mut self, state: &GroupState) -> Result<(), EngineError> {
        let Some(schema_bytes) = &state.schema else {
            if !state.groups.is_empty() || state.typed_keys != TypedKeyPayload::Uninitialized {
                return Err(snapshot_shape_error("groups exist without a schema"));
            }
            self.groups.clear();
            self.native_keys = None;
            self.schema = None;
            self.converter = None;
            self.maintained_cut = None;
            #[cfg(test)]
            {
                self.work = 0;
            }
            return Ok(());
        };
        let schema = decode_schema(schema_bytes)?;
        if self
            .key
            .iter()
            .any(|&column| column >= schema.fields().len())
        {
            return Err(snapshot_shape_error("group key column out of bounds"));
        }
        let key_schema = Arc::new(Schema::new(
            self.key
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect::<Vec<_>>(),
        ));
        let converter = if self.key.is_empty() {
            arrow::row::RowConverter::new(vec![SortField::new(arrow::datatypes::DataType::UInt8)])?
        } else {
            converter_for(&schema, &self.key)?
        };
        let TypedKeyPayload::Ipc(key_bytes) = &state.typed_keys else {
            return Err(snapshot_shape_error(
                "initialized group state missing typed keys",
            ));
        };
        let (batches, row_count) = typed_keys::decode(key_bytes, &key_schema)?;
        if row_count != state.groups.len() {
            return Err(snapshot_shape_error(
                "typed key and entry cardinality mismatch",
            ));
        }
        let expected = empty_entry(&self.aggs, &schema)?;
        let mut groups = std::collections::HashMap::with_capacity(state.groups.len());
        let native_enabled = typed_keys::schema_uses_native_capture(&key_schema);
        let mut native_keys = native_enabled.then(std::collections::HashMap::new);
        let mut index = 0usize;
        for batch in batches {
            let converted =
                if self.key.is_empty() {
                    Some(converter.convert_columns(&[Arc::new(UInt8Array::from(
                        vec![0; batch.num_rows()],
                    )) as ArrayRef])?)
                } else {
                    Some(converter.convert_columns(batch.columns())?)
                };
            for row in 0..batch.num_rows() {
                let key = converted.as_ref().unwrap().row(row).owned();
                let bytes = key.as_ref().to_vec();
                let entry = &state.groups[index];
                if entry.values.len() != expected.values.len()
                    || entry
                        .values
                        .iter()
                        .zip(&expected.values)
                        .any(|(actual, expected)| !same_accumulator_shape(actual, expected))
                {
                    return Err(snapshot_shape_error("aggregate accumulator shape mismatch"));
                }
                if groups.insert(bytes.clone(), entry.clone()).is_some() {
                    return Err(snapshot_shape_error("duplicate canonical group key"));
                }
                if let Some(cache) = &mut native_keys {
                    cache.insert(
                        bytes,
                        crate::core::ipc::typed_keys::TypedKeyRow::capture(&batch, row)?,
                    );
                }
                index += 1;
            }
        }
        self.groups = groups;
        self.native_keys = native_keys;
        self.schema = Some(schema);
        self.converter = Some(converter);
        self.maintained_cut = None;
        #[cfg(test)]
        {
            self.work = 0;
        }
        Ok(())
    }
}

fn same_accumulator_shape(
    actual: &hotlap_core::snapshot::AggValue,
    expected: &hotlap_core::snapshot::AggValue,
) -> bool {
    use hotlap_core::snapshot::AggValue;
    matches!(
        (actual, expected),
        (AggValue::Count(_), AggValue::Count(_))
            | (AggValue::SumInteger { .. }, AggValue::SumInteger { .. })
            | (AggValue::SumFloat { .. }, AggValue::SumFloat { .. })
            | (AggValue::Avg { .. }, AggValue::Avg { .. })
            | (AggValue::Min(_), AggValue::Min(_))
            | (AggValue::Max(_), AggValue::Max(_))
    )
}

fn snapshot_shape_error(detail: &str) -> EngineError {
    EngineError::Infrastructure(format!("invalid restored GroupAggregate state: {detail}"))
}
