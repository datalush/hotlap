use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};

use crate::error::EngineError;

/// Encodes a projected group key from the already-canonical full join key.
/// Group positions may be reordered or repeated; the full key is decoded once.
pub(crate) struct GroupKeyCodec {
    full: RowConverter,
    group: RowConverter,
    mapping: Vec<usize>,
    group_types: Vec<DataType>,
}

impl GroupKeyCodec {
    pub(crate) fn known_capacity_bytes(&self) -> Result<usize, EngineError> {
        let mapping_capacity = self
            .mapping
            .capacity()
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| {
                EngineError::Infrastructure("group key codec memory size overflow".into())
            })?;
        let type_capacity = self
            .group_types
            .capacity()
            .checked_mul(std::mem::size_of::<DataType>())
            .ok_or_else(|| {
                EngineError::Infrastructure("group key codec memory size overflow".into())
            })?;
        let mut bytes = std::mem::size_of::<Self>()
            .checked_add(mapping_capacity)
            .and_then(|bytes| bytes.checked_add(type_capacity))
            .ok_or_else(|| {
                EngineError::Infrastructure("group key codec memory size overflow".into())
            })?;
        for data_type in &self.group_types {
            bytes = bytes
                .checked_add(crate::ops::join::data_type_allocated_bytes(data_type)?)
                .ok_or_else(|| {
                    EngineError::Infrastructure("group key codec memory size overflow".into())
                })?;
        }
        Ok(bytes)
    }

    pub(crate) fn new(key_types: &[DataType], mapping: &[usize]) -> Result<Self, EngineError> {
        if mapping.iter().any(|&index| index >= key_types.len()) {
            return Err(EngineError::Unsupported(
                "group key mapping out of range".into(),
            ));
        }
        let full = RowConverter::new(key_types.iter().cloned().map(SortField::new).collect())?;
        let group_types: Vec<_> = mapping
            .iter()
            .map(|&index| key_types[index].clone())
            .collect();
        let group = RowConverter::new(group_types.iter().cloned().map(SortField::new).collect())?;
        Ok(Self {
            full,
            group,
            mapping: mapping.to_vec(),
            group_types,
        })
    }

    pub(crate) fn encode(&self, full_key: &[u8]) -> Result<Vec<u8>, EngineError> {
        let parser = self.full.parser();
        let decoded = self.full.convert_rows([parser.parse(full_key)])?;
        let projected: Vec<ArrayRef> = self
            .mapping
            .iter()
            .map(|&index| decoded[index].clone())
            .collect();
        let rows = self.group.convert_columns(&projected)?;
        Ok(rows.row(0).as_ref().to_vec())
    }

    #[allow(dead_code)]
    pub(crate) fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(
            self.group_types
                .iter()
                .enumerate()
                .map(|(index, ty)| Field::new(format!("group_{index}"), ty.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }
}
