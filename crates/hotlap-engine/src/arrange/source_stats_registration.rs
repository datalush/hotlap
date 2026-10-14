use std::collections::HashMap;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::row::Row;

use super::source_stats::{SourceColumnDemand, SourceColumnValue, SourceStats};
use super::{KeyedArrangement, source_column_value};
#[cfg(test)]
use super::{
    SOURCE_STATS_KEY_DECODE_CALLS, SOURCE_STATS_PAYLOAD_ROWS_DECODED,
    SOURCE_STATS_REBUILD_ROWS_VISITED,
};
use crate::error::EngineError;

impl KeyedArrangement {
    pub(crate) fn register_source_stats(
        &mut self,
        columns: &[usize],
        data_types: &[DataType],
        sums: &[bool],
    ) -> Result<(), EngineError> {
        if columns.len() != data_types.len() || columns.len() != sums.len() {
            return Err(EngineError::Unsupported(
                "source stats demand shape mismatch".into(),
            ));
        }
        let demand = columns
            .iter()
            .zip(data_types)
            .zip(sums)
            .map(|((&index, data_type), &sum)| {
                let field = self.schema.fields().get(index).ok_or_else(|| {
                    EngineError::Unsupported("source stats column is out of range".into())
                })?;
                if field.data_type() != data_type {
                    return Err(EngineError::Unsupported(
                        "source stats schema changed after demand compilation".into(),
                    ));
                }
                if sum
                    && !matches!(
                        data_type,
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                    )
                {
                    return Err(EngineError::Unsupported(
                        "source SUM stats require a signed integer column".into(),
                    ));
                }
                Ok(SourceColumnDemand { index, sum })
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        if let Some(existing) = &self.source_stats_demand {
            return if existing == &demand {
                Ok(())
            } else {
                Err(EngineError::Unsupported(
                    "source stats demand changed after registration".into(),
                ))
            };
        }

        let mut rebuilt = HashMap::with_capacity(self.state.key_count());
        for (key, mut rows) in self.state.iter_buckets() {
            let entries = rows
                .by_ref()
                .map(|(payload, weight)| (payload.as_ref(), *weight))
                .collect::<Vec<_>>();
            let payload_bytes = entries
                .iter()
                .map(|(payload, _)| *payload)
                .collect::<Vec<_>>();
            let key_arrays = decode_key(self, key, &demand)?;
            let payload_arrays = decode_payloads(self, &payload_bytes, &demand)?;
            let mut stats = SourceStats::default();
            for (row, (_, weight)) in entries.iter().enumerate() {
                #[cfg(test)]
                SOURCE_STATS_REBUILD_ROWS_VISITED.with(|count| count.set(count.get() + 1));
                let values = values_at(
                    self,
                    &demand,
                    key_arrays.as_deref(),
                    payload_arrays.as_deref(),
                    row,
                )?;
                stats = stats.stage_weight_change(0, *weight, &values)?;
            }
            rebuilt.insert(key.to_vec(), stats);
        }
        self.source_stats = rebuilt;
        self.source_stats_demand = Some(demand);
        Ok(())
    }
}

fn decode_key(
    owner: &KeyedArrangement,
    key: &[u8],
    demand: &[SourceColumnDemand],
) -> Result<Option<Vec<ArrayRef>>, EngineError> {
    if !demand
        .iter()
        .any(|column| owner.key_indices.contains(&column.index))
    {
        return Ok(None);
    }
    #[cfg(test)]
    SOURCE_STATS_KEY_DECODE_CALLS.with(|count| count.set(count.get() + 1));
    let parser = owner.key_converter.parser();
    Ok(Some(owner.key_converter.convert_rows([parser.parse(key)])?))
}

fn decode_payloads(
    owner: &KeyedArrangement,
    rows: &[&[u8]],
    demand: &[SourceColumnDemand],
) -> Result<Option<Vec<ArrayRef>>, EngineError> {
    if rows.is_empty()
        || !demand
            .iter()
            .any(|column| owner.payload_indices.contains(&column.index))
    {
        return Ok(None);
    }
    #[cfg(test)]
    SOURCE_STATS_PAYLOAD_ROWS_DECODED.with(|count| count.set(count.get() + rows.len()));
    let parser = owner.payload_converter.parser();
    let parsed = rows
        .iter()
        .map(|row| parser.parse(row))
        .collect::<Vec<Row<'_>>>();
    Ok(Some(owner.payload_converter.convert_rows(parsed)?))
}

pub(super) fn values_for_changed_rows(
    owner: &KeyedArrangement,
    rows: &[(Vec<u8>, Vec<u8>, i64, i64)],
    demand: &[SourceColumnDemand],
) -> Result<Vec<Vec<SourceColumnValue>>, EngineError> {
    if demand.is_empty() {
        return Ok(vec![Vec::new(); rows.len()]);
    }
    let mut groups = HashMap::<&[u8], Vec<(usize, &[u8])>>::new();
    for (index, (key, payload, _, _)) in rows.iter().enumerate() {
        groups.entry(key).or_default().push((index, payload));
    }
    let mut result = vec![Vec::new(); rows.len()];
    for (key, payloads) in groups {
        let key_arrays = decode_key(owner, key, demand)?;
        let bytes = payloads
            .iter()
            .map(|(_, payload)| *payload)
            .collect::<Vec<_>>();
        let payload_arrays = decode_payloads(owner, &bytes, demand)?;
        for (row, (index, _)) in payloads.iter().enumerate() {
            result[*index] = values_at(
                owner,
                demand,
                key_arrays.as_deref(),
                payload_arrays.as_deref(),
                row,
            )?;
        }
    }
    Ok(result)
}

fn values_at(
    owner: &KeyedArrangement,
    demand: &[SourceColumnDemand],
    key_arrays: Option<&[ArrayRef]>,
    payload_arrays: Option<&[ArrayRef]>,
    row: usize,
) -> Result<Vec<SourceColumnValue>, EngineError> {
    demand
        .iter()
        .map(|column| {
            let (array, value_row) = if let Some(position) = owner
                .key_indices
                .iter()
                .position(|&index| index == column.index)
            {
                (
                    &key_arrays.expect("demanded key values decoded")[position],
                    0,
                )
            } else {
                let position = owner
                    .payload_indices
                    .iter()
                    .position(|&index| index == column.index)
                    .ok_or_else(|| EngineError::Infrastructure("source column missing".into()))?;
                (
                    &payload_arrays.expect("demanded payload values decoded")[position],
                    row,
                )
            };
            source_column_value(array.as_ref(), value_row, column.sum)
        })
        .collect()
}
