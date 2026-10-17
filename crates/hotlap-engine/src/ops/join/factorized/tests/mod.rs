use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use hotlap_core::InputId;
use hotlap_core::plan::AggSpec;
use hotlap_core::plan_normalize::{ColumnMapping, Recognition};
use hotlap_core::snapshot::{AggValue, GroupEntry, GroupState, TypedKeyPayload};

use super::{
    ConsumerCaps, CutDecision, DeltaPair, DenseReason, GroupChange, OldState, PrepareInput, prepare,
};
use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::keys::KeyConverter;

mod flat_cursor;
mod flat_prepare;
mod flat_prepare_frame;
mod join_commit;
mod maintained_avg;
mod maintained_avg_certificate;

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, ty)| Field::new(*name, ty.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn zset(schema: SchemaRef, columns: Vec<ArrayRef>, diffs: &[i64]) -> ZSetBatch {
    ZSetBatch::new(
        RecordBatch::try_new(schema, columns).unwrap(),
        Arc::new(Int64Array::from(diffs.to_vec())),
    )
    .unwrap()
}

fn simple(values: &[i64], diffs: &[i64]) -> ZSetBatch {
    let schema = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    zset(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![7_i64; values.len()])),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
        diffs,
    )
}

fn arrangement(initial: &ZSetBatch, key_columns: &[usize]) -> KeyedArrangement {
    let mut result = KeyedArrangement::new(initial.schema(), key_columns).unwrap();
    let keys = KeyConverter::new(initial.schema().as_ref(), key_columns).unwrap();
    result.apply(initial, &keys).unwrap();
    result
}

fn group_map(groups: &[(Vec<u8>, GroupEntry)]) -> HashMap<Vec<u8>, GroupEntry> {
    groups.iter().cloned().collect()
}

fn group_map_from_state(state: &GroupState) -> HashMap<Vec<u8>, GroupEntry> {
    let TypedKeyPayload::Ipc(bytes) = &state.typed_keys else {
        assert!(state.groups.is_empty());
        return HashMap::new();
    };
    let schema = StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .schema();
    let (batches, rows) = crate::core::ipc::typed_keys::decode(bytes, &schema).unwrap();
    assert_eq!(rows, state.groups.len());
    let converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .unwrap();
    let mut groups = HashMap::new();
    let mut index = 0;
    for batch in batches {
        let keys = converter.convert_columns(batch.columns()).unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                groups
                    .insert(keys.row(row).as_ref().to_vec(), state.groups[index].clone())
                    .is_none()
            );
            index += 1;
        }
    }
    groups
}

fn groups_vec(groups: &HashMap<Vec<u8>, GroupEntry>) -> Vec<(Vec<u8>, GroupEntry)> {
    let mut groups: Vec<_> = groups.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    groups.sort_by(|a, b| a.0.cmp(&b.0));
    groups
}

fn empty_groups() -> HashMap<Vec<u8>, GroupEntry> {
    HashMap::new()
}

fn changed_entry<'a>(changes: &'a [GroupChange], key: &[u8]) -> Option<&'a GroupEntry> {
    changes.iter().find_map(|change| match change {
        GroupChange::Upsert {
            key: changed,
            entry,
        } if changed == key => Some(entry),
        _ => None,
    })
}

fn apply_changes_for_test(
    prior: &HashMap<Vec<u8>, GroupEntry>,
    changes: &[GroupChange],
) -> HashMap<Vec<u8>, GroupEntry> {
    let mut candidate = prior.clone();
    for change in changes {
        match change {
            GroupChange::Upsert { key, entry } => {
                candidate.insert(key.clone(), entry.clone());
            }
            GroupChange::Remove { key } => {
                candidate.remove(key);
            }
        }
    }
    candidate
}

fn recognition(aggregate_inputs: Vec<Option<usize>>, types: Vec<Option<DataType>>) -> Recognition {
    Recognition::Eligible(ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs,
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: types,
    })
}

fn run(
    inputs: DeltaPair,
    left: &KeyedArrangement,
    right: &KeyedArrangement,
    groups: &HashMap<Vec<u8>, GroupEntry>,
    recognition: Recognition,
) -> CutDecision {
    let aggregates = match &recognition {
        Recognition::Eligible(mapping) => vec![AggSpec::count(); mapping.aggregate_inputs.len()],
        Recognition::Dense(_) => vec![],
    };
    prepare(PrepareInput {
        recognition: &recognition,
        aggregates: &aggregates,
        inputs,
        state: OldState {
            left,
            right,
            groups,
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    })
}

fn run_with_aggs(
    inputs: DeltaPair,
    left: &KeyedArrangement,
    right: &KeyedArrangement,
    groups: &HashMap<Vec<u8>, GroupEntry>,
    mapping: Recognition,
    aggregates: &[AggSpec],
) -> CutDecision {
    prepare(PrepareInput {
        recognition: &mapping,
        aggregates,
        inputs,
        state: OldState {
            left,
            right,
            groups,
        },
        consumer: ConsumerCaps::FactorizedAggregates,
    })
}

fn simple_key(value: i64) -> Vec<u8> {
    let conv =
        arrow::row::RowConverter::new(vec![arrow::row::SortField::new(DataType::Int64)]).unwrap();
    conv.convert_columns(&[Arc::new(Int64Array::from(vec![value]))])
        .unwrap()
        .row(0)
        .as_ref()
        .to_vec()
}

fn typed_rows(z: &ZSetBatch) -> Vec<(Vec<String>, i64)> {
    use arrow::array::Array;
    let diff = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len())
        .map(|i| {
            (
                z.batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                            values.value(i).to_string()
                        } else if let Some(values) =
                            column.as_any().downcast_ref::<arrow::array::Float64Array>()
                        {
                            format!("{:016x}", values.value(i).to_bits())
                        } else if let Some(values) =
                            column.as_any().downcast_ref::<arrow::array::StringArray>()
                        {
                            values.value(i).to_string()
                        } else {
                            panic!("unexpected output type {:?}", column.data_type())
                        }
                    })
                    .collect(),
                diff.value(i),
            )
        })
        .collect()
}

mod aggregates;
mod avgs;
use super::history::group_key_covers_join_key;
mod certified_consume;
mod certified_consume_support;
mod certified_domains;
mod certified_grouped_parity;
mod certified_projection;
mod consume;
mod consume_failures;
mod consume_nulls;
mod consume_retractions;
mod consume_schemas;
mod guards;
mod nulls;
mod state;
mod sums;
