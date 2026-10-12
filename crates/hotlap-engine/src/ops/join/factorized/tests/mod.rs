use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap_core::InputId;
use hotlap_core::plan::AggSpec;
use hotlap_core::plan_normalize::{ColumnMapping, Recognition};
use hotlap_core::snapshot::{AggValue, GroupEntry};

use super::{
    ConsumerCaps, CutDecision, DeltaPair, DenseReason, GroupChange, OldState, PrepareInput, prepare,
};
use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::keys::KeyConverter;

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
    groups: &[(Vec<u8>, GroupEntry)],
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
    groups: &[(Vec<u8>, GroupEntry)],
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

mod aggregates;
mod avgs;
mod guards;
mod nulls;
mod state;
mod sums;
