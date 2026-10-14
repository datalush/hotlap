use super::*;
pub(super) use crate::ops::join::factorized::certified_consume::{
    CertifiedDecision, prepare_grouped_update,
};
pub(super) use crate::ops::join::factorized::demand::compile_demand;
pub(super) use arrow::array::{BooleanArray, Int32Array, StringArray};
pub(super) use hotlap_core::plan::{AggSpec, Plan};
pub(super) use hotlap_core::plan_normalize::{Recognition, recognize};

pub(super) fn source_schema(prefix: &str) -> SchemaRef {
    schema(&[
        (&format!("{prefix}_group"), DataType::Int64),
        (&format!("{prefix}_key"), DataType::Int64),
        (&format!("{prefix}_value"), DataType::Int64),
    ])
}

pub(super) fn source_batch(
    schema: SchemaRef,
    rows: &[(i64, i64, Option<i64>)],
    diffs: &[i64],
) -> ZSetBatch {
    zset(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
        diffs,
    )
}

pub(super) type RichLeftRow = (Option<i64>, Option<i64>, Option<i32>, Option<&'static str>);
pub(super) type RichRightRow = (Option<i64>, Option<i64>, Option<i64>, Option<bool>);

pub(super) fn rich_left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int32, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

pub(super) fn rich_right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ]))
}

pub(super) fn rich_left(schema: SchemaRef, rows: &[RichLeftRow], diffs: &[i64]) -> ZSetBatch {
    zset(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
        diffs,
    )
}

pub(super) fn rich_right(schema: SchemaRef, rows: &[RichRightRow], diffs: &[i64]) -> ZSetBatch {
    zset(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
        diffs,
    )
}

pub(super) fn assert_source_arrangements_equal(
    candidate: &crate::ops::Join,
    dense: &crate::ops::Join,
) {
    for (left, right) in [
        (
            candidate.left_for_cut().unwrap(),
            dense.left_for_cut().unwrap(),
        ),
        (
            candidate.right_for_cut().unwrap(),
            dense.right_for_cut().unwrap(),
        ),
    ] {
        let mut keys = left
            .iter_key_bytes()
            .map(<[u8]>::to_vec)
            .collect::<Vec<_>>();
        keys.sort();
        let mut right_keys = right
            .iter_key_bytes()
            .map(<[u8]>::to_vec)
            .collect::<Vec<_>>();
        right_keys.sort();
        assert_eq!(keys, right_keys);
        for key in keys {
            assert_eq!(
                left.source_stats(&key),
                right.source_stats(&key),
                "source stats differ at canonical key {key:?}"
            );
        }
    }
}

pub(super) fn grouped_mapping(group_key: Vec<usize>) -> (ColumnMapping, Vec<AggSpec>) {
    let aggs = vec![
        AggSpec::sum(6),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(3),
        },
        AggSpec::sum(2),
        AggSpec::count(),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(7),
        },
    ];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key,
        aggregate_inputs: vec![Some(6), Some(3), Some(2), None, Some(7)],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![
            Some(DataType::Int64),
            Some(DataType::Utf8),
            Some(DataType::Int32),
            None,
            Some(DataType::Boolean),
        ],
    };
    (mapping, aggs)
}
