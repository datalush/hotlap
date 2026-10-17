use super::*;

fn core() -> EngineCore {
    core_with_aggs(vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)])
}

fn core_with_aggs(aggs: Vec<AggSpec>) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            }),
            key: vec![0],
            aggs,
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core
}

fn projected_avg_core() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Project {
                input: Box::new(Plan::Join {
                    left: Box::new(Plan::Source(InputId(0))),
                    right: Box::new(Plan::Source(InputId(1))),
                    left_key: vec![0],
                    right_key: vec![0],
                }),
                cols: vec![0, 1, 3, 1],
            }),
            key: vec![0],
            aggs: vec![AggSpec::avg(1), AggSpec::avg(2), AggSpec::avg(3)],
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core
}

fn float_aggregate_core() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::GroupAggregate {
            input: Box::new(Plan::Join {
                left: Box::new(Plan::Source(InputId(0))),
                right: Box::new(Plan::Source(InputId(1))),
                left_key: vec![0],
                right_key: vec![0],
            }),
            key: vec![0],
            aggs: vec![
                AggSpec::avg(1),
                AggSpec::sum(1),
                AggSpec {
                    func: hotlap_core::plan::AggFunc::Min,
                    input: Some(1),
                },
                AggSpec {
                    func: hotlap_core::plan::AggFunc::Max,
                    input: Some(1),
                },
            ],
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core
}

fn float_pairs(rows: &[(i64, f64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    ZSetBatch::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(arrow::array::Float64Array::from(
                    rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

fn same_avg_history(left: &EngineCore, right: &EngineCore) {
    use hotlap_core::snapshot::{AggValue, OperatorState};

    let left = left.checkpoint().unwrap();
    let right = right.checkpoint().unwrap();
    fn group_state(
        snapshot: &hotlap_core::snapshot::EngineSnapshot,
    ) -> &hotlap_core::snapshot::GroupState {
        snapshot.views[0]
            .operators
            .iter()
            .flatten()
            .find_map(|operator| match operator {
                OperatorState::Group(state) => Some(state),
                _ => None,
            })
            .expect("plan contains a group aggregate")
    }
    let left_state = group_state(&left);
    let right_state = group_state(&right);
    assert_eq!(left_state.groups.len(), right_state.groups.len());
    for (left_entry, right_entry) in left_state.groups.iter().zip(&right_state.groups) {
        assert_eq!(left_entry.rows, right_entry.rows);
        assert_eq!(left_entry.values.len(), right_entry.values.len());
        for (left_value, right_value) in left_entry.values.iter().zip(&right_entry.values) {
            match (left_value, right_value) {
                (
                    AggValue::Avg {
                        sum: left,
                        count: left_count,
                        nan: left_nan,
                        pos_inf: left_pos,
                        neg_inf: left_neg,
                    },
                    AggValue::Avg {
                        sum: right,
                        count: right_count,
                        nan: right_nan,
                        pos_inf: right_pos,
                        neg_inf: right_neg,
                    },
                ) => {
                    assert_eq!(left.to_bits(), right.to_bits());
                    assert_eq!(
                        (left_count, left_nan, left_pos, left_neg),
                        (right_count, right_nan, right_pos, right_neg)
                    );
                }
                (left, right) => assert_eq!(left, right),
            }
        }
    }
}

fn avg_sum_bits(core: &EngineCore) -> Vec<u64> {
    use hotlap_core::snapshot::{AggValue, OperatorState};
    core.checkpoint().unwrap().views[0]
        .operators
        .iter()
        .flatten()
        .find_map(|operator| match operator {
            OperatorState::Group(state) => Some(
                state
                    .groups
                    .iter()
                    .flat_map(|entry| entry.values.iter())
                    .filter_map(|value| match value {
                        AggValue::Avg { sum, .. } => Some(sum.to_bits()),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .expect("plan contains a group aggregate")
}

fn same_batches_bitwise(left: &ZSetBatch, right: &ZSetBatch) {
    use arrow::array::Float64Array;

    assert_eq!(left.schema(), right.schema());
    assert_eq!(left.len(), right.len());
    assert_eq!(left.batch.num_columns(), right.batch.num_columns());
    assert_eq!(
        left.diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        right
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
    );
    for (left, right) in left.batch.columns().iter().zip(right.batch.columns()) {
        match (
            left.as_any().downcast_ref::<Float64Array>(),
            right.as_any().downcast_ref::<Float64Array>(),
        ) {
            (Some(left), Some(right)) => {
                assert_eq!(left.len(), right.len());
                assert_eq!(left.null_count(), right.null_count());
                for row in 0..left.len() {
                    assert_eq!(left.is_null(row), right.is_null(row));
                    if !left.is_null(row) {
                        assert_eq!(left.value(row).to_bits(), right.value(row).to_bits());
                    }
                }
            }
            (None, None) => assert_eq!(left.to_data(), right.to_data()),
            _ => panic!("output float column type differs"),
        }
    }
}

mod direct_join_parity;
mod eligible;
mod nulls;
mod projections;
mod recovery;
