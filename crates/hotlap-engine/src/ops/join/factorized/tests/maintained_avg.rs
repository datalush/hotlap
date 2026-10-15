use super::*;
use hotlap_core::plan::AggFunc;

fn left_source(rows: &[(i64, i64, Option<i64>)], diffs: &[i64]) -> ZSetBatch {
    let schema = schema(&[
        ("k", DataType::Int64),
        ("id", DataType::Int64),
        ("v", DataType::Int64),
    ]);
    zset(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
        diffs,
    )
}

fn mapping(aggregates: &[AggSpec]) -> ColumnMapping {
    ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        aggregate_inputs: aggregates.iter().map(|agg| agg.input).collect(),
        join_key_types: vec![DataType::Int64],
        aggregate_input_types: aggregates
            .iter()
            .map(|agg| agg.input.map(|_| DataType::Int64))
            .collect(),
    }
}

fn group_sum(group: &crate::ops::GroupAggregate) -> (f64, i64) {
    let entry = group.retained_groups().values().next().unwrap();
    let AggValue::Avg { sum, count, .. } = entry.values[0] else {
        panic!("first aggregate must be AVG")
    };
    (sum, count)
}

#[test]
fn maintained_avg_count_expr_rejects_dense_rounding_hidden_by_missing_raw_sum_demand() {
    let m = 1_i64 << 53;
    let aggs = vec![
        AggSpec::avg(2),
        AggSpec {
            func: AggFunc::Count,
            input: Some(2),
        },
    ];
    let mapping = mapping(&aggs);
    let old_left = left_source(&[(7, 3, Some(m - 1)), (7, 4, Some(0))], &[1, 1]);
    let right_schema = schema(&[("k", DataType::Int64)]);
    let right_initial = zset(
        right_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![7]))],
        &[1],
    );
    let empty_right = ZSetBatch::empty(right_schema.clone());
    let delta = left_source(
        &[
            (7, 1, Some(2)),
            (7, 2, Some(m - 3)),
            (7, 3, Some(m - 1)),
            (7, 4, Some(0)),
        ],
        &[1, 1, -1, -1],
    );

    let mut dense_join = crate::ops::Join::new(&[0], &[0]);
    let dense_old_rows = dense_join.apply(&old_left, &right_initial).unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    dense_group.apply(&dense_old_rows).unwrap();
    let (old_dense_sum, old_count) = group_sum(&dense_group);
    assert_eq!(old_count, 2);
    assert_eq!(old_dense_sum.to_bits(), ((m - 1) as f64).to_bits());
    let dense_rows = dense_join.apply(&delta, &empty_right).unwrap();
    dense_group.apply(&dense_rows).unwrap();
    let (new_dense_sum, new_count) = group_sum(&dense_group);
    assert_eq!(new_count, 2);
    assert_ne!(
        new_dense_sum.to_bits(),
        old_dense_sum.to_bits(),
        "fixture must expose actual Dense fold-order drift"
    );

    let mut owner_join = crate::ops::Join::new(&[0], &[0]);
    let owner_old_rows = owner_join.apply(&old_left, &right_initial).unwrap();
    let left_schema = old_left.schema();
    owner_join
        .register_demand(
            super::super::demand::compile_demand(&mapping, &aggs, &left_schema, &right_schema)
                .unwrap(),
        )
        .unwrap();
    let mut owner_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    owner_group.apply(&owner_old_rows).unwrap();
    assert!(
        owner_group
            .initialize_maintained_cut(
                &mapping,
                owner_join.left_for_cut().unwrap(),
                owner_join.right_for_cut().unwrap(),
            )
            .unwrap()
    );
    assert_eq!(
        owner_group.maintained_cut().unwrap().left_raw_columns,
        vec![super::super::raw_bounds::RawColumnDemand {
            index: 2,
            sum: true,
        }]
    );

    let full_key = owner_join
        .left_for_cut()
        .unwrap()
        .iter_key_bytes()
        .next()
        .unwrap()
        .to_vec();
    let cut = owner_group.maintained_cut().unwrap();
    let group_key = cut.codec.encode(&full_key).unwrap();
    let old_entry = owner_group.retained_groups().get(&group_key).unwrap();
    let old_left_stats = owner_join
        .left_for_cut()
        .unwrap()
        .source_stats(&full_key)
        .unwrap();
    let old_right_stats = owner_join
        .right_for_cut()
        .unwrap()
        .source_stats(&full_key)
        .unwrap();
    let sources = owner_join
        .prepare_source_updates(&delta, &empty_right)
        .unwrap();
    let left_raw = super::super::raw_bounds::collect_raw_delta(
        &delta,
        &mapping.left_join_key,
        &cut.left_raw_columns,
    )
    .unwrap();
    let right_raw = super::super::raw_bounds::collect_raw_delta(
        &empty_right,
        &mapping.right_join_key,
        &cut.right_raw_columns,
    )
    .unwrap();
    let next_contribution = super::super::group_memo::Contribution::from_optional_source_stats(
        group_key.clone(),
        sources.proposed_left(&owner_join, &full_key),
        sources.proposed_right(&owner_join, &full_key),
        &cut.demands,
    )
    .unwrap();
    let memo_stage = owner_group
        .stage_maintained_memo(vec![(full_key.clone(), Some(next_contribution))])
        .unwrap()
        .unwrap();
    let proposed = owner_group
        .maintained_memo()
        .unwrap()
        .group_after_stage(&memo_stage, &group_key);
    let key_bounds = super::super::raw_bounds::derive_key_bounds(
        Some(old_left_stats),
        Some(old_right_stats),
        left_raw.get(&full_key),
        right_raw.get(&full_key),
        &cut.demands,
    )
    .unwrap();
    let group_bounds =
        super::super::certificate::aggregate_group_bounds([(group_key.clone(), key_bounds)])
            .unwrap();
    let bindings = super::super::certificate::compile_metric_bindings(&mapping, &aggs).unwrap();
    let decision = super::super::certificate::certify_group(
        Some(old_entry),
        owner_group.maintained_memo().unwrap().group(&group_key),
        proposed,
        group_bounds.get(&group_key).unwrap(),
        &bindings,
    );
    assert_eq!(
        decision,
        super::super::certificate::CertificateDecision::NeedsRowEvaluation,
        "actual Dense history drifted from {:#018x} to {:#018x}; AVG COUNT(expr) raw columns were {:?}",
        old_dense_sum.to_bits(),
        new_dense_sum.to_bits(),
        owner_group.maintained_cut().unwrap().left_raw_columns,
    );
}

#[test]
fn maintained_avg_raw_sum_demand_is_shared_with_sum_and_present_for_avg_only() {
    for aggs in [
        vec![AggSpec::avg(2)],
        vec![AggSpec::avg(2), AggSpec::sum(2)],
    ] {
        let mapping = mapping(&aggs);
        let left = left_source(&[(7, 1, Some(9))], &[1]);
        let right_schema = schema(&[("k", DataType::Int64)]);
        let right = zset(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![7]))],
            &[1],
        );
        let mut join = crate::ops::Join::new(&[0], &[0]);
        let joined = join.apply(&left, &right).unwrap();
        join.register_demand(
            super::super::demand::compile_demand(&mapping, &aggs, &left.schema(), &right_schema)
                .unwrap(),
        )
        .unwrap();
        let mut group = crate::ops::GroupAggregate::new(&[0], aggs);
        group.apply(&joined).unwrap();
        assert!(
            group
                .initialize_maintained_cut(
                    &mapping,
                    join.left_for_cut().unwrap(),
                    join.right_for_cut().unwrap(),
                )
                .unwrap()
        );
        assert_eq!(
            group.maintained_cut().unwrap().left_raw_columns,
            vec![super::super::raw_bounds::RawColumnDemand {
                index: 2,
                sum: true,
            }]
        );
    }
}

#[test]
fn maintained_avg_count_star_uses_cardinality_slot_after_avg_nn_slot() {
    let cases = [
        (vec![AggSpec::avg(2), AggSpec::count()], 2),
        (vec![AggSpec::count(), AggSpec::avg(2)], 0),
        (vec![AggSpec::avg(2), AggSpec::avg(2), AggSpec::count()], 4),
    ];
    for (aggs, expected_row_slot) in cases {
        let mapping = mapping(&aggs);
        let left = left_source(&[(7, 1, None)], &[1]);
        let right_schema = schema(&[("k", DataType::Int64)]);
        let right = zset(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![7]))],
            &[1],
        );
        let mut join = crate::ops::Join::new(&[0], &[0]);
        let joined = join.apply(&left, &right).unwrap();
        join.register_demand(
            super::super::demand::compile_demand(&mapping, &aggs, &left.schema(), &right_schema)
                .unwrap(),
        )
        .unwrap();
        let mut group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
        group.apply(&joined).unwrap();
        assert!(
            group
                .initialize_maintained_cut(
                    &mapping,
                    join.left_for_cut().unwrap(),
                    join.right_for_cut().unwrap(),
                )
                .unwrap()
        );
        let cut = group.maintained_cut().unwrap();
        assert_eq!(cut.row_cardinality_index, expected_row_slot);
        assert_eq!(
            cut.left_raw_columns.len(),
            1,
            "repeated AVG borrows one source-column demand"
        );
        assert!(cut.left_raw_columns[0].sum);
        let group_key = cut
            .codec
            .encode(
                join.left_for_cut()
                    .unwrap()
                    .iter_key_bytes()
                    .next()
                    .unwrap(),
            )
            .unwrap();
        let metrics = &group
            .maintained_memo()
            .unwrap()
            .group(&group_key)
            .unwrap()
            .metrics;
        let mut metric_index = 0;
        for aggregate in &aggs {
            match (aggregate.func, aggregate.input) {
                (AggFunc::Avg, Some(_)) => {
                    assert_eq!(
                        metrics[metric_index + 1].subtotal,
                        arrow::datatypes::i256::ZERO
                    );
                    metric_index += 2;
                }
                (AggFunc::Count, None) => {
                    assert_eq!(metric_index, expected_row_slot);
                    assert_eq!(metrics[metric_index].subtotal, arrow::datatypes::i256::ONE);
                    metric_index += 1;
                }
                (AggFunc::Count, Some(_)) => metric_index += 1,
                _ => unreachable!(),
            }
        }
    }
}
