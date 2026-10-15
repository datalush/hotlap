use super::*;

#[test]
fn repeated_avg_and_reordered_count_bindings_certify_together() {
    let aggs = vec![
        AggSpec::avg(1),
        AggSpec::count(),
        AggSpec::avg(1),
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(1),
        },
        AggSpec::sum(1),
    ];
    let mut maintained = core_with_aggs(aggs.clone());
    let mut dense = core_with_aggs(aggs);
    maintained
        .push(InputId(0), &pairs(&[(1, 2, 1), (1, 6, 1)]))
        .unwrap();
    dense
        .push(InputId(0), &pairs(&[(1, 2, 1), (1, 6, 1)]))
        .unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    maintained.push(InputId(1), &pairs(&[(1, 9, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(1), &pairs(&[(1, 9, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_avg_history(&maintained, &dense);
}

#[test]
fn integer_avgs_on_both_join_sides_certify_with_shared_count_slots() {
    let mut maintained = core_with_aggs(vec![AggSpec::avg(1), AggSpec::avg(3)]);
    let mut dense = core_with_aggs(vec![AggSpec::avg(1), AggSpec::avg(3)]);
    let left = pairs(&[(1, 2, 1), (1, 4, 1)]);
    let right = pairs(&[(1, 6, 1), (1, 8, 1)]);
    maintained.push(InputId(0), &left).unwrap();
    dense.push(InputId(0), &left).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    maintained.push(InputId(1), &right).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(1), &right).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_avg_history(&maintained, &dense);
    let snapshot = maintained.snapshot(ViewId(0)).unwrap();
    let left_avg = snapshot
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let right_avg = snapshot
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(left_avg.value(0), 3.0);
    assert_eq!(right_avg.value(0), 7.0);
}

#[test]
fn projected_reordered_and_repeated_integer_avgs_certify_with_dense_parity() {
    let mut maintained = projected_avg_core();
    let mut dense = projected_avg_core();
    let left = pairs(&[(1, 2, 1), (1, 4, 1)]);
    let right = pairs(&[(1, 6, 1), (1, 8, 1)]);
    maintained.push(InputId(0), &left).unwrap();
    dense.push(InputId(0), &left).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    maintained.push(InputId(1), &right).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(1), &right).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_avg_history(&maintained, &dense);
    let snapshot = maintained.snapshot(ViewId(0)).unwrap();
    for (column, expected) in [(1, 3.0), (2, 7.0), (3, 3.0)] {
        let values = snapshot
            .batch
            .column(column)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), expected);
    }
}

#[test]
fn floating_avg_sum_min_max_remain_on_dense_join_path() {
    let mut engine = float_aggregate_core();
    engine
        .push(InputId(0), &float_pairs(&[(1, 1.5, 1), (1, 2.5, 1)]))
        .unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|count| count.set(0));
    engine
        .push(InputId(1), &float_pairs(&[(1, 8.0, 1)]))
        .unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert_eq!(count.get(), 0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|count| assert!(count.get() > 0));
    let snapshot = engine.snapshot(ViewId(0)).unwrap();
    for (index, expected) in [(1, 2.0_f64), (2, 4.0), (3, 1.5), (4, 2.5)] {
        let column = snapshot
            .batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(column.value(0).to_bits(), expected.to_bits());
    }
}
