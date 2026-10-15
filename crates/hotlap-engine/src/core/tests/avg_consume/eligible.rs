use super::*;

#[test]
fn exact_integer_avg_uses_certified_engine_cut_and_matches_dense() {
    let mut maintained = core();
    let mut dense = core();
    maintained
        .push(InputId(0), &pairs(&[(1, 2, 1), (1, 4, 1)]))
        .unwrap();
    dense
        .push(InputId(0), &pairs(&[(1, 2, 1), (1, 4, 1)]))
        .unwrap();

    crate::core::graph::FACTORIZED_CUTS.with(|n| n.set(0));
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|n| n.set(0));
    crate::core::graph::FLAT_ROWS_CUTS.with(|n| n.set(0));
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|n| n.set(0));
    maintained.push(InputId(1), &pairs(&[(1, 7, 1)])).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| {
        assert!(count.get() > 0, "eligible integer AVG should certify");
    });
    crate::core::graph::FLAT_ROWS_CUTS.with(|count| {
        assert_eq!(
            count.get(),
            0,
            "eligible integer AVG must not materialize rows"
        );
    });
    crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|count| {
        assert_eq!(
            count.get(),
            0,
            "certification must not construct join payload rows"
        );
    });
    crate::ops::join::factorized::FLAT_PAIR_PRODUCTS.with(|count| assert_eq!(count.get(), 0));
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(1), &pairs(&[(1, 7, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_avg_history(&maintained, &dense);
    let checkpoint = maintained.checkpoint().unwrap();
    let mut restored = core();
    restored.restore(&checkpoint).unwrap();
    let continuation = pairs(&[(1, 2, -1), (1, 6, 1)]);
    restored.push(InputId(0), &continuation).unwrap();
    maintained.push(InputId(0), &continuation).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(0), &continuation).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    let restored_changes = restored.take_changes(ViewId(0)).unwrap();
    let dense_changes = dense.take_changes(ViewId(0)).unwrap();
    same_batches_bitwise(&restored_changes, &dense_changes);
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &restored_changes,
    );
    same_batches_bitwise(
        &maintained.snapshot(ViewId(0)).unwrap(),
        &restored.snapshot(ViewId(0)).unwrap(),
    );
    same_avg_history(&restored, &dense);

    let replacement = pairs(&[(1, 4, -1), (1, 8, 1)]);
    maintained.push(InputId(0), &replacement).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(0), &replacement).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_batches_bitwise(
        &maintained.snapshot(ViewId(0)).unwrap(),
        &dense.snapshot(ViewId(0)).unwrap(),
    );
    same_avg_history(&maintained, &dense);
}

#[test]
fn integer_avg_certifies_inclusive_two_to_53_boundary() {
    let mut maintained = core();
    let mut dense = core();
    let boundary = pairs(&[(1, 1_i64 << 53, 1)]);
    maintained.push(InputId(0), &boundary).unwrap();
    dense.push(InputId(0), &boundary).unwrap();
    crate::core::graph::FACTORIZED_CUTS.with(|count| count.set(0));
    maintained.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(true));
    dense.push(InputId(1), &pairs(&[(1, 0, 1)])).unwrap();
    crate::core::graph::FORCE_DENSE_GROUP_JOIN.with(|force| force.set(false));
    crate::core::graph::FACTORIZED_CUTS.with(|count| assert!(count.get() > 0));
    same_batches_bitwise(
        &maintained.take_changes(ViewId(0)).unwrap(),
        &dense.take_changes(ViewId(0)).unwrap(),
    );
    same_avg_history(&maintained, &dense);
    let avg = maintained
        .snapshot(ViewId(0))
        .unwrap()
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(avg.to_bits(), ((1_i64 << 53) as f64).to_bits());
}
