use super::*;

#[test]
#[ignore = "isolated public EngineCore push allocation comparison for borrowed vs owned key buckets"]
fn borrowed_key_public_push_saves_allocator_traffic_with_direct_indices_unchanged() {
    let fixture = Fixture {
        name: "borrowed-key-one-key-128x128",
        keys: 1,
        fanout: 128,
        duplicate_payloads: false,
        skewed: false,
        mixed_flat_view: false,
        unsafe_avg_history: false,
        subset_group: false,
        update: Update::AppendRight,
    };
    let right_delta = zset(
        vec![0; 128],
        (0..128).map(|row| Some(1_000_000 + row)).collect(),
        vec![1; 128],
    );
    let empty_right = ZSetBatch::empty(right_delta.schema());
    let left_seed = fixture.initial_left();
    let expected_pairs = expected_dense_pairs(fixture, InputId(1), &right_delta);
    let (expected_change, expected_snapshot) = expected_group_outputs_from(
        source_bag(&left_seed),
        SourceBag::new(),
        InputId(1),
        &right_delta,
    );
    assert_eq!(expected_pairs, 16_384);

    let run = |owned_keys: bool| {
        let _mode = ModeGuard::new(true);
        let _owned_keys =
            owned_keys.then(crate::ops::join::test_support::OwnedDenseJoinKeyGuard::force);
        crate::ops::join::test_support::reset_counts();
        let mut engine = build_empty(fixture);
        engine.push(InputId(0), &left_seed).unwrap();
        engine.push(InputId(1), &empty_right).unwrap();
        let _ = engine.take_changes(ViewId(0)).unwrap();

        crate::ops::join::test_support::reset_counts();
        crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(|count| count.set(0));
        let region = Region::new(TEST_ALLOCATOR);
        engine.push(InputId(1), &right_delta).unwrap();
        let stats = region.change();
        let routes = crate::ops::join::test_support::counts();
        let dense_calls = crate::core::graph::DENSE_JOIN_MATERIALIZATIONS.with(Cell::get);
        let change = engine.take_changes(ViewId(0)).unwrap();
        let snapshot = engine.snapshot(ViewId(0)).unwrap();
        (engine, stats, routes, dense_calls, change, snapshot)
    };

    let (
        borrowed,
        borrowed_stats,
        borrowed_routes,
        borrowed_calls,
        borrowed_change,
        borrowed_snapshot,
    ) = run(false);
    let (owned, owned_stats, owned_routes, owned_calls, owned_change, owned_snapshot) = run(true);

    for (routes, calls, expected_borrowed, expected_owned) in [
        (borrowed_routes, borrowed_calls, 3, 0),
        (owned_routes, owned_calls, 0, 3),
    ] {
        assert_eq!(routes.direct, 3);
        assert_eq!(routes.forced_legacy, 0);
        assert_eq!(routes.direct_pairs, expected_pairs);
        assert_eq!(routes.borrowed_key_buckets, expected_borrowed);
        assert_eq!(routes.owned_key_buckets, expected_owned);
        assert_eq!(calls, 1);
    }
    assert_same_batch(&borrowed_change, &owned_change);
    assert_typed_aggregate_rows(&borrowed_change, &expected_change, fixture.name);
    assert_same_batch(&borrowed_snapshot, &owned_snapshot);
    assert_typed_aggregate_rows(&borrowed_snapshot, &expected_snapshot, fixture.name);
    assert_eq!(borrowed.checkpoint().unwrap(), owned.checkpoint().unwrap());

    let borrowed_bytes = borrowed_stats.bytes_allocated;
    let owned_bytes = owned_stats.bytes_allocated;
    assert!(
        borrowed_bytes < owned_bytes,
        "borrowed-key public push must allocate less with direct-index routes held fixed: borrowed={borrowed_bytes} owned={owned_bytes}"
    );
    println!(
        "borrowed_key_public_push pairs={expected_pairs} helper_calls=3 borrowed_bytes={borrowed_bytes} owned_key_direct_bytes={owned_bytes} saved_bytes={}",
        owned_bytes - borrowed_bytes,
    );
}
