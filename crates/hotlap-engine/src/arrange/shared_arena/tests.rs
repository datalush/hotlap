use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::ops::join::Join;
use crate::ops::join::factorized::flat::materialize_shared_flat_delta;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> ZSetBatch {
    let keys = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let values = rows.iter().map(|row| row.0 + 100).collect::<Vec<_>>();
    let diffs = rows.iter().map(|row| row.1).collect::<Vec<_>>();
    ZSetBatch::new(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(diffs)),
    )
    .unwrap()
}

fn nullable_batch(rows: &[(Option<i64>, i64, i64)]) -> ZSetBatch {
    let keys = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let values = rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>();
    let diffs = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    ZSetBatch::new(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
            ],
        )
        .unwrap(),
        Arc::new(Int64Array::from(diffs)),
    )
    .unwrap()
}

fn descriptor(source: u64, partner: u64, keys: Vec<usize>) -> ArrangementDescriptor {
    ArrangementDescriptor {
        root_left_source: source,
        root_right_source: partner,
        root_left_keys: keys.clone(),
        root_right_keys: vec![0],
        root_left_schema: schema(),
        root_right_schema: schema(),
        source,
        partner,
        key_indices: keys,
        partner_key_indices: vec![0],
        schema: schema(),
        cohort: 1,
        consumed_revision_start: 0,
        consumed_revision_end: 0,
        demand_none: true,
    }
}

fn same_delta(left: &ZSetBatch, right: &ZSetBatch) {
    assert_eq!(left.batch, right.batch);
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
}

#[test]
fn descriptors_alias_only_on_full_equality_and_final_release_retires_generation() {
    let mut arena = ArrangementArena::default();
    let first = arena.lease(descriptor(4, 8, vec![0])).unwrap();
    let alias = arena.lease(descriptor(4, 8, vec![0])).unwrap();
    let partner = arena.lease(descriptor(4, 9, vec![0])).unwrap();
    let keys = arena.lease(descriptor(4, 8, vec![1])).unwrap();
    assert_eq!(first, alias);
    assert_ne!(first, partner);
    assert_ne!(first, keys);
    arena.release(first).unwrap();
    assert_eq!(arena.owner_count(), 3);
    arena.release(alias).unwrap();
    assert!(arena.owner(first).is_err());
    let reused = arena.lease(descriptor(90, 91, vec![0])).unwrap();
    assert_eq!(first.slot, reused.slot);
    assert_ne!(first.generation, reused.generation);
    assert!(arena.owner(first).is_err());
}

#[test]
fn frame_keeps_pre_rows_and_rejects_conflicting_revision_without_commit() {
    let mut arena = ArrangementArena::default();
    let owner = arena.lease(descriptor(1, 2, vec![0])).unwrap();
    let mut frame = arena.frame();
    frame.stage(owner, 1, &batch(&[(7, 2)])).unwrap();
    frame.stage(owner, 1, &batch(&[(7, 2)])).unwrap();
    assert!(frame.owner(owner).unwrap().to_zset().unwrap().is_empty());
    assert!(frame.stage(owner, 1, &batch(&[(7, 1)])).is_err());
    assert!(frame.commit().is_err());
    assert!(arena.owner(owner).unwrap().to_zset().unwrap().is_empty());
}

#[test]
fn shared_flat_adapter_matches_private_join_for_bilateral_signed_updates() {
    let left_seed = batch(&[(7, 2)]);
    let right_seed = batch(&[(7, 2)]);
    let left_delta = batch(&[(7, 2), (7, -1)]);
    let right_delta = batch(&[(7, -1)]);
    let mut private = Join::new(&[0], &[0]);
    let output_schema = private.apply(&left_seed, &right_seed).unwrap().schema();
    let mut arena = ArrangementArena::default();
    let left = arena.lease(descriptor(1, 2, vec![0])).unwrap();
    let right = arena.lease(descriptor(2, 1, vec![0])).unwrap();
    let mut seed = arena.frame();
    seed.stage(left, 1, &left_seed).unwrap();
    seed.stage(right, 1, &right_seed).unwrap();
    seed.commit().unwrap();

    let expected_state = private.export_state().unwrap();
    let shared_state = Join::export_external_state(
        Some(arena.owner(left).unwrap()),
        Some(arena.owner(right).unwrap()),
        None,
        None,
    )
    .unwrap();
    assert_eq!(shared_state, expected_state);

    let mut frame = arena.frame();
    frame.stage(left, 2, &left_delta).unwrap();
    frame.stage(right, 2, &right_delta).unwrap();
    let actual = materialize_shared_flat_delta(
        frame.owner(left).unwrap(),
        frame.owner(right).unwrap(),
        &left_delta,
        &right_delta,
        &[0],
        &[0],
        output_schema,
        1,
    )
    .unwrap();
    let expected = private.apply(&left_delta, &right_delta).unwrap();
    same_delta(&actual, &expected);
    assert_eq!(frame.owner(left).unwrap().to_zset().unwrap().len(), 1);
    assert_eq!(frame.owner(right).unwrap().to_zset().unwrap().len(), 1);
    frame.commit().unwrap();
    assert_eq!(
        arena
            .owner(left)
            .unwrap()
            .to_zset()
            .unwrap()
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        arena
            .owner(right)
            .unwrap()
            .to_zset()
            .unwrap()
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );

    let left_seed = nullable_batch(&[(None, 42, 1)]);
    let right_seed = nullable_batch(&[(None, 43, 2)]);
    let left_delta = nullable_batch(&[(None, 42, 2), (None, 42, -1), (None, 42, 0)]);
    let right_delta = nullable_batch(&[]);
    let mut private = Join::new(&[0], &[0]);
    let output_schema = private.apply(&left_seed, &right_seed).unwrap().schema();
    let mut arena = ArrangementArena::default();
    let left = arena.lease(descriptor(11, 12, vec![0])).unwrap();
    let right = arena.lease(descriptor(12, 11, vec![0])).unwrap();
    let mut seed = arena.frame();
    seed.stage(left, 1, &left_seed).unwrap();
    seed.stage(right, 1, &right_seed).unwrap();
    seed.commit().unwrap();
    let mut frame = arena.frame();
    frame.stage(left, 2, &left_delta).unwrap();
    frame.stage(right, 2, &right_delta).unwrap();
    let actual = materialize_shared_flat_delta(
        frame.owner(left).unwrap(),
        frame.owner(right).unwrap(),
        &left_delta,
        &right_delta,
        &[0],
        &[0],
        output_schema,
        1,
    )
    .unwrap();
    let expected = private.apply(&left_delta, &right_delta).unwrap();
    same_delta(&actual, &expected);
    frame.commit().unwrap();
}

#[test]
fn self_join_adapter_reads_one_pre_owner_twice_and_commits_once() {
    let seed = batch(&[(11, 1)]);
    let delta = batch(&[(11, 1)]);
    let mut private = Join::new(&[0], &[0]);
    let output_schema = private.apply(&seed, &seed).unwrap().schema();
    let mut arena = ArrangementArena::default();
    let owner = arena.lease(descriptor(3, 3, vec![0])).unwrap();
    let mut seed_frame = arena.frame();
    seed_frame.stage(owner, 1, &seed).unwrap();
    seed_frame.commit().unwrap();
    let mut frame = arena.frame();
    frame.stage(owner, 2, &delta).unwrap();
    let actual = materialize_shared_flat_delta(
        frame.owner(owner).unwrap(),
        frame.owner(owner).unwrap(),
        &delta,
        &delta,
        &[0],
        &[0],
        output_schema,
        1,
    )
    .unwrap();
    let expected = private.apply(&delta, &delta).unwrap();
    same_delta(&actual, &expected);
    assert_eq!(
        actual
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    frame.commit().unwrap();
    assert_eq!(
        arena
            .owner(owner)
            .unwrap()
            .to_zset()
            .unwrap()
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[test]
fn empty_self_join_tables_move_to_arena_and_keep_logical_export() {
    let empty = batch(&[]);
    let mut join = Join::new(&[0], &[0]);
    join.apply(&empty, &empty).unwrap();
    let expected = join.export_state().unwrap();
    let (left, right) = join.take_empty_source_arrangements(true).unwrap();
    assert!(right.is_none());
    let mut arena = ArrangementArena::default();
    let owner = arena
        .lease_arrangement(descriptor(3, 3, vec![0]), left)
        .unwrap();
    join.ensure_ready(&empty, &empty).unwrap();
    assert!(join.left_for_cut().is_none() && join.right_for_cut().is_none());
    assert_eq!(join.joined_schema_for_cut().unwrap().fields().len(), 4);
    let actual = Join::export_external_state(
        Some(arena.owner(owner).unwrap()),
        Some(arena.owner(owner).unwrap()),
        None,
        None,
    )
    .unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn nonempty_join_tables_are_not_taken_or_discarded() {
    let rows = batch(&[(5, 1)]);
    let mut join = Join::new(&[0], &[0]);
    join.apply(&rows, &rows).unwrap();
    assert!(join.take_empty_source_arrangements(true).is_err());
    assert_eq!(join.left_for_cut().unwrap().to_zset().unwrap().len(), 1);
    assert_eq!(join.right_for_cut().unwrap().to_zset().unwrap().len(), 1);
}

#[test]
fn arena_rejects_demands_and_invalid_revision_ranges() {
    let mut arena = ArrangementArena::default();
    let mut demanded = descriptor(1, 2, vec![0]);
    demanded.demand_none = false;
    assert!(arena.lease(demanded).is_err());
    let mut range = descriptor(1, 2, vec![0]);
    range.consumed_revision_start = 9;
    range.consumed_revision_end = 8;
    assert!(arena.lease(range).is_err());
}
