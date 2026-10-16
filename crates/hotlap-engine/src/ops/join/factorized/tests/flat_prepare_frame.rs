use super::*;
use crate::ops::join::Join;
use crate::ops::join::factorized::flat::{materialize_flat_delta, prepare_flat_join_update};

fn assert_same_join_delta(left: &ZSetBatch, right: &ZSetBatch) {
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
fn prepared_flat_update_allows_multiple_readers_before_single_source_commit() {
    let initial_left = simple(&[10, 20], &[1, 1]);
    let initial_right = simple(&[3, 4], &[1, 1]);
    let left_delta = simple(&[30, 10], &[1, -1]);
    let right_delta = simple(&[5, 3], &[1, -1]);
    let mut join = Join::new(&[0], &[0]);
    join.apply(&initial_left, &initial_right).unwrap();

    let old_left = join.left_for_cut().unwrap().iter().collect::<Vec<_>>();
    let old_right = join.right_for_cut().unwrap().iter().collect::<Vec<_>>();
    let prepared = prepare_flat_join_update(
        &mut join,
        &left_delta,
        &right_delta,
        ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS,
    )
    .unwrap();
    let (first_output, sources) = prepared.into_parts();
    let second_output = materialize_flat_delta(
        &join,
        &sources,
        &left_delta,
        &right_delta,
        first_output.schema(),
        ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS,
    )
    .unwrap();

    assert_same_join_delta(&first_output, &second_output);
    assert_eq!(
        join.left_for_cut().unwrap().iter().collect::<Vec<_>>(),
        old_left
    );
    assert_eq!(
        join.right_for_cut().unwrap().iter().collect::<Vec<_>>(),
        old_right
    );

    join.commit_source_updates(sources);
    assert_eq!(
        join.left_for_cut().unwrap().iter().collect::<Vec<_>>(),
        arrangement(&simple(&[20, 30], &[1, 1]), &[0])
            .iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        join.right_for_cut().unwrap().iter().collect::<Vec<_>>(),
        arrangement(&simple(&[4, 5], &[1, 1]), &[0])
            .iter()
            .collect::<Vec<_>>()
    );
}

#[test]
fn prepared_flat_self_join_uses_both_pre_sides_before_single_commit() {
    let initial = simple(&[10, 20], &[1, 1]);
    let delta = simple(&[30, 10], &[1, -1]);
    let mut actual = Join::new(&[0], &[0]);
    let mut dense = Join::new(&[0], &[0]);
    actual.apply(&initial, &initial).unwrap();
    dense.apply(&initial, &initial).unwrap();

    let prepared = prepare_flat_join_update(
        &mut actual,
        &delta,
        &delta,
        ConsumerCaps::DEFAULT_FLAT_CHUNK_ROWS,
    )
    .unwrap();
    let (output, sources) = prepared.into_parts();
    let expected = dense.apply(&delta, &delta).unwrap();
    assert_same_join_delta(&output, &expected);

    actual.commit_source_updates(sources);
    assert_eq!(
        actual.left_for_cut().unwrap().iter().collect::<Vec<_>>(),
        actual.right_for_cut().unwrap().iter().collect::<Vec<_>>()
    );
}
