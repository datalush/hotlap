use super::*;
use crate::ops::join::factorized::PreparedCut;
use crate::ops::join::factorized::stage::TouchedRow;

fn initialized_join() -> (crate::ops::Join, ZSetBatch, ZSetBatch) {
    let left_schema = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let right_schema = schema(&[("k", DataType::Int64), ("v", DataType::Int64)]);
    let left = simple(&[10], &[1]);
    let right = simple(&[20], &[1]);
    let mut join = crate::ops::Join::new(&[0], &[0]);
    join.apply(&left, &right).unwrap();
    assert_eq!(left.schema(), left_schema);
    assert_eq!(right.schema(), right_schema);
    (join, left, right)
}

fn touched(value: i64, old: i64, new: i64) -> TouchedRow {
    let batch = simple(&[value], &[1]);
    let mut staged = crate::ops::join::factorized::stage::stage_side(
        &batch,
        &[0],
        &[],
        &KeyedArrangement::new(batch.schema(), &[0]).unwrap(),
    )
    .unwrap()
    .rows
    .remove(0);
    staged.old = old;
    staged.new = new;
    staged
}

#[test]
fn staged_commit_replaces_final_weight_instead_of_adding_it() {
    let (mut join, left, right) = initialized_join();
    join.apply(&simple(&[11], &[1]), &simple(&[], &[])).unwrap();
    join.register_demand(crate::ops::join::factorized::demand::Demand {
        left_join_key: vec![0],
        right_join_key: vec![0],
        group_key: vec![0],
        left_columns: vec![0, 1],
        right_columns: vec![],
        left_column_types: vec![DataType::Int64, DataType::Int64],
        right_column_types: vec![],
        left_column_sums: vec![true, true],
        right_column_sums: vec![],
    })
    .unwrap();
    let key = join
        .left
        .as_ref()
        .unwrap()
        .iter_key_bytes()
        .next()
        .unwrap()
        .to_vec();
    let mut prepared = empty_prepared(touched(10, 1, 2), touched(20, 1, 1));
    prepared.left.push(touched(11, 1, 3));
    KeyedArrangement::reset_source_stats_rebuild_rows_visited_for_test();
    let staged = join.stage_cut_commit(&prepared, &left, &right).unwrap();
    assert_eq!(
        KeyedArrangement::source_stats_key_decode_calls_for_test(),
        1
    );
    assert_eq!(
        KeyedArrangement::source_stats_payload_rows_decoded_for_test(),
        2
    );
    join.commit_cut(staged);
    assert_eq!(
        join.left
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| r.2)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    let stats = join.left.as_ref().unwrap().source_stats(&key).unwrap();
    assert_eq!(stats.weight, 5);
    assert_eq!(stats.support, 2);
    assert_eq!(stats.columns[0].nonnull, 5);
    assert_eq!(
        stats.columns[0].sum,
        Some(arrow::datatypes::i256::from_i128(35))
    );
    assert_eq!(stats.columns[1].nonnull, 5);
    assert_eq!(
        stats.columns[1].sum,
        Some(arrow::datatypes::i256::from_i128(53))
    );
    join.apply(&simple(&[10], &[2]), &simple(&[], &[])).unwrap();
    let stats = join.left.as_ref().unwrap().source_stats(&key).unwrap();
    assert_eq!(stats.weight, 7);
    assert_eq!(
        stats.columns[0].sum,
        Some(arrow::datatypes::i256::from_i128(49))
    );
    assert_eq!(
        stats.columns[1].sum,
        Some(arrow::datatypes::i256::from_i128(73))
    );
}

#[test]
fn invalid_right_stage_leaves_both_arrangements_unchanged() {
    let (join, left, right) = initialized_join();
    let before_left = join.left.as_ref().unwrap().iter().collect::<Vec<_>>();
    let before_right = join.right.as_ref().unwrap().iter().collect::<Vec<_>>();
    let prepared = empty_prepared(touched(10, 1, 4), touched(20, 99, 2));
    assert!(join.stage_cut_commit(&prepared, &left, &right).is_err());
    assert_eq!(
        join.left.as_ref().unwrap().iter().collect::<Vec<_>>(),
        before_left
    );
    assert_eq!(
        join.right.as_ref().unwrap().iter().collect::<Vec<_>>(),
        before_right
    );
}

#[test]
fn staged_zero_final_weight_removes_row() {
    let (mut join, left, right) = initialized_join();
    let prepared = empty_prepared(touched(10, 1, 0), touched(20, 1, 1));
    let staged = join.stage_cut_commit(&prepared, &left, &right).unwrap();
    join.commit_cut(staged);
    assert!(join.left.as_ref().unwrap().is_empty());
}

fn empty_prepared(left: TouchedRow, right: TouchedRow) -> PreparedCut {
    PreparedCut {
        epoch: 0,
        inputs: DeltaPair {
            epoch: 0,
            left: Arc::new(simple(&[], &[])),
            right: Arc::new(simple(&[], &[])),
        },
        mapping: match recognition(vec![], vec![]) {
            Recognition::Eligible(m) => m,
            _ => unreachable!(),
        },
        left: vec![left],
        right: vec![right],
        groups: vec![],
        prior_groups: vec![],
        side_bounds: (0, 0),
        staged_touched: vec![],
        flat_rows: None,
    }
}
