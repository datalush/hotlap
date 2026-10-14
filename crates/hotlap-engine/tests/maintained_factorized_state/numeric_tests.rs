use super::{Change, Relation, Summary};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::i256;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::plan::AggSpec;
use hotlap_engine::ZSetBatch;
use hotlap_engine::ops::GroupAggregate;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceDomainStatus {
    SourceDomainOnly,
    NeedsRowEvaluation,
}

fn narrow_i128(value: i256) -> Option<i128> {
    value.to_string().parse().ok()
}

fn classify_source_state(summary: &Summary) -> SourceDomainStatus {
    if summary.weight < 0
        || summary.nonnull < 0
        || summary.negative_rows != 0
        || i64::try_from(summary.weight).is_err()
        || i64::try_from(summary.nonnull).is_err()
        || narrow_i128(summary.sum).is_none()
        || summary.sum < i256::from_i128(i128::from(i64::MIN))
        || summary.sum > i256::from_i128(i128::from(i64::MAX))
        || summary.abs_sum > i256::from_i128(i128::from(i64::MAX))
    {
        SourceDomainStatus::NeedsRowEvaluation
    } else {
        SourceDomainStatus::SourceDomainOnly
    }
}

fn classify_source_update(old: &Summary, changes: &[Change], next: &Summary) -> SourceDomainStatus {
    let (Ok(mut rows), Ok(mut nonnull), Some(mut sum)) = (
        i64::try_from(old.weight),
        i64::try_from(old.nonnull),
        narrow_i128(old.sum),
    ) else {
        return SourceDomainStatus::NeedsRowEvaluation;
    };
    for change in changes {
        let Some(next_rows) = rows.checked_add(change.delta) else {
            return SourceDomainStatus::NeedsRowEvaluation;
        };
        if next_rows < 0 {
            return SourceDomainStatus::NeedsRowEvaluation;
        }
        rows = next_rows;
        if let Some(value) = change.value {
            let Some(next_nonnull) = nonnull.checked_add(change.delta) else {
                return SourceDomainStatus::NeedsRowEvaluation;
            };
            if next_nonnull < 0 {
                return SourceDomainStatus::NeedsRowEvaluation;
            }
            nonnull = next_nonnull;
            let Some(term) = i128::from(value).checked_mul(i128::from(change.delta)) else {
                return SourceDomainStatus::NeedsRowEvaluation;
            };
            let Some(next_sum) = sum.checked_add(term) else {
                return SourceDomainStatus::NeedsRowEvaluation;
            };
            sum = next_sum;
        }
    }
    if i128::from(rows) != next.weight
        || i128::from(nonnull) != next.nonnull
        || i256::from_i128(sum) != next.sum
    {
        return SourceDomainStatus::NeedsRowEvaluation;
    }
    classify_source_state(next)
}

fn batch(changes: &[(i64, Option<i64>, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let keys = changes.iter().map(|(key, _, _)| *key).collect::<Vec<_>>();
    let values = changes
        .iter()
        .map(|(_, value, _)| *value)
        .collect::<Vec<_>>();
    let diffs = changes.iter().map(|(_, _, diff)| *diff).collect::<Vec<_>>();
    ZSetBatch::new(
        RecordBatch::try_new(
            schema,
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

fn reducer() -> GroupAggregate {
    GroupAggregate::new(
        &[0],
        vec![
            AggSpec::count(),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(1),
            },
            AggSpec::sum(1),
        ],
    )
}

fn one_summary(relation: &Relation, overlay: &super::Overlay, key: i64) -> Summary {
    overlay
        .summaries
        .get(&key)
        .or_else(|| relation.by_key.get(&key))
        .cloned()
        .unwrap_or_default()
}

#[test]
fn fpzb_count_star_prefix_overflow_is_dense_error_and_not_a_certificate() {
    let changes = [Change {
        id: (1, 1),
        value: None,
        delta: i64::MAX,
    }];
    let mut source = Relation::default();
    let staged = source.prepare(&changes).unwrap();
    let old = Summary::default();
    let new = one_summary(&source, &staged, 1);
    assert_eq!(
        classify_source_update(&old, &changes, &new),
        SourceDomainStatus::SourceDomainOnly
    );
    source.commit(staged);

    let mut dense = reducer();
    dense.apply(&batch(&[(1, None, i64::MAX)])).unwrap();
    let overflowing = [
        Change {
            id: (1, 2),
            value: None,
            delta: 1,
        },
        Change {
            id: (1, 2),
            value: None,
            delta: -1,
        },
    ];
    let staged = source.prepare(&overflowing).unwrap();
    let next = one_summary(&source, &staged, 1);
    assert_eq!(
        classify_source_update(source.by_key.get(&1).unwrap(), &overflowing, &next),
        SourceDomainStatus::NeedsRowEvaluation
    );
    assert!(dense.apply(&batch(&[(1, None, 1), (1, None, -1)])).is_err());
}

#[test]
fn fpzb_nullable_count_prefix_overflow_is_checked_separately() {
    let seed = [
        Change {
            id: (2, 1),
            value: Some(0),
            delta: i64::MAX,
        },
        Change {
            id: (2, 2),
            value: None,
            delta: -(i64::MAX - 1),
        },
    ];
    let mut source = Relation::default();
    let staged = source.prepare(&seed).unwrap();
    source.commit(staged);
    let old = source.by_key[&2].clone();
    assert_eq!(old.weight, 1);
    assert_eq!(old.nonnull, i128::from(i64::MAX));
    assert_eq!(
        classify_source_state(&old),
        SourceDomainStatus::NeedsRowEvaluation
    );

    let delta = [
        Change {
            id: (2, 3),
            value: Some(0),
            delta: 1,
        },
        Change {
            id: (2, 3),
            value: Some(0),
            delta: -1,
        },
    ];
    let staged = source.prepare(&delta).unwrap();
    assert_eq!(
        classify_source_update(&old, &delta, &one_summary(&source, &staged, 2)),
        SourceDomainStatus::NeedsRowEvaluation
    );

    let mut dense = reducer();
    dense
        .apply(&batch(&[
            (2, Some(0), i64::MAX),
            (2, None, -(i64::MAX - 1)),
        ]))
        .unwrap();
    assert!(
        dense
            .apply(&batch(&[(2, Some(0), 1), (2, Some(0), -1)]))
            .is_err()
    );
}

#[test]
fn fpzb_sum_render_overflow_matches_dense_rejection_not_wide_acceptance() {
    let seed = [
        Change {
            id: (3, 1),
            value: Some(i64::MAX),
            delta: 1,
        },
        Change {
            id: (3, 2),
            value: Some(1),
            delta: 1,
        },
    ];
    let source = Relation::default();
    let staged = source.prepare(&seed).unwrap();
    let next = one_summary(&source, &staged, 3);
    assert_eq!(
        classify_source_update(&Summary::default(), &seed, &next),
        SourceDomainStatus::NeedsRowEvaluation
    );

    let mut dense = reducer();
    assert!(
        dense
            .apply(&batch(&[(3, Some(i64::MAX), 1), (3, Some(1), 1)]))
            .is_err()
    );
    assert!(next.sum > i256::from_i128(i128::from(i64::MAX)));
}

#[test]
fn fpzb_sum_raw_prefix_can_fail_even_when_wide_final_net_is_valid() {
    let seed = [
        Change {
            id: (4, 1),
            value: Some(0),
            delta: -1,
        },
        Change {
            id: (4, 2),
            value: Some(i64::MAX),
            delta: i64::MAX,
        },
        Change {
            id: (4, 3),
            value: Some(i64::MIN),
            delta: -i64::MAX,
        },
        Change {
            id: (4, 4),
            value: Some(i64::MAX),
            delta: i64::MAX,
        },
        Change {
            id: (4, 5),
            value: Some(i64::MIN),
            delta: -i64::MAX,
        },
        Change {
            id: (4, 6),
            value: Some(i64::MAX),
            delta: -i64::MAX,
        },
        Change {
            id: (4, 7),
            value: Some(i64::MIN),
            delta: i64::MAX,
        },
        Change {
            id: (4, 8),
            value: Some(i64::MAX),
            delta: -i64::MAX,
        },
        Change {
            id: (4, 9),
            value: Some(i64::MIN),
            delta: i64::MAX,
        },
    ];
    let source = Relation::default();
    let staged = source.prepare(&seed).unwrap();
    let old = Summary::default();
    let state = one_summary(&source, &staged, 4);
    assert_eq!(state.weight, -1);
    assert_eq!(state.sum, i256::ZERO);
    assert_eq!(
        classify_source_update(&old, &seed, &state),
        SourceDomainStatus::NeedsRowEvaluation
    );

    let mut dense = reducer();
    let dense_changes = seed
        .iter()
        .map(|change| (change.id.0, change.value, change.delta))
        .collect::<Vec<_>>();
    assert!(dense.apply(&batch(&dense_changes)).is_err());
}

#[test]
fn fpzb_negative_weights_keep_wide_metadata_but_never_claim_certificate() {
    let mut source = Relation::default();
    let insert_negative = [Change {
        id: (5, 1),
        value: Some(i64::MIN),
        delta: -2,
    }];
    let staged = source.prepare(&insert_negative).unwrap();
    let negative = one_summary(&source, &staged, 5);
    assert_eq!(
        classify_source_update(&Summary::default(), &insert_negative, &negative),
        SourceDomainStatus::NeedsRowEvaluation
    );
    source.commit(staged);
    assert_eq!(source.by_key[&5].weight, -2);
    assert_eq!(source.by_key[&5].abs_weight, 2);
    assert_eq!(source.by_key[&5].sum, i256::from_i128(1_i128 << 64));

    let restore = [Change {
        id: (5, 1),
        value: Some(i64::MIN),
        delta: 3,
    }];
    let old = source.by_key[&5].clone();
    let staged = source.prepare(&restore).unwrap();
    let positive = one_summary(&source, &staged, 5);
    assert_eq!(positive.weight, 1);
    assert_eq!(positive.abs_weight, 1);
    assert_eq!(
        classify_source_state(&positive),
        SourceDomainStatus::NeedsRowEvaluation
    );
    source.commit(staged);
    assert_eq!(source.by_key[&5].sum, i256::from_i128(-(1_i128 << 63)));
    assert_eq!(old.weight, -2);
}

#[test]
fn fpzb_negative_raw_prefix_falls_back_even_when_dense_accepts_final_net() {
    let seed = [Change {
        id: (7, 1),
        value: Some(0),
        delta: 1,
    }];
    let mut source = Relation::default();
    source.apply(&seed).unwrap();
    let old = source.by_key[&7].clone();
    let raw = [
        Change {
            id: (7, 1),
            value: Some(0),
            delta: -2,
        },
        Change {
            id: (7, 1),
            value: Some(0),
            delta: 2,
        },
    ];
    let staged = source.prepare(&raw).unwrap();
    let next = one_summary(&source, &staged, 7);
    assert_eq!(next.weight, old.weight);
    assert_eq!(
        classify_source_update(&old, &raw, &next),
        SourceDomainStatus::NeedsRowEvaluation
    );

    let mut dense = reducer();
    dense.apply(&batch(&[(7, Some(0), 1)])).unwrap();
    assert!(
        dense
            .apply(&batch(&[(7, Some(0), -2), (7, Some(0), 2)]))
            .is_ok()
    );
}

#[test]
fn fpzb_oversized_abs_summary_becomes_certified_after_one_touched_retraction() {
    let mut source = Relation::default();
    let seed = [
        Change {
            id: (6, 1),
            value: Some(i64::MAX),
            delta: 1,
        },
        Change {
            id: (6, 2),
            value: Some(-i64::MAX),
            delta: 1,
        },
    ];
    let mut dense = reducer();
    dense
        .apply(&batch(&[(6, Some(i64::MAX), 1), (6, Some(-i64::MAX), 1)]))
        .unwrap();
    let staged = source.prepare(&seed).unwrap();
    let before = one_summary(&source, &staged, 6);
    assert_eq!(before.sum, i256::ZERO);
    assert_eq!(
        before.abs_sum,
        i256::from_i128(i128::from(i64::MAX)) * i256::from_i128(2)
    );
    assert_eq!(
        classify_source_update(&Summary::default(), &seed, &before),
        SourceDomainStatus::NeedsRowEvaluation
    );
    source.commit(staged);

    let retract = [Change {
        id: (6, 2),
        value: Some(-i64::MAX),
        delta: -1,
    }];
    let staged = source.prepare(&retract).unwrap();
    let after = one_summary(&source, &staged, 6);
    assert_eq!(
        classify_source_update(source.by_key.get(&6).unwrap(), &retract, &after),
        SourceDomainStatus::SourceDomainOnly
    );
    assert_eq!(staged.work.canonical_row_reads, 1);
    assert_eq!(staged.work.summary_copies, 1);
    dense.apply(&batch(&[(6, Some(-i64::MAX), -1)])).unwrap();
    source.commit(staged);
    assert_eq!(
        source.by_key[&6].abs_sum,
        i256::from_i128(i128::from(i64::MAX))
    );
    assert_eq!(
        classify_source_state(&source.by_key[&6]),
        SourceDomainStatus::SourceDomainOnly
    );
}
