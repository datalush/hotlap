use super::*;

#[test]
fn fpzb_incremental_insert_retract_matches_independent_full_recompute() {
    let mut relation = Relation::default();
    let changes = [
        Change {
            id: (1, 10),
            value: Some(i64::MAX),
            delta: 1,
        },
        Change {
            id: (1, 11),
            value: None,
            delta: -2,
        },
        Change {
            id: (2, 12),
            value: Some(i64::MIN),
            delta: 3,
        },
    ];
    relation.apply(&changes).unwrap();
    assert_eq!(relation.by_key, full_recompute(&relation.rows));
    assert_eq!(relation.work.row_stages, changes.len());

    relation
        .apply(&[
            Change {
                id: (1, 10),
                value: Some(i64::MAX),
                delta: -1,
            },
            Change {
                id: (1, 11),
                value: None,
                delta: 2,
            },
            Change {
                id: (2, 12),
                value: Some(i64::MIN),
                delta: -3,
            },
        ])
        .unwrap();
    assert!(relation.rows.is_empty());
    assert!(relation.by_key.is_empty());
    assert_eq!(relation.by_key, full_recompute(&relation.rows));
}

#[test]
fn fpzb_bad_prefix_rolls_back_but_wide_net_cancellation_is_legal() {
    let mut relation = Relation::default();
    relation
        .apply(&[
            Change {
                id: (4, 1),
                value: Some(7),
                delta: 3,
            },
            Change {
                id: (4, 2),
                value: Some(8),
                delta: i64::MAX,
            },
        ])
        .unwrap();
    let before = relation.logical_snapshot();
    assert_eq!(
        relation.apply(&[
            Change {
                id: (4, 1),
                value: Some(7),
                delta: 1,
            },
            Change {
                id: (4, 2),
                value: Some(8),
                delta: 1,
            },
            Change {
                id: (4, 2),
                value: Some(8),
                delta: -1,
            },
        ]),
        Err("row weight overflow")
    );
    assert_eq!(relation.logical_snapshot(), before);

    let mut wide = Relation::default();
    wide.apply(&[
        Change {
            id: (5, 1),
            value: Some(9),
            delta: i64::MIN,
        },
        Change {
            id: (5, 2),
            value: Some(9),
            delta: i64::MAX,
        },
        Change {
            id: (5, 3),
            value: Some(9),
            delta: 1,
        },
    ])
    .unwrap();
    assert_eq!(wide.by_key[&5].weight, 0);
    assert_eq!(wide.by_key[&5].abs_weight, 1_u128 << 64);
    assert_eq!(wide.by_key, full_recompute(&wide.rows));
}

#[test]
fn fpzb_sparse_apply_matches_independent_reference_after_each_batch() {
    let mut relation = Relation::default();
    let mut reference = Reference::default();
    let batches = (0..30_i64).map(|step| {
        let id = (step % 4, step % 7);
        let value = match step % 3 {
            0 => Some(i64::MIN),
            1 => None,
            _ => Some(i64::MAX),
        };
        let delta = if step % 2 == 0 { 2 } else { -1 };
        vec![Change { id, value, delta }]
    });

    for batch in batches {
        let relation_before = relation.logical_snapshot();
        let reference_result = reference.apply(&batch);
        let actual_result = relation.apply(&batch);
        assert_eq!(actual_result, reference_result);
        if actual_result.is_err() {
            assert_eq!(relation.logical_snapshot(), relation_before);
        }
        assert_eq!(relation.rows, reference.rows);
        assert_eq!(relation.by_key, reference.summaries());
    }
}

#[test]
fn fpzb_negative_net_keeps_exact_nullable_stats_while_eligibility_changes() {
    let mut relation = Relation::default();
    relation
        .apply(&[
            Change {
                id: (6, 1),
                value: Some(i64::MIN),
                delta: -3,
            },
            Change {
                id: (6, 2),
                value: None,
                delta: 1,
            },
        ])
        .unwrap();
    let negative = &relation.by_key[&6];
    assert!(negative.weight < 0);
    assert_eq!(negative.nonnull, -3);
    assert_eq!(
        negative.sum,
        i256::from_i128(3) * i256::from_i128(1_i128 << 63)
    );
    assert_eq!(relation.by_key, full_recompute(&relation.rows));

    relation
        .apply(&[Change {
            id: (6, 1),
            value: Some(i64::MIN),
            delta: 4,
        }])
        .unwrap();
    assert!(relation.by_key[&6].weight >= 0);
    assert_eq!(relation.by_key, full_recompute(&relation.rows));
}

#[test]
fn fpzb_touched_updates_visit_d_rows_not_unrelated_n_and_rebuild_once() {
    for n in [100_i64, 1_000] {
        let mut source = Relation::default();
        source
            .apply(
                &(0..n)
                    .map(|key| Change {
                        id: (key, key),
                        value: Some(key),
                        delta: 1,
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();

        let mut rebuilt = Relation::rebuild(&source.rows);
        assert_eq!(rebuilt.work.canonical_row_reads, n as usize);
        rebuilt
            .apply(&[
                Change {
                    id: (3, 3),
                    value: Some(3),
                    delta: 1,
                },
                Change {
                    id: (n - 1, n - 1),
                    value: Some(n - 1),
                    delta: -1,
                },
            ])
            .unwrap();
        assert_eq!(rebuilt.work.canonical_row_reads, 2);
        assert_eq!(rebuilt.work.row_overlay_reads, 0);
        assert_eq!(rebuilt.work.row_stages, 2);
        assert_eq!(rebuilt.work.summary_copies, 2);
        assert_eq!(rebuilt.work.affected_summary_keys, 2);
        assert_eq!(rebuilt.by_key, full_recompute(&rebuilt.rows));
    }

    let mut repeated = Relation::default();
    repeated
        .apply(&[Change {
            id: (8, 8),
            value: Some(5),
            delta: 1,
        }])
        .unwrap();
    repeated
        .apply(&[
            Change {
                id: (8, 8),
                value: Some(5),
                delta: -1,
            },
            Change {
                id: (8, 8),
                value: Some(5),
                delta: 2,
            },
            Change {
                id: (8, 8),
                value: Some(5),
                delta: -2,
            },
        ])
        .unwrap();
    assert!(repeated.rows.is_empty());
    assert!(repeated.by_key.is_empty());
    assert_eq!(repeated.by_key, full_recompute(&repeated.rows));
}

#[test]
fn fpzb_wide_certificate_abs_handles_minimum_before_subtraction() {
    let min = i256::from_i128(i128::from(i64::MIN));
    let max = i256::from_i128(i128::from(i64::MAX));
    let two63 = i256::from_i128(1_i128 << 63);
    assert_eq!(magnitude(min), two63);
    assert_eq!(
        magnitude(max).checked_add(two63),
        Some(i256::from_i128((1_i128 << 64) - 1))
    );
    assert_eq!(two63.checked_sub(magnitude(max)), Some(i256::ONE));
}
