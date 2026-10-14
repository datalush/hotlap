use super::*;

#[test]
fn fpzb_join_memo_replaces_bilateral_and_subset_group_contributions() {
    let mut view = JoinView::default();
    view.group_for_join_key.insert(11, 7);
    view.group_for_join_key.insert(12, 7);
    view.group_for_join_key.insert(99, 99);
    view.update(
        &[
            Change {
                id: (11, 7),
                value: Some(5),
                delta: 2,
            },
            Change {
                id: (12, 7),
                value: None,
                delta: 1,
            },
            Change {
                id: (99, 99),
                value: Some(3),
                delta: 1,
            },
        ],
        &[
            Change {
                id: (11, 1),
                value: Some(2),
                delta: 3,
            },
            Change {
                id: (12, 1),
                value: Some(4),
                delta: 2,
            },
            Change {
                id: (99, 1),
                value: None,
                delta: 1,
            },
        ],
    )
    .unwrap();
    assert_eq!(view.by_group[&7].count, i256::from_i128(8));
    assert_eq!(view.by_group[&7].left_nonnull, i256::from_i128(6));
    assert_eq!(view.by_group[&7].left_sum, i256::from_i128(30));
    assert_eq!(view.by_group[&7].right_sum, i256::from_i128(20));

    view.update(
        &[Change {
            id: (11, 7),
            value: Some(5),
            delta: 1,
        }],
        &[Change {
            id: (11, 1),
            value: Some(2),
            delta: 1,
        }],
    )
    .unwrap();
    assert_eq!(view.by_group[&7].count, i256::from_i128(14));
    assert_eq!(view.by_group[&7].left_nonnull, i256::from_i128(12));
    assert_eq!(view.by_group[&7].left_sum, i256::from_i128(60));
    assert_eq!(view.by_group[&7].right_sum, i256::from_i128(32));
    assert_eq!(
        view.by_group[&99],
        Contribution {
            count: i256::ONE,
            left_nonnull: i256::ONE,
            left_sum: i256::from_i128(3),
            right_nonnull: i256::ZERO,
            ..Contribution::default()
        }
    );
    assert_eq!(view.work.changed_join_keys, 1);
    assert_eq!(view.work.memo_lookups, 1);
    assert_eq!(view.work.summary_lookups, 2);
    assert_eq!(view.work.group_total_copies, 1);
    assert_eq!(view.work.contribution_replacements, 1);

    view.update(
        &[
            Change {
                id: (11, 7),
                value: Some(5),
                delta: -3,
            },
            Change {
                id: (12, 7),
                value: None,
                delta: -1,
            },
        ],
        &[
            Change {
                id: (11, 1),
                value: Some(2),
                delta: -4,
            },
            Change {
                id: (12, 1),
                value: Some(4),
                delta: -2,
            },
        ],
    )
    .unwrap();
    assert!(!view.by_join_key.contains_key(&11));
    assert!(!view.by_group.contains_key(&7));
    assert_eq!(view.by_group[&99].count, i256::ONE);
}

#[test]
fn fpzb_source_domain_checks_are_not_claimed_as_join_certificates() {
    let mut view = JoinView::default();
    view.update(
        &[Change {
            id: (21, 1),
            value: None,
            delta: i64::MAX,
        }],
        &[Change {
            id: (21, 1),
            value: None,
            delta: 2,
        }],
    )
    .unwrap();
    assert_eq!(view.left.by_key[&21].weight, i128::from(i64::MAX));
    assert_eq!(view.right.by_key[&21].weight, 2);
    assert_eq!(
        view.by_group[&21].count,
        i256::from_i128(2 * i128::from(i64::MAX))
    );
    assert_eq!(view.gate, JoinGate::NeedsRowEvaluation);
}

#[test]
fn fpzb_group_mapping_lookup_visits_only_changed_keys() {
    super::set_group_mapping((0..1_001).map(|key| (key, key % 8)));
    let mut view = JoinView::default();
    view.update(
        &[Change {
            id: (500, 9),
            value: Some(10),
            delta: 1,
        }],
        &[Change {
            id: (500, 3),
            value: Some(20),
            delta: 1,
        }],
    )
    .unwrap();
    assert_eq!(view.work.group_mapping_reads, 1);
    assert_eq!(view.group_for_join_key.len(), 1);
}

#[test]
fn fpzb_join_failed_right_stage_leaves_both_sources_and_memos_unchanged() {
    let mut view = JoinView::default();
    view.update(
        &[Change {
            id: (5, 1),
            value: Some(4),
            delta: 1,
        }],
        &[Change {
            id: (5, 1),
            value: Some(6),
            delta: i64::MAX,
        }],
    )
    .unwrap();
    let left_before = view.left.logical_snapshot();
    let right_before = view.right.logical_snapshot();
    let memo_before = view.by_join_key.clone();
    let groups_before = view.by_group.clone();

    assert_eq!(
        view.update(
            &[Change {
                id: (5, 2),
                value: Some(7),
                delta: 1
            }],
            &[Change {
                id: (5, 1),
                value: Some(6),
                delta: 1
            }],
        ),
        Err("row weight overflow")
    );
    assert_eq!(view.left.logical_snapshot(), left_before);
    assert_eq!(view.right.logical_snapshot(), right_before);
    assert_eq!(view.by_join_key, memo_before);
    assert_eq!(view.by_group, groups_before);
}

#[test]
fn fpzb_join_dense_operator_and_group_aggregate_match_count_sum_nulls() {
    use arrow::array::{Array, ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use hotlap_core::plan::AggSpec;
    use hotlap_engine::ZSetBatch;
    use hotlap_engine::ops::{GroupAggregate, Join};

    let mut view = JoinView::default();
    view.group_for_join_key.insert(31, 8);
    view.update(
        &[
            Change {
                id: (31, 8),
                value: Some(5),
                delta: 2,
            },
            Change {
                id: (31, 9),
                value: None,
                delta: 1,
            },
        ],
        &[
            Change {
                id: (31, 1),
                value: Some(3),
                delta: 2,
            },
            Change {
                id: (31, 2),
                value: Some(7),
                delta: 1,
            },
        ],
    )
    .unwrap();

    fn input(relation: &Relation, group_for_key: &HashMap<i64, i64>, left: bool) -> ZSetBatch {
        let (fields, mut columns): (Vec<(&str, DataType)>, Vec<ArrayRef>) = if left {
            (
                vec![
                    ("jk", DataType::Int64),
                    ("group", DataType::Int64),
                    ("value", DataType::Int64),
                ],
                vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef; 3],
            )
        } else {
            (
                vec![("jk", DataType::Int64), ("value", DataType::Int64)],
                vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef; 2],
            )
        };
        let mut keys = Vec::new();
        let mut groups = Vec::new();
        let mut values = Vec::new();
        let mut diffs = Vec::new();
        for (&(key, _tag), row) in &relation.rows {
            keys.push(key);
            groups.push(*group_for_key.get(&key).unwrap_or(&key));
            values.push(row.value);
            diffs.push(row.weight);
        }
        columns[0] = Arc::new(Int64Array::from(keys));
        if left {
            columns[1] = Arc::new(Int64Array::from(groups));
            columns[2] = Arc::new(Int64Array::from(values));
        } else {
            columns[1] = Arc::new(Int64Array::from(values));
        }
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, data_type)| Field::new(name, data_type, true))
                .collect::<Vec<_>>(),
        ));
        ZSetBatch::new(
            RecordBatch::try_new(schema, columns).unwrap(),
            Arc::new(Int64Array::from(diffs)),
        )
        .unwrap()
    }

    let check_dense = |view: &JoinView| {
        let mut dense_join = Join::new(&[0], &[0]);
        let joined = dense_join
            .apply(
                &input(&view.left, &view.group_for_join_key, true),
                &input(&view.right, &view.group_for_join_key, false),
            )
            .unwrap();
        let specs = vec![
            AggSpec::count(),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(2),
            },
            AggSpec::sum(2),
            AggSpec {
                func: hotlap_core::plan::AggFunc::Count,
                input: Some(4),
            },
            AggSpec::sum(4),
        ];
        let mut dense_group = GroupAggregate::new(&[1], specs);
        let dense = dense_group.apply(&joined).unwrap();
        let group_col = dense
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(group_col.len(), view.by_group.len());
        for row in 0..group_col.len() {
            let group = group_col.value(row);
            let summary = &view.by_group[&group];
            let cells: Vec<Option<i64>> = dense
                .batch
                .columns()
                .iter()
                .skip(1)
                .map(|column| {
                    let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    (!values.is_null(row)).then(|| values.value(row))
                })
                .collect();
            assert_eq!(
                summary.count,
                i256::from_i128(i128::from(cells[0].unwrap()))
            );
            assert_eq!(
                summary.left_nonnull,
                i256::from_i128(i128::from(cells[1].unwrap()))
            );
            if summary.left_nonnull == i256::ZERO {
                assert_eq!(cells[2], None);
            } else {
                assert_eq!(
                    cells[2],
                    Some(summary.left_sum.to_string().parse::<i64>().unwrap())
                );
            }
            assert_eq!(
                summary.right_nonnull,
                i256::from_i128(i128::from(cells[3].unwrap()))
            );
            if summary.right_nonnull == i256::ZERO {
                assert_eq!(cells[4], None);
            } else {
                assert_eq!(
                    cells[4],
                    Some(summary.right_sum.to_string().parse::<i64>().unwrap())
                );
            }
        }
    };

    check_dense(&view);
    view.update(
        &[Change {
            id: (31, 8),
            value: Some(5),
            delta: 1,
        }],
        &[],
    )
    .unwrap();
    check_dense(&view);
    view.update(
        &[Change {
            id: (31, 8),
            value: Some(5),
            delta: 1,
        }],
        &[Change {
            id: (31, 1),
            value: Some(3),
            delta: 1,
        }],
    )
    .unwrap();
    check_dense(&view);
    view.update(
        &[
            Change {
                id: (31, 8),
                value: Some(5),
                delta: -4,
            },
            Change {
                id: (31, 9),
                value: None,
                delta: -1,
            },
        ],
        &[
            Change {
                id: (31, 1),
                value: Some(3),
                delta: -3,
            },
            Change {
                id: (31, 2),
                value: Some(7),
                delta: -1,
            },
        ],
    )
    .unwrap();
    check_dense(&view);

    let mut null_lifecycle = JoinView::default();
    null_lifecycle.group_for_join_key.insert(41, 8);
    null_lifecycle
        .update(
            &[Change {
                id: (41, 1),
                value: None,
                delta: 1,
            }],
            &[Change {
                id: (41, 1),
                value: Some(3),
                delta: 1,
            }],
        )
        .unwrap();
    assert_eq!(null_lifecycle.by_group[&8].count, i256::ONE);
    assert_eq!(null_lifecycle.by_group[&8].left_nonnull, i256::ZERO);
    check_dense(&null_lifecycle);

    null_lifecycle
        .update(
            &[
                Change {
                    id: (41, 1),
                    value: None,
                    delta: -1,
                },
                Change {
                    id: (41, 2),
                    value: Some(0),
                    delta: 1,
                },
            ],
            &[],
        )
        .unwrap();
    assert_eq!(null_lifecycle.by_group[&8].count, i256::ONE);
    assert_eq!(null_lifecycle.by_group[&8].left_nonnull, i256::ONE);
    assert_eq!(null_lifecycle.by_group[&8].left_sum, i256::ZERO);
    check_dense(&null_lifecycle);

    null_lifecycle
        .update(
            &[
                Change {
                    id: (41, 2),
                    value: Some(0),
                    delta: -1,
                },
                Change {
                    id: (41, 3),
                    value: None,
                    delta: 1,
                },
            ],
            &[],
        )
        .unwrap();
    assert_eq!(null_lifecycle.by_group[&8].count, i256::ONE);
    assert_eq!(null_lifecycle.by_group[&8].left_nonnull, i256::ZERO);
    check_dense(&null_lifecycle);

    null_lifecycle
        .update(
            &[],
            &[Change {
                id: (41, 1),
                value: Some(3),
                delta: -1,
            }],
        )
        .unwrap();
    assert!(!null_lifecycle.by_group.contains_key(&8));
    check_dense(&null_lifecycle);
}
