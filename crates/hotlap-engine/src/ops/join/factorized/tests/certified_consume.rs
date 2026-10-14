use super::certified_consume_support::*;
use super::*;

#[test]
fn certified_mixed_integer_aggregates_match_incremental_dense_before_publication() {
    let left_schema = source_schema("l");
    let right_schema = source_schema("r");
    let left_initial = source_batch(left_schema.clone(), &[(7, 1, None)], &[1]);
    let right_initial = source_batch(right_schema.clone(), &[(7, 1, Some(5))], &[2]);
    let aggs = vec![
        AggSpec::sum(5), // SUM(right) precedes the row-count metrics.
        AggSpec {
            func: hotlap_core::plan::AggFunc::Count,
            input: Some(2),
        },
        AggSpec::sum(2),
        AggSpec::count(),
    ];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![Some(5), Some(2), Some(2), None],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![
            Some(DataType::Int64),
            Some(DataType::Int64),
            Some(DataType::Int64),
            None,
        ],
    };

    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let candidate_initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    let demand = compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap();
    candidate_join.register_demand(demand).unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&candidate_initial).unwrap();

    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    dense_group.apply(&dense_initial).unwrap();

    let left_delta = source_batch(left_schema, &[(7, 1, Some(0))], &[1]);
    let right_delta = source_batch(right_schema, &[(7, 1, Some(3))], &[1]);
    let prepared = prepare_grouped_update(
        &mut candidate_join,
        &mut candidate_group,
        &mapping,
        &left_delta,
        &right_delta,
        output_schema,
    )
    .unwrap();
    let CertifiedDecision::Certified(prepared) = prepared else {
        panic!("the bounded positive COUNT/SUM update should certify");
    };

    let dense_joined = dense_join.apply(&left_delta, &right_delta).unwrap();
    let dense_output = dense_group.apply(&dense_joined).unwrap();
    assert_eq!(prepared.output().schema(), dense_output.schema());
    assert_eq!(typed_rows(prepared.output()), typed_rows(&dense_output));
    assert_eq!(prepared.group_changes().len(), 1);

    prepared.commit(&mut candidate_join, &mut candidate_group);
    assert_eq!(
        groups_vec(candidate_group.retained_groups()),
        groups_vec(dense_group.retained_groups())
    );
    for (left, right) in [
        (
            candidate_join.left_for_cut().unwrap(),
            dense_join.left_for_cut().unwrap(),
        ),
        (
            candidate_join.right_for_cut().unwrap(),
            dense_join.right_for_cut().unwrap(),
        ),
    ] {
        assert_eq!(
            left.iter().collect::<Vec<_>>(),
            right.iter().collect::<Vec<_>>()
        );
    }
    assert_eq!(candidate_group.maintained_memo().unwrap().len(), 1);
}

#[test]
fn certified_and_flat_consumers_share_one_uncommitted_source_update() {
    let left_schema = source_schema("l");
    let right_schema = source_schema("r");
    let left_initial = source_batch(left_schema.clone(), &[(7, 1, Some(3))], &[1]);
    let right_initial = source_batch(right_schema.clone(), &[(7, 1, Some(5))], &[1]);
    let left_delta = source_batch(left_schema.clone(), &[(7, 1, Some(4))], &[1]);
    let right_delta = source_batch(right_schema.clone(), &[(7, 1, Some(6))], &[1]);
    let aggs = vec![AggSpec::count()];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![None],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![None],
    };
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    candidate_join
        .register_demand(compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap())
        .unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&initial).unwrap();

    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs);
    dense_group
        .apply(&dense_join.apply(&left_initial, &right_initial).unwrap())
        .unwrap();
    dense_join
        .register_demand(
            compile_demand(&mapping, &[AggSpec::count()], &left_schema, &right_schema).unwrap(),
        )
        .unwrap();

    let sources = candidate_join
        .prepare_source_updates(&left_delta, &right_delta)
        .unwrap();
    super::super::FLAT_PAIR_PRODUCTS.with(|count| count.set(0));
    let aggregate =
        crate::ops::join::factorized::certified_consume::prepare_grouped_update_with_sources(
            &mut candidate_join,
            &mut candidate_group,
            crate::ops::join::factorized::certified_consume::GroupedUpdate {
                mapping: &mapping,
                left_delta: &left_delta,
                right_delta: &right_delta,
                output_schema: output_schema.clone(),
                projection: None,
            },
            &sources,
        )
        .unwrap();
    let crate::ops::join::factorized::certified_consume::CertifiedDecision::Certified(aggregate) =
        aggregate
    else {
        panic!("COUNT update should prepare the aggregate consumer");
    };
    super::super::FLAT_PAIR_PRODUCTS.with(|count| assert_eq!(count.get(), 0));
    let flat = crate::ops::join::factorized::flat::materialize_flat_delta(
        &candidate_join,
        &sources,
        &left_delta,
        &right_delta,
        output_schema,
        1,
    )
    .unwrap();
    super::super::FLAT_PAIR_PRODUCTS.with(|count| assert!(count.get() > 0));

    assert!(
        candidate_join
            .left_for_cut()
            .unwrap()
            .source_stats(
                candidate_join
                    .left_for_cut()
                    .unwrap()
                    .iter_key_bytes()
                    .next()
                    .unwrap()
            )
            .unwrap()
            .weight
            == 1
    );
    let dense_rows = dense_join.apply(&left_delta, &right_delta).unwrap();
    assert_eq!(typed_rows(&flat), typed_rows(&dense_rows));
    let dense_output = dense_group.apply(&dense_rows).unwrap();
    assert_eq!(typed_rows(aggregate.output()), typed_rows(&dense_output));

    aggregate.commit_group_state(&mut candidate_group);
    candidate_join.commit_source_updates(sources);
    assert_source_arrangements_equal(&candidate_join, &dense_join);
    assert_eq!(
        groups_vec(candidate_group.retained_groups()),
        groups_vec(dense_group.retained_groups())
    );
}

#[test]
fn flat_prepare_failure_after_aggregate_preparation_publishes_neither_consumer() {
    let left_schema = source_schema("l");
    let right_schema = source_schema("r");
    let k = i64::MAX / 2 + 1;
    let left_initial = source_batch(left_schema.clone(), &[(7, 1, Some(0))], &[k]);
    let right_initial = source_batch(right_schema.clone(), &[(7, 1, Some(0))], &[1]);
    let left_delta = source_batch(left_schema.clone(), &[(7, 1, Some(0))], &[-k]);
    let right_delta = source_batch(
        right_schema.clone(),
        &[(7, 1, Some(0)), (7, 1, Some(0))],
        &[1, 1],
    );
    let aggs = vec![AggSpec::count()];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![None],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![None],
    };
    let mut join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let initial = join.apply(&left_initial, &right_initial).unwrap();
    join.register_demand(compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap())
        .unwrap();
    let output_schema = join.cut_schema(&left_initial, &right_initial).unwrap();
    let mut group = crate::ops::GroupAggregate::new(&[0], aggs);
    group.apply(&initial).unwrap();
    assert!(
        group
            .initialize_maintained_cut(
                &mapping,
                join.left_for_cut().unwrap(),
                join.right_for_cut().unwrap(),
            )
            .unwrap()
    );
    let old_groups = groups_vec(group.retained_groups());
    let old_memo = group.maintained_memo().unwrap().clone();
    let old_left: Vec<_> = join.left_for_cut().unwrap().iter().collect();
    let old_right: Vec<_> = join.right_for_cut().unwrap().iter().collect();
    let old_left_stats: Vec<_> = join
        .left_for_cut()
        .unwrap()
        .iter_key_bytes()
        .map(|key| {
            (
                key.to_vec(),
                join.left_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone(),
            )
        })
        .collect();
    let old_right_stats: Vec<_> = join
        .right_for_cut()
        .unwrap()
        .iter_key_bytes()
        .map(|key| {
            (
                key.to_vec(),
                join.right_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone(),
            )
        })
        .collect();

    let sources = join
        .prepare_source_updates(&left_delta, &right_delta)
        .unwrap();
    let aggregate =
        crate::ops::join::factorized::certified_consume::prepare_grouped_update_with_sources(
            &mut join,
            &mut group,
            crate::ops::join::factorized::certified_consume::GroupedUpdate {
                mapping: &mapping,
                left_delta: &left_delta,
                right_delta: &right_delta,
                output_schema: output_schema.clone(),
                projection: None,
            },
            &sources,
        )
        .unwrap();
    let crate::ops::join::factorized::certified_consume::CertifiedDecision::NeedsRows(Some(
        _staged_memo,
    )) = aggregate
    else {
        panic!("large weighted COUNT update should prepare a staged Dense fallback");
    };
    let mut dense = crate::ops::Join::new(&[0, 1], &[0, 1]);
    dense.apply(&left_initial, &right_initial).unwrap();
    dense
        .register_demand(
            compile_demand(&mapping, &[AggSpec::count()], &left_schema, &right_schema).unwrap(),
        )
        .unwrap();
    let dense_error = match dense.apply(&left_delta, &right_delta) {
        Ok(_) => panic!("Dense must reject the per-term duplicate-weight overflow"),
        Err(error) => error,
    };
    super::super::FLAT_PAIR_PRODUCTS.with(|count| count.set(0));
    let flat_error = crate::ops::join::factorized::flat::materialize_flat_delta(
        &join,
        &sources,
        &left_delta,
        &right_delta,
        output_schema,
        1,
    )
    .err()
    .expect("flat term consolidation must fail after all its products are checked");
    assert_eq!(flat_error.to_string(), dense_error.to_string());
    assert!(matches!(
        flat_error,
        crate::error::EngineError::Infrastructure(ref message)
            if message == "consolidated diff sum overflowed i64"
    ));
    super::super::FLAT_PAIR_PRODUCTS.with(|count| assert_eq!(count.get(), 3));
    assert_eq!(
        join.left_for_cut().unwrap().iter().collect::<Vec<_>>(),
        old_left
    );
    assert_eq!(
        join.right_for_cut().unwrap().iter().collect::<Vec<_>>(),
        old_right
    );
    assert_eq!(
        groups_vec(group.retained_groups()),
        old_groups,
        "staged aggregate memo is not committed when flat preparation fails"
    );
    assert_eq!(group.maintained_memo().unwrap(), &old_memo);
    assert_eq!(
        join.left_for_cut()
            .unwrap()
            .iter_key_bytes()
            .map(|key| (
                key.to_vec(),
                join.left_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone()
            ))
            .collect::<Vec<_>>(),
        old_left_stats
    );
    assert_eq!(
        join.right_for_cut()
            .unwrap()
            .iter_key_bytes()
            .map(|key| (
                key.to_vec(),
                join.right_for_cut()
                    .unwrap()
                    .source_stats(key)
                    .unwrap()
                    .clone()
            ))
            .collect::<Vec<_>>(),
        old_right_stats
    );
}

#[test]
fn dense_fallback_publishes_complete_memo_before_next_certified_update() {
    let left_schema = source_schema("l");
    let right_schema = source_schema("r");
    let left_initial = source_batch(
        left_schema.clone(),
        &[(7, 1, Some(99)), (7, 2, Some(10)), (7, 3, Some(20))],
        &[-1, 1, 1],
    );
    let right_initial = source_batch(
        right_schema.clone(),
        &[(7, 2, Some(1)), (7, 3, Some(1))],
        &[1, 1],
    );
    let aggs = vec![AggSpec::sum(2), AggSpec::count()];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![Some(2), None],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64), None],
    };
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let candidate_initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    candidate_join
        .register_demand(compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap())
        .unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&candidate_initial).unwrap();
    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    let initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    dense_join
        .register_demand(compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap())
        .unwrap();
    dense_group.apply(&initial).unwrap();

    // Key 1 carries physical negative source support but no joined rows. The
    // first touched key forces Dense after keys 1, 2, and 3 all change.
    let fallback_delta = source_batch(
        left_schema.clone(),
        &[
            (7, 1, Some(99)),
            (7, 2, Some(10)),
            (7, 2, Some(20)),
            (7, 3, Some(20)),
            (7, 3, Some(10)),
        ],
        &[1, -1, 1, -1, 1],
    );
    let decision = prepare_grouped_update(
        &mut candidate_join,
        &mut candidate_group,
        &mapping,
        &fallback_delta,
        &ZSetBatch::empty(right_schema.clone()),
        output_schema.clone(),
    )
    .unwrap();
    let CertifiedDecision::NeedsRows(Some(stage)) = decision else {
        panic!("negative source support must select Dense with staged metadata");
    };
    let candidate_rows = candidate_join
        .apply(&fallback_delta, &ZSetBatch::empty(right_schema.clone()))
        .unwrap();
    let candidate_output = candidate_group.apply(&candidate_rows).unwrap();
    let dense_rows = dense_join
        .apply(&fallback_delta, &ZSetBatch::empty(right_schema.clone()))
        .unwrap();
    let dense_output = dense_group.apply(&dense_rows).unwrap();
    assert_eq!(candidate_output.schema(), dense_output.schema());
    assert_eq!(typed_rows(&candidate_output), typed_rows(&dense_output));
    candidate_group.commit_maintained_memo(stage);
    assert_eq!(candidate_group.maintained_memo().unwrap().len(), 2);
    assert_eq!(
        groups_vec(candidate_group.retained_groups()),
        groups_vec(dense_group.retained_groups())
    );
    assert_source_arrangements_equal(&candidate_join, &dense_join);

    // A later bounded update to key 2 must use the complete refreshed memo,
    // not stale contribution metrics from before the Dense fallback.
    let next_delta = source_batch(left_schema, &[(7, 2, Some(20)), (7, 2, Some(21))], &[-1, 1]);
    let next = prepare_grouped_update(
        &mut candidate_join,
        &mut candidate_group,
        &mapping,
        &next_delta,
        &ZSetBatch::empty(right_schema),
        output_schema,
    )
    .unwrap();
    let CertifiedDecision::Certified(next) = next else {
        panic!("memo after successful Dense fallback must certify the next safe update");
    };
    let dense_rows = dense_join
        .apply(
            &next_delta,
            &ZSetBatch::empty(candidate_join.right_for_cut().unwrap().schema().clone()),
        )
        .unwrap();
    let dense_output = dense_group.apply(&dense_rows).unwrap();
    assert_eq!(next.output().schema(), dense_output.schema());
    assert_eq!(typed_rows(next.output()), typed_rows(&dense_output));
    next.commit(&mut candidate_join, &mut candidate_group);
    assert_eq!(
        groups_vec(candidate_group.retained_groups()),
        groups_vec(dense_group.retained_groups())
    );
    assert_source_arrangements_equal(&candidate_join, &dense_join);
}
