use super::certified_consume_support::*;
use super::*;

#[test]
fn valid_avg_stays_on_dense_path_without_changing_dense_output() {
    let left_schema = rich_left_schema();
    let right_schema = rich_right_schema();
    let left_initial = rich_left(
        left_schema.clone(),
        &[(Some(2), Some(1), Some(4), Some("x"))],
        &[1],
    );
    let right_initial = rich_right(
        right_schema.clone(),
        &[(Some(2), Some(1), Some(8), Some(true))],
        &[1],
    );
    let aggs = vec![AggSpec::avg(6)];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![Some(6)],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
    };
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&initial).unwrap();
    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    dense_group.apply(&dense_initial).unwrap();

    let left_delta = rich_left(
        left_schema,
        &[
            (Some(2), Some(1), Some(4), Some("x")),
            (Some(2), Some(1), Some(5), Some("y")),
        ],
        &[-1, 1],
    );
    let right_delta = ZSetBatch::empty(right_schema);
    let decision = prepare_grouped_update(
        &mut candidate_join,
        &mut candidate_group,
        &mapping,
        &left_delta,
        &right_delta,
        output_schema,
    )
    .unwrap();
    assert!(matches!(decision, CertifiedDecision::NeedsRows(None)));
    let candidate_rows = candidate_join.apply(&left_delta, &right_delta).unwrap();
    let candidate_output = candidate_group.apply(&candidate_rows).unwrap();
    let dense_rows = dense_join.apply(&left_delta, &right_delta).unwrap();
    let dense_output = dense_group.apply(&dense_rows).unwrap();
    assert_eq!(candidate_output.schema(), dense_output.schema());
    assert_eq!(typed_rows(&candidate_output), typed_rows(&dense_output));
    assert_eq!(
        groups_vec(candidate_group.retained_groups()),
        groups_vec(dense_group.retained_groups())
    );
}

#[test]
fn certified_render_overflow_falls_back_without_publishing_any_cut_state() {
    let left_schema = source_schema("l");
    let right_schema = source_schema("r");
    let left_initial = source_batch(left_schema.clone(), &[(1, 1, Some(i64::MAX - 1))], &[1]);
    let right_initial = source_batch(right_schema.clone(), &[(1, 1, Some(1))], &[1]);
    let aggs = vec![AggSpec::sum(2)];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![Some(2)],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Int64)],
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
    candidate_group
        .initialize_maintained_cut(
            &mapping,
            candidate_join.left_for_cut().unwrap(),
            candidate_join.right_for_cut().unwrap(),
        )
        .unwrap();
    let before_groups = groups_vec(candidate_group.retained_groups());
    let before_memo = candidate_group.maintained_memo().unwrap().clone();
    let before_left = candidate_join
        .left_for_cut()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    let delta = source_batch(left_schema, &[(1, 1, Some(2))], &[1]);
    let decision = prepare_grouped_update(
        &mut candidate_join,
        &mut candidate_group,
        &mapping,
        &delta,
        &ZSetBatch::empty(right_schema.clone()),
        output_schema,
    )
    .unwrap();
    assert!(matches!(decision, CertifiedDecision::NeedsRows(_)));
    assert_eq!(groups_vec(candidate_group.retained_groups()), before_groups);
    assert_eq!(candidate_group.maintained_memo().unwrap(), &before_memo);
    let after_left = candidate_join
        .left_for_cut()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(after_left, before_left);

    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs);
    dense_group.apply(&dense_initial).unwrap();
    let joined = dense_join
        .apply(&delta, &ZSetBatch::empty(right_schema.clone()))
        .unwrap();
    let dense_error = match dense_group.apply(&joined) {
        Ok(_) => panic!("Dense rendering should overflow the Int64 result"),
        Err(error) => error,
    };
    assert!(
        matches!(
            &dense_error,
            crate::error::EngineError::Infrastructure(message)
                if message == "group sum out of range for Int64"
        ),
        "unexpected Dense error: {dense_error:?}"
    );
    let candidate_rows = candidate_join
        .apply(&delta, &ZSetBatch::empty(right_schema))
        .unwrap();
    let candidate_error = match candidate_group.apply(&candidate_rows) {
        Ok(_) => panic!("ordinary fallback must retain the Dense render error"),
        Err(error) => error,
    };
    assert_eq!(format!("{candidate_error:?}"), format!("{dense_error:?}"));
}

#[test]
fn populated_float_sum_is_classified_dense_before_memo_initialization() {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let right_schema = schema(&[("group", DataType::Int64), ("id", DataType::Int64)]);
    let left_batch = |rows: &[(i64, i64, Option<f64>)], diffs: &[i64]| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(arrow::array::Float64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
            diffs,
        )
    };
    let right_batch = |rows: &[(i64, i64)], diffs: &[i64]| {
        zset(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
            diffs,
        )
    };
    let left_initial = left_batch(&[(7, 1, Some(1.25)), (7, 2, Some(2.5))], &[1, 1]);
    let right_initial = right_batch(&[(7, 1), (7, 2)], &[1, 1]);
    let aggs = vec![AggSpec::sum(2)];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![Some(2)],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Float64)],
    };
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let candidate_initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&candidate_initial).unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs);
    dense_group.apply(&dense_initial).unwrap();

    let left_delta = left_batch(&[(7, 1, Some(1.5))], &[1]);
    let right_delta = ZSetBatch::empty(right_schema.clone());
    let decision = prepare_grouped_update(
        &mut candidate_join,
        &mut candidate_group,
        &mapping,
        &left_delta,
        &right_delta,
        output_schema,
    )
    .unwrap();
    assert!(matches!(decision, CertifiedDecision::NeedsRows(None)));
    assert!(candidate_group.maintained_memo().is_none());
    let candidate_rows = candidate_join.apply(&left_delta, &right_delta).unwrap();
    let candidate_output = candidate_group.apply(&candidate_rows).unwrap();
    let dense_rows = dense_join.apply(&left_delta, &right_delta).unwrap();
    let dense_output = dense_group.apply(&dense_rows).unwrap();
    assert_eq!(candidate_output.schema(), dense_output.schema());
    assert_eq!(typed_rows(&candidate_output), typed_rows(&dense_output));
    assert_eq!(
        groups_vec(candidate_group.retained_groups()),
        groups_vec(dense_group.retained_groups())
    );
}

#[test]
fn float_nullable_count_remains_certified_without_a_sum_stat() {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let right_schema = schema(&[("group", DataType::Int64), ("id", DataType::Int64)]);
    let left_batch = |value: f64| {
        zset(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(arrow::array::Float64Array::from(vec![Some(value)])),
            ],
            &[1],
        )
    };
    let right_initial = zset(
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![1])),
        ],
        &[1],
    );
    let aggs = vec![AggSpec {
        func: hotlap_core::plan::AggFunc::Count,
        input: Some(2),
    }];
    let mapping = ColumnMapping {
        left_source: InputId(0),
        right_source: InputId(1),
        left_join_key: vec![0, 1],
        right_join_key: vec![0, 1],
        group_key: vec![0],
        aggregate_inputs: vec![Some(2)],
        join_key_types: vec![DataType::Int64, DataType::Int64],
        aggregate_input_types: vec![Some(DataType::Float64)],
    };
    let left_initial = left_batch(1.25);
    let mut candidate_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let candidate_initial = candidate_join.apply(&left_initial, &right_initial).unwrap();
    let output_schema = candidate_join
        .cut_schema(&left_initial, &right_initial)
        .unwrap();
    let mut candidate_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    candidate_group.apply(&candidate_initial).unwrap();
    let mut dense_join = crate::ops::Join::new(&[0, 1], &[0, 1]);
    let dense_initial = dense_join.apply(&left_initial, &right_initial).unwrap();
    let mut dense_group = crate::ops::GroupAggregate::new(&[0], aggs.clone());
    dense_group.apply(&dense_initial).unwrap();
    dense_join
        .register_demand(compile_demand(&mapping, &aggs, &left_schema, &right_schema).unwrap())
        .unwrap();

    let left_delta = left_batch(2.5);
    let right_delta = ZSetBatch::empty(right_schema);
    let decision = prepare_grouped_update(
        &mut candidate_join,
        &mut candidate_group,
        &mapping,
        &left_delta,
        &right_delta,
        output_schema,
    )
    .unwrap();
    let CertifiedDecision::Certified(prepared) = decision else {
        panic!("COUNT(nullable Float64) only needs a non-null summary, not a SUM stat");
    };
    let dense_rows = dense_join.apply(&left_delta, &right_delta).unwrap();
    let dense_output = dense_group.apply(&dense_rows).unwrap();
    assert_eq!(prepared.output().schema(), dense_output.schema());
    assert_eq!(typed_rows(prepared.output()), typed_rows(&dense_output));
    prepared.commit(&mut candidate_join, &mut candidate_group);
    assert_eq!(
        groups_vec(candidate_group.retained_groups()),
        groups_vec(dense_group.retained_groups())
    );
    assert_source_arrangements_equal(&candidate_join, &dense_join);
}
