//! Aggregate translation: `count`/`sum`/`avg`/`min`/`max` accepted, the rest
//! rejected.

use hotlap::{AggFunc, AggSpec, Plan};

use super::{ctx_with_src, plan_for};

#[tokio::test]
async fn rejects_unsupported_aggregate_modifiers() {
    let ctx = ctx_with_src();
    assert!(
        plan_for(&ctx, "SELECT k, count(DISTINCT k) FROM src GROUP BY k")
            .await
            .is_err(),
        "DISTINCT would be silently dropped"
    );
}

#[tokio::test]
async fn translates_group_count_without_tumble() {
    let ctx = ctx_with_src();
    let plan = plan_for(&ctx, "SELECT k, count(*) FROM src GROUP BY k")
        .await
        .expect("grouped count must remain supported");
    match plan {
        Plan::GroupAggregate { key, aggs, .. } => {
            assert_eq!(key, vec![0]);
            assert_eq!(aggs, vec![AggSpec::count()]);
        }
        other => panic!("expected GroupAggregate, got {other:?}"),
    }
}

#[tokio::test]
async fn translates_sum_and_avg() {
    let ctx = ctx_with_src();
    let plan = plan_for(&ctx, "SELECT k, sum(ts), avg(ts) FROM src GROUP BY k")
        .await
        .unwrap();
    match plan {
        Plan::GroupAggregate { key, aggs, .. } => {
            assert_eq!(key, vec![0]);
            assert_eq!(
                aggs,
                vec![
                    AggSpec {
                        func: AggFunc::Sum,
                        input: Some(1),
                    },
                    AggSpec {
                        func: AggFunc::Avg,
                        input: Some(1),
                    },
                ]
            );
        }
        other => panic!("expected GroupAggregate, got {other:?}"),
    }
}

#[tokio::test]
async fn translates_count_of_column() {
    let ctx = ctx_with_src();
    let plan = plan_for(&ctx, "SELECT k, count(ts) FROM src GROUP BY k")
        .await
        .unwrap();
    match plan {
        Plan::GroupAggregate { aggs, .. } => {
            assert_eq!(
                aggs,
                vec![AggSpec {
                    func: AggFunc::Count,
                    input: Some(1),
                }]
            );
        }
        other => panic!("expected GroupAggregate, got {other:?}"),
    }
}

#[tokio::test]
async fn translates_min_and_max() {
    let ctx = ctx_with_src();
    let plan = plan_for(&ctx, "SELECT k, min(ts), max(ts) FROM src GROUP BY k")
        .await
        .unwrap();
    match plan {
        Plan::GroupAggregate { aggs, .. } => {
            assert_eq!(aggs, vec![AggSpec::min(1), AggSpec::max(1)]);
        }
        other => panic!("expected GroupAggregate, got {other:?}"),
    }
}

#[tokio::test]
async fn reconciles_float_aggregate_output_types() {
    // `sum`/`avg` over Float64 resolve to Float64 and `min`/`max` keep the
    // input type; the reconciliation guard compares each to DataFusion's
    // resolved `Aggregate.schema` and must accept them.
    let ctx = ctx_with_src();
    let plan = plan_for(
        &ctx,
        "SELECT k, sum(f), avg(f), min(f), max(f) FROM src GROUP BY k",
    )
    .await
    .expect("Float64 aggregates must reconcile with DataFusion");
    match plan {
        Plan::GroupAggregate { aggs, .. } => assert_eq!(
            aggs,
            vec![
                AggSpec::sum(2),
                AggSpec::avg(2),
                AggSpec::min(2),
                AggSpec::max(2)
            ]
        ),
        other => panic!("expected GroupAggregate, got {other:?}"),
    }
}
