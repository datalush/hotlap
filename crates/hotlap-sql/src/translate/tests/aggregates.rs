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
