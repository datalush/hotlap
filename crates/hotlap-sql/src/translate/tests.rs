//! Tests for `translate::to_kernel_plan`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use hotlap::{InputId, Plan};

use super::{register_tumble_udf, to_kernel_plan};
use crate::bindings::SourceBindings;

mod aggregates;
mod join_keys;
mod joins;

fn register_empty_table(ctx: &SessionContext, name: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("f", DataType::Float64, false),
    ]));
    let batch = RecordBatch::new_empty(schema.clone());
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table(name, Arc::new(mem)).unwrap();
}

fn ctx_with_src() -> SessionContext {
    let ctx = SessionContext::new();
    register_tumble_udf(&ctx);
    register_empty_table(&ctx, "src");
    ctx
}

/// The default single-source binding used by the non-join tests.
fn src_bindings() -> SourceBindings {
    SourceBindings::from([("src".into(), InputId(0))])
}

async fn plan_for(ctx: &SessionContext, sql: &str) -> Result<Plan, crate::SqlError> {
    let df = ctx.sql(sql).await.unwrap();
    to_kernel_plan(df.logical_plan(), &src_bindings())
}

#[tokio::test]
async fn translates_tumble_count() {
    let ctx = ctx_with_src();
    let plan = plan_for(
        &ctx,
        "SELECT k, count(*) FROM src GROUP BY k, tumble(ts, 10000)",
    )
    .await
    .unwrap();
    match plan {
        Plan::TumbleCount {
            key,
            time_col,
            size,
            ..
        } => {
            assert_eq!(key, vec![0]);
            assert_eq!(time_col, 1);
            assert_eq!(size, 10000);
        }
        other => panic!("expected TumbleCount, got {other:?}"),
    }
}

#[tokio::test]
async fn rejects_non_identity_projection() {
    let ctx = ctx_with_src();
    assert!(
        plan_for(
            &ctx,
            "SELECT k + 1, count(*) FROM src GROUP BY k, tumble(ts, 10000)",
        )
        .await
        .is_err(),
        "a projection over an aggregate must be identity to unwrap"
    );
}

#[tokio::test]
async fn rejects_subset_projection_over_aggregate() {
    let ctx = ctx_with_src();
    let df = ctx
        .sql("SELECT count(*) FROM src GROUP BY k")
        .await
        .unwrap();
    match df.logical_plan() {
        LogicalPlan::Projection(projection) => {
            assert_eq!(projection.expr.len(), 1);
            assert_eq!(projection.input.schema().fields().len(), 2);
        }
        other => panic!("expected projected aggregate, got {other:?}"),
    }
    let err = plan_for(&ctx, "SELECT count(*) FROM src GROUP BY k")
        .await
        .expect_err("an aggregate projection must retain every output column");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rejects_reordered_projection_over_aggregate() {
    let ctx = ctx_with_src();
    let err = plan_for(&ctx, "SELECT ts, k, count(*) FROM src GROUP BY k, ts")
        .await
        .expect_err("an aggregate projection must retain output order");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rejects_duplicate_projection_over_aggregate() {
    let ctx = ctx_with_src();
    let err = plan_for(
        &ctx,
        "SELECT k, k AS duplicate, count(*) FROM src GROUP BY k, ts",
    )
    .await
    .expect_err("aggregate projection must not duplicate output columns");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rejects_cast_projection_over_aggregate() {
    let ctx = ctx_with_src();
    let err = plan_for(
        &ctx,
        "SELECT CAST(k AS BIGINT), count(*) FROM src GROUP BY k",
    )
    .await
    .expect_err("aggregate projection must not transform output columns");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn accepts_aliases_in_normalized_window_output_order() {
    let ctx = ctx_with_src();
    let plan = plan_for(
        &ctx,
        "SELECT k AS group_key, count(*) FROM src GROUP BY k, tumble(ts, 10000)",
    )
    .await
    .expect("aliases of all normalized window output columns must be accepted");
    assert!(matches!(plan, Plan::TumbleCount { key, .. } if key == vec![0]));
}

#[tokio::test]
async fn accepts_aggregate_alias_in_normalized_output_order() {
    let ctx = ctx_with_src();
    let plan = plan_for(&ctx, "SELECT k, count(*) AS n FROM src GROUP BY k")
        .await
        .expect("an alias must not change aggregate output identity");
    assert!(matches!(plan, Plan::GroupAggregate { key, .. } if key == vec![0]));
}

#[tokio::test]
async fn accepts_window_before_multiple_keys_in_grouping_order() {
    let ctx = ctx_with_src();
    let plan = plan_for(
        &ctx,
        "SELECT k, f, count(*) FROM src GROUP BY tumble(ts, 10000), k, f",
    )
    .await
    .expect("tumble position must not change normalized key/count output order");
    assert!(matches!(plan, Plan::TumbleCount { key, .. } if key == vec![0, 2]));
}

#[tokio::test]
async fn rejects_reordered_window_projection() {
    let ctx = ctx_with_src();
    let err = plan_for(
        &ctx,
        "SELECT count(*) AS n, k FROM src GROUP BY k, tumble(ts, 10000)",
    )
    .await
    .expect_err("window projection must follow normalized key/count output order");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rejects_window_projection_that_includes_window_expression() {
    let ctx = ctx_with_src();
    let err = plan_for(
        &ctx,
        "SELECT k, tumble(ts, 10000), count(*) FROM src GROUP BY k, tumble(ts, 10000)",
    )
    .await
    .expect_err("raw tumble expression is not a normalized output column");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rejects_global_count_without_group_key() {
    let ctx = ctx_with_src();
    let err = plan_for(&ctx, "SELECT count(*) FROM src")
        .await
        .expect_err("global count(*) has no representable group key");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "must be a planning error, got {err:?}"
    );
}

#[tokio::test]
async fn rejects_tumble_count_without_group_key() {
    let ctx = ctx_with_src();
    let err = plan_for(&ctx, "SELECT count(*) FROM src GROUP BY tumble(ts, 10000)")
        .await
        .expect_err("tumble without a column key has no representable group key");
    assert!(
        matches!(err, crate::SqlError::Unsupported(_)),
        "must be a planning error, got {err:?}"
    );
}

#[tokio::test]
async fn rejects_count_filter_clause() {
    let ctx = ctx_with_src();
    assert!(
        plan_for(
            &ctx,
            "SELECT count(*) FILTER (WHERE k > 0) FROM src GROUP BY k, tumble(ts, 10000)",
        )
        .await
        .is_err(),
        "a FILTER clause would be silently dropped"
    );
}
