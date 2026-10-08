//! Tests for `translate::to_kernel_plan`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use hotlap::{InputId, Plan};

use super::{register_tumble_udf, to_kernel_plan};

mod aggregates;

fn register_empty_table(ctx: &SessionContext, name: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
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

async fn plan_for(ctx: &SessionContext, sql: &str) -> Result<Plan, crate::SqlError> {
    let df = ctx.sql(sql).await.unwrap();
    to_kernel_plan(df.logical_plan(), InputId(0))
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

#[tokio::test]
async fn rejects_join_of_distinct_sources() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    assert!(
        plan_for(&ctx, "SELECT a.k FROM src a JOIN other b ON a.k = b.k")
            .await
            .is_err(),
        "single-source interface must reject a two-table join"
    );
}
