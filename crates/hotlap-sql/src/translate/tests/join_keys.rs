//! Join-key extraction from plans DataFusion builds directly, plus relation
//! name canonicalization against the plan's reported table name.

use std::sync::Arc;

use datafusion::common::{Column, JoinConstraint, JoinType, NullEquality};
use datafusion::logical_expr::{Expr, Join as LogicalJoin, LogicalPlan};
use datafusion::prelude::{SessionContext, lit};
use hotlap::InputId;

use super::{ctx_with_src, register_empty_table, register_tumble_udf};
use crate::bindings::SourceBindings;
use crate::translate::to_kernel_plan;

fn qualified(relation: &str, name: &str) -> Expr {
    Expr::Column(Column::new(Some(relation), name))
}

#[tokio::test]
async fn rejects_residual_filter_when_on_is_nonempty() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    let left = ctx.sql("SELECT * FROM src a").await.unwrap();
    let right = ctx.sql("SELECT * FROM other b").await.unwrap();
    let on = vec![(qualified("a", "k"), qualified("b", "k"))];
    let filter = qualified("a", "ts").gt(lit(0i64));
    // DataFusion normally folds `ON` into `filter`; a plan may still carry
    // equi-pairs in `on` together with a residual filter, which must not be
    // ignored just because `on` is populated.
    let join = LogicalJoin::try_new(
        Arc::new(left.logical_plan().clone()),
        Arc::new(right.logical_plan().clone()),
        on,
        Some(filter),
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap();
    let plan = LogicalPlan::Join(join);
    let bindings = SourceBindings::from([("src".into(), InputId(7)), ("other".into(), InputId(9))]);
    assert!(
        to_kernel_plan(&plan, &bindings).is_err(),
        "a residual filter must be rejected even when `on` is non-empty"
    );
}

#[tokio::test]
async fn mixed_case_unquoted_relation_matches_its_binding() {
    let ctx = SessionContext::new();
    register_tumble_udf(&ctx);
    register_empty_table(&ctx, "Src");
    let df = ctx
        .sql("SELECT k FROM Src")
        .await
        .expect("DataFusion resolves a registered relation by its unquoted reference");
    // The DDL name is canonicalized exactly as DataFusion resolves it.
    let name = crate::bindings::canonical_relation("Src");
    let bindings = SourceBindings::from([(name, InputId(7))]);
    assert!(
        to_kernel_plan(df.logical_plan(), &bindings).is_ok(),
        "the canonical DDL name must match the plan's relation name"
    );
}
