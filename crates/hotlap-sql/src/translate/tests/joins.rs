//! Join translation: each side resolves to its own bound source.

use hotlap::{InputId, Plan};

use super::{ctx_with_src, register_empty_table};
use crate::bindings::SourceBindings;
use crate::translate::to_kernel_plan;

/// Two distinct relations, each bound to its own input.
fn two_bindings() -> SourceBindings {
    SourceBindings::from([("src".into(), InputId(7)), ("other".into(), InputId(9))])
}

#[tokio::test]
async fn resolves_distinct_join_inputs() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    let bindings = two_bindings();
    let df = ctx
        .sql("SELECT a.k FROM src a JOIN other b ON a.k=b.k")
        .await
        .unwrap();
    let plan = to_kernel_plan(df.logical_plan(), &bindings).unwrap();
    let Plan::Project { input, .. } = plan else {
        panic!("expected projection")
    };
    let Plan::Join {
        left,
        right,
        left_key,
        right_key,
    } = *input
    else {
        panic!("expected join")
    };
    assert!(matches!(*left, Plan::Source(InputId(7))));
    assert!(matches!(*right, Plan::Source(InputId(9))));
    assert_eq!((left_key, right_key), (vec![0], vec![0]));
}

#[tokio::test]
async fn resolves_two_aliases_of_one_source_to_the_same_input() {
    let ctx = ctx_with_src();
    let bindings = SourceBindings::from([("src".into(), InputId(7))]);
    let df = ctx
        .sql("SELECT a.k FROM src a JOIN src b ON a.k=b.k")
        .await
        .unwrap();
    let plan = to_kernel_plan(df.logical_plan(), &bindings).unwrap();
    let Plan::Project { input, .. } = plan else {
        panic!("expected projection")
    };
    let Plan::Join { left, right, .. } = *input else {
        panic!("expected join")
    };
    assert!(matches!(*left, Plan::Source(InputId(7))));
    assert!(matches!(*right, Plan::Source(InputId(7))));
}

#[tokio::test]
async fn resolves_composite_join_key() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    let bindings = two_bindings();
    let df = ctx
        .sql("SELECT a.k FROM src a JOIN other b ON a.k=b.k AND a.ts=b.ts")
        .await
        .unwrap();
    let plan = to_kernel_plan(df.logical_plan(), &bindings).unwrap();
    let Plan::Project { input, .. } = plan else {
        panic!("expected projection")
    };
    let Plan::Join {
        left_key,
        right_key,
        ..
    } = *input
    else {
        panic!("expected join")
    };
    assert_eq!((left_key, right_key), (vec![0, 1], vec![0, 1]));
}

#[tokio::test]
async fn rejects_ambiguous_unqualified_column() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    // Both relations expose `k`; DataFusion must refuse the bare reference
    // rather than picking one side silently.
    assert!(
        ctx.sql("SELECT k FROM src a JOIN other b ON a.k=b.k")
            .await
            .is_err(),
        "unqualified `k` is ambiguous across both joins"
    );
}

#[tokio::test]
async fn rejects_relation_without_binding() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    let bindings = SourceBindings::from([("src".into(), InputId(7))]);
    let df = ctx
        .sql("SELECT a.k FROM src a JOIN other b ON a.k=b.k")
        .await
        .unwrap();
    assert!(
        to_kernel_plan(df.logical_plan(), &bindings).is_err(),
        "an unbound relation must be rejected, not defaulted"
    );
}

#[tokio::test]
async fn rejects_join_over_more_than_two_inputs() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    register_empty_table(&ctx, "third");
    let bindings = SourceBindings::from([
        ("src".into(), InputId(7)),
        ("other".into(), InputId(9)),
        ("third".into(), InputId(11)),
    ]);
    let df = ctx
        .sql("SELECT a.k FROM src a JOIN other b ON a.k=b.k JOIN third c ON a.k=c.k")
        .await
        .unwrap();
    assert!(
        to_kernel_plan(df.logical_plan(), &bindings).is_err(),
        "three distinct inputs are outside this slice"
    );
}

#[tokio::test]
async fn rejects_residual_join_filter() {
    let ctx = ctx_with_src();
    register_empty_table(&ctx, "other");
    let bindings = two_bindings();
    let df = ctx
        .sql("SELECT a.k FROM src a JOIN other b ON a.k=b.k AND a.ts > 0")
        .await
        .unwrap();
    assert!(
        to_kernel_plan(df.logical_plan(), &bindings).is_err(),
        "a residual ON predicate must not be dropped"
    );
}
