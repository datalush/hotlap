//! Translation tests: SQL `WHERE` predicates become the expected kernel IR.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use hotlap::{CmpOp, InputId, Plan, Predicate, Scalar};
use hotlap_sql::translate::to_kernel_plan;

/// A nullable table with one column per supported scalar type.
fn ctx() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("i", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::new_empty(schema.clone());
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("src", Arc::new(mem)).unwrap();
    ctx
}

async fn predicate_for(
    ctx: &SessionContext,
    where_sql: &str,
) -> Result<Predicate, hotlap_sql::SqlError> {
    let df = ctx
        .sql(&format!("SELECT * FROM src WHERE {where_sql}"))
        .await
        .unwrap();
    let plan = to_kernel_plan(df.logical_plan(), InputId(0))?;
    Ok(kernel_predicate(&plan).clone())
}

fn kernel_predicate(plan: &Plan) -> &Predicate {
    match plan {
        Plan::Filter { pred, .. } => pred,
        Plan::Project { input, .. } => kernel_predicate(input),
        other => panic!("expected a filter under the projection, got {other:?}"),
    }
}

#[tokio::test]
async fn translates_every_comparison_operator() {
    let ctx = ctx();
    let cases = [
        ("i < 2", CmpOp::Lt),
        ("i <= 2", CmpOp::Le),
        ("i > 2", CmpOp::Gt),
        ("i >= 2", CmpOp::Ge),
        ("i = 2", CmpOp::Eq),
        ("i <> 2", CmpOp::Ne),
    ];
    for (sql, op) in cases {
        let pred = predicate_for(&ctx, sql).await.unwrap();
        assert!(
            matches!(pred, Predicate::Cmp { op: got, col: 1, .. } if got == op),
            "`{sql}` translated to {pred:?}"
        );
    }
}

#[tokio::test]
async fn translates_int32_column_and_float64_literal() {
    let ctx = ctx();
    // The Int32 column is accepted; DataFusion keeps `3` as an Int64 literal,
    // which the kernel casts to the column type at evaluation time.
    let int = predicate_for(&ctx, "i = 3").await.unwrap();
    assert!(
        matches!(
            int,
            Predicate::Cmp {
                op: CmpOp::Eq,
                col: 1,
                ..
            }
        ),
        "got {int:?}"
    );
    let float = predicate_for(&ctx, "f = 1.5").await.unwrap();
    assert_eq!(
        float,
        Predicate::Cmp {
            op: CmpOp::Eq,
            col: 2,
            scalar: Scalar::F64(1.5),
        }
    );
}

#[tokio::test]
async fn flips_literal_on_the_left() {
    // `2 >= i` means `i <= 2`; the column must be on the left of the kernel op.
    let ctx = ctx();
    let pred = predicate_for(&ctx, "2 >= i").await.unwrap();
    assert!(
        matches!(
            pred,
            Predicate::Cmp {
                op: CmpOp::Le,
                col: 1,
                ..
            }
        ),
        "got {pred:?}"
    );
}

#[tokio::test]
async fn translates_boolean_combinators() {
    let ctx = ctx();
    assert!(matches!(
        predicate_for(&ctx, "k > 0 AND i < 2").await.unwrap(),
        Predicate::And(..)
    ));
    assert!(matches!(
        predicate_for(&ctx, "k > 0 OR i < 2").await.unwrap(),
        Predicate::Or(..)
    ));
    assert!(matches!(
        predicate_for(&ctx, "NOT (k > 0)").await.unwrap(),
        Predicate::Not(_)
    ));
}

#[tokio::test]
async fn translates_null_checks() {
    let ctx = ctx();
    assert!(matches!(
        predicate_for(&ctx, "k IS NULL").await.unwrap(),
        Predicate::IsNull(0)
    ));
    assert!(matches!(
        predicate_for(&ctx, "k IS NOT NULL").await.unwrap(),
        Predicate::IsNotNull(0)
    ));
}

#[tokio::test]
async fn rejects_unsupported_predicates() {
    let ctx = ctx();
    for sql in ["k + 1 > 2", "abs(k) > 0"] {
        assert!(
            predicate_for(&ctx, sql).await.is_err(),
            "`{sql}` must be rejected, not silently mistranslated"
        );
    }
}
