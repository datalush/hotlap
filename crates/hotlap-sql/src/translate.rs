//! Translate a DataFusion `LogicalPlan` into the hotlap kernel `Plan` IR.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::JoinType;
use datafusion::logical_expr::{
    Aggregate, Expr, Join, LogicalPlan, Projection, Volatility, create_udf,
};
use datafusion::prelude::SessionContext;
use hotlap::{InputId, Plan};

use crate::error::SqlError;
use crate::translate_expr::{
    column_index, ensure_count_only, is_tumble, parse_tumble, predicate, projection_indices,
};

/// Register the planning-only `tumble(ts, size)` scalar function.
pub fn register_tumble_udf(ctx: &SessionContext) {
    ctx.register_udf(create_udf(
        "tumble",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| Ok(args[0].clone())),
    ));
}

/// Translate a supported `SELECT` plan into the kernel IR.
pub fn to_kernel_plan(plan: &LogicalPlan, source: InputId) -> Result<Plan, SqlError> {
    match plan {
        LogicalPlan::Projection(p) => project(p, source),
        LogicalPlan::Filter(f) => {
            let input = to_kernel_plan(&f.input, source)?;
            let pred = predicate(&f.predicate, f.input.schema())?;
            Ok(Plan::Filter {
                input: Box::new(input),
                pred,
            })
        }
        LogicalPlan::Aggregate(a) => translate_aggregate(a, source),
        LogicalPlan::TableScan(_) => Ok(Plan::Source(source)),
        LogicalPlan::Join(j) => translate_join(j, source),
        other => Err(SqlError::Unsupported(format!(
            "unsupported logical plan node: {other:?}"
        ))),
    }
}

/// DataFusion wraps every `SELECT` over an aggregate in a projection; the
/// kernel normalizes aggregate output itself, so that wrapper is unwrapped.
fn project(p: &Projection, source: InputId) -> Result<Plan, SqlError> {
    if matches!(p.input.as_ref(), LogicalPlan::Aggregate(_)) {
        return to_kernel_plan(&p.input, source);
    }
    let input = to_kernel_plan(&p.input, source)?;
    let cols = projection_indices(&p.expr, p.input.schema())?;
    Ok(Plan::Project {
        input: Box::new(input),
        cols,
    })
}

fn translate_aggregate(a: &Aggregate, source: InputId) -> Result<Plan, SqlError> {
    ensure_count_only(&a.aggr_expr)?;
    let input = to_kernel_plan(&a.input, source)?;
    let schema = a.input.schema();
    let mut key = Vec::new();
    let mut tumble = None;
    for expr in &a.group_expr {
        match expr {
            Expr::Column(_) => key.push(column_index(expr, schema)?),
            Expr::ScalarFunction(sf) if is_tumble(sf) => {
                if tumble.is_some() {
                    return Err(SqlError::Unsupported("multiple `tumble` calls".into()));
                }
                tumble = Some(parse_tumble(sf, schema)?);
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "unsupported GROUP BY expression `{other}`"
                )));
            }
        }
    }
    match tumble {
        Some((time_col, size)) => Ok(Plan::TumbleCount {
            input: Box::new(input),
            key,
            time_col,
            size,
        }),
        None => Ok(Plan::GroupCount {
            input: Box::new(input),
            key,
        }),
    }
}

fn translate_join(j: &Join, source: InputId) -> Result<Plan, SqlError> {
    if j.join_type != JoinType::Inner || j.filter.is_some() {
        return Err(SqlError::Unsupported(
            "only inner equi-joins are supported".into(),
        ));
    }
    let left = to_kernel_plan(&j.left, source)?;
    let right = to_kernel_plan(&j.right, source)?;
    let mut left_key = Vec::new();
    let mut right_key = Vec::new();
    for (l, r) in &j.on {
        left_key.push(column_index(l, j.left.schema())?);
        right_key.push(column_index(r, j.right.schema())?);
    }
    Ok(Plan::Join {
        left: Box::new(left),
        right: Box::new(right),
        left_key,
        right_key,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemTable;
    use datafusion::prelude::SessionContext;
    use hotlap::{InputId, Plan};

    use super::*;

    fn register_empty_table(ctx: &SessionContext, name: &str, schema: Arc<Schema>) {
        let batch = RecordBatch::new_empty(schema.clone());
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(name, Arc::new(mem)).unwrap();
    }

    fn ctx_with_src() -> SessionContext {
        let ctx = SessionContext::new();
        register_tumble_udf(&ctx);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        register_empty_table(&ctx, "src", schema);
        ctx
    }

    #[tokio::test]
    async fn translates_tumble_count() {
        let ctx = ctx_with_src();
        let df = ctx
            .sql("SELECT k, count(*) FROM src GROUP BY k, tumble(ts, 10000)")
            .await
            .unwrap();
        let plan = to_kernel_plan(df.logical_plan(), InputId(0)).unwrap();
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
    async fn rejects_unsupported() {
        let ctx = ctx_with_src();
        let df = ctx.sql("SELECT sum(k) FROM src").await.unwrap();
        assert!(to_kernel_plan(df.logical_plan(), InputId(0)).is_err());
    }
}
