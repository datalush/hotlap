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
    column_index, ensure_count_only, ensure_identity_projection, is_tumble, parse_tumble,
    predicate, projection_indices,
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
/// kernel normalizes aggregate output itself. Unwrap that wrapper only when it
/// is an identity selection, so lossy transforms are rejected rather than
/// silently dropped.
fn project(p: &Projection, source: InputId) -> Result<Plan, SqlError> {
    if let LogicalPlan::Aggregate(a) = p.input.as_ref() {
        ensure_identity_projection(&p.expr, &a.schema)?;
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
    let left_tables = base_tables(&j.left)?;
    if left_tables.is_empty() || left_tables != base_tables(&j.right)? {
        return Err(SqlError::Unsupported(
            "both join sides must read the same single source".into(),
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

/// Collect the base-table names under `plan`, rejecting non-scan leaves. The
/// current `to_kernel_plan` interface has a single `InputId`, so both join
/// sides must be the same table; otherwise two tables would collapse silently.
fn base_tables(plan: &LogicalPlan) -> Result<Vec<String>, SqlError> {
    match plan {
        LogicalPlan::TableScan(t) => Ok(vec![t.table_name.to_string()]),
        LogicalPlan::Projection(p) => base_tables(&p.input),
        LogicalPlan::Filter(f) => base_tables(&f.input),
        other => Err(SqlError::Unsupported(format!(
            "unsupported join input: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests;
