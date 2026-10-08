//! Translate a DataFusion `LogicalPlan` into the hotlap kernel `Plan` IR.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan, Projection, Volatility, create_udf};
use datafusion::prelude::SessionContext;
use hotlap::{AggSpec, Plan, aggregate_output_type};

use crate::bindings::SourceBindings;
use crate::error::SqlError;
use crate::translate_expr::{
    column_index, ensure_identity_projection, is_tumble, parse_aggs, parse_tumble,
    projection_indices,
};
use crate::translate_predicate::predicate;

mod join;

use join::translate_join;

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
///
/// Every relation is resolved through `sources`; an alias recurses to its
/// underlying scan so the bound input is preserved while the parent schema
/// still resolves the qualified columns.
pub fn to_kernel_plan(plan: &LogicalPlan, sources: &SourceBindings) -> Result<Plan, SqlError> {
    match plan {
        LogicalPlan::Projection(p) => project(p, sources),
        LogicalPlan::Filter(f) => {
            let input = to_kernel_plan(&f.input, sources)?;
            let pred = predicate(&f.predicate, f.input.schema())?;
            Ok(Plan::Filter {
                input: Box::new(input),
                pred,
            })
        }
        LogicalPlan::Aggregate(a) => translate_aggregate(a, sources),
        LogicalPlan::TableScan(t) => {
            let name = t.table_name.to_string();
            let id = sources
                .get(&name)
                .ok_or_else(|| SqlError::Catalog(format!("unknown source relation: {name}")))?;
            Ok(Plan::Source(*id))
        }
        LogicalPlan::SubqueryAlias(a) => to_kernel_plan(&a.input, sources),
        LogicalPlan::Join(j) => translate_join(j, sources),
        other => Err(SqlError::Unsupported(format!(
            "unsupported logical plan node: {other:?}"
        ))),
    }
}

/// DataFusion wraps every `SELECT` over an aggregate in a projection; the
/// kernel normalizes aggregate output itself. Unwrap that wrapper only when it
/// is an identity selection, so lossy transforms are rejected rather than
/// silently dropped.
fn project(p: &Projection, sources: &SourceBindings) -> Result<Plan, SqlError> {
    if let LogicalPlan::Aggregate(a) = p.input.as_ref() {
        ensure_identity_projection(&p.expr, &a.schema)?;
        return to_kernel_plan(&p.input, sources);
    }
    let input = to_kernel_plan(&p.input, sources)?;
    let cols = projection_indices(&p.expr, p.input.schema())?;
    Ok(Plan::Project {
        input: Box::new(input),
        cols,
    })
}

fn translate_aggregate(a: &Aggregate, sources: &SourceBindings) -> Result<Plan, SqlError> {
    let input = to_kernel_plan(&a.input, sources)?;
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
    let aggs = parse_aggs(&a.aggr_expr, schema)?;
    reconcile_output_types(a, &aggs)?;
    // A grouping-key-less aggregate cannot be represented: every irreducible
    // kernel group operator requires a non-empty key, so reject it here while
    // planning the view, not later at ingest.
    if key.is_empty() {
        return Err(SqlError::Unsupported(
            "aggregate without a grouping key is not supported".into(),
        ));
    }
    match tumble {
        Some((time_col, size)) => {
            if aggs.as_slice() != [AggSpec::count()] {
                return Err(SqlError::Unsupported(
                    "windowed aggregates other than count(*) are not supported".into(),
                ));
            }
            Ok(Plan::TumbleCount {
                input: Box::new(input),
                key,
                time_col,
                size,
            })
        }
        None => Ok(Plan::GroupAggregate {
            input: Box::new(input),
            key,
            aggs,
        }),
    }
}

/// Fail-stop unless our mirrored aggregate output types match the types
/// DataFusion resolved into `a.schema`.
///
/// [`aggregate_output_type`] restates DataFusion's rules for the supported
/// subset. If the two ever disagree, the view schema built from our mirror
/// would not match the plan that produces the rows, so reject at translation
/// time instead of emitting a mis-typed changelog. The aggregate outputs follow
/// the group expressions in `a.schema`.
fn reconcile_output_types(a: &Aggregate, aggs: &[AggSpec]) -> Result<(), SqlError> {
    let input = a.input.schema();
    let offset = a.group_expr.len();
    for (index, agg) in aggs.iter().enumerate() {
        let input_type = agg.input.map(|i| input.field(i).data_type());
        let ours = aggregate_output_type(agg.func, input_type)
            .map_err(|e| SqlError::Unsupported(e.to_string()))?;
        let resolved = a.schema.field(offset + index).data_type();
        if &ours != resolved {
            return Err(SqlError::Unsupported(format!(
                "{} output type mismatch: kernel {ours:?}, DataFusion {resolved:?}",
                agg.func.output_name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
