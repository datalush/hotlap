//! Inner equi-join translation and the two-source guard.

use datafusion::common::JoinType;
use datafusion::logical_expr::{Expr, Join, Operator};
use hotlap::{InputId, Plan};

use crate::bindings::SourceBindings;
use crate::error::SqlError;
use crate::translate_expr::column_index;

use super::to_kernel_plan;

/// Translate an inner join, resolving each side through `sources`.
pub(super) fn translate_join(j: &Join, sources: &SourceBindings) -> Result<Plan, SqlError> {
    if j.join_type != JoinType::Inner {
        return Err(SqlError::Unsupported(
            "only inner equi-joins are supported".into(),
        ));
    }
    let left = to_kernel_plan(&j.left, sources)?;
    let right = to_kernel_plan(&j.right, sources)?;
    reject_more_than_two_inputs(&left, &right)?;
    let (left_key, right_key) = join_keys(j)?;
    Ok(Plan::Join {
        left: Box::new(left),
        right: Box::new(right),
        left_key,
        right_key,
    })
}

/// The equality keys of the ON condition, in the order they appear.
///
/// DataFusion usually carries a SQL `ON` predicate in `filter` (with `on`
/// empty); a normalized plan may keep equi-pairs in `on`. Both are read, so a
/// residual predicate in `filter` is rejected even when `on` is populated.
fn join_keys(j: &Join) -> Result<(Vec<usize>, Vec<usize>), SqlError> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for (l, r) in &j.on {
        left.push(column_index(l, j.left.schema())?);
        right.push(column_index(r, j.right.schema())?);
    }
    if let Some(filter) = &j.filter {
        collect_keys(filter, j, &mut left, &mut right)?;
    }
    if left.is_empty() {
        return Err(SqlError::Unsupported(
            "join without an equality condition".into(),
        ));
    }
    Ok((left, right))
}

/// Walk the ON condition, pushing one `(left, right)` key pair per equality.
fn collect_keys(
    expr: &Expr,
    j: &Join,
    left: &mut Vec<usize>,
    right: &mut Vec<usize>,
) -> Result<(), SqlError> {
    match expr {
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            collect_keys(&b.left, j, left, right)?;
            collect_keys(&b.right, j, left, right)
        }
        Expr::BinaryExpr(b) if b.op == Operator::Eq => {
            let (l, r) = orient(&b.left, &b.right, j)?;
            left.push(column_index(l, j.left.schema())?);
            right.push(column_index(r, j.right.schema())?);
            Ok(())
        }
        other => Err(SqlError::Unsupported(format!(
            "residual join condition `{other}`"
        ))),
    }
}

/// Order an equality so the first expression resolves in the left schema.
fn orient<'a>(a: &'a Expr, b: &'a Expr, j: &Join) -> Result<(&'a Expr, &'a Expr), SqlError> {
    if let Expr::Column(c) = a {
        if j.left.schema().index_of_column(c).is_ok() {
            return Ok((a, b));
        }
        if j.right.schema().index_of_column(c).is_ok() {
            return Ok((b, a));
        }
    }
    Err(SqlError::Unsupported(
        "join condition must compare a column from each side".into(),
    ))
}

/// A join in this slice reads at most two distinct source inputs. A third one
/// would otherwise be folded into a binary plan whose runtime registration is
/// not defined, so reject it instead of silently dropping a relation.
fn reject_more_than_two_inputs(left: &Plan, right: &Plan) -> Result<(), SqlError> {
    let mut ids: Vec<InputId> = hotlap::plan::sources(left);
    ids.extend(hotlap::plan::sources(right));
    ids.sort();
    ids.dedup();
    if ids.len() > 2 {
        return Err(SqlError::Unsupported(
            "joins over more than two sources are not supported".into(),
        ));
    }
    Ok(())
}
