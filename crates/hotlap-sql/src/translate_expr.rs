//! Expression helpers for `translate`: predicates, columns, literals, `tumble`.

use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::expr::{AggregateFunction, BinaryExpr, ScalarFunction};
use datafusion::logical_expr::{Expr, Operator};
use hotlap::Scalar;
use hotlap::plan::Predicate;

use crate::error::SqlError;

/// Column position of `expr` in `schema`; rejects non-column expressions.
pub(crate) fn column_index(expr: &Expr, schema: &DFSchema) -> Result<usize, SqlError> {
    let Expr::Column(c) = expr else {
        return Err(SqlError::Unsupported(format!(
            "expected a column reference, got `{expr}`"
        )));
    };
    schema
        .index_of_column(c)
        .map_err(|e| SqlError::Unsupported(format!("unknown column `{c}`: {e}")))
}

/// Turn a projection's expressions into positional column indices.
pub(crate) fn projection_indices(
    exprs: &[Expr],
    schema: &DFSchema,
) -> Result<Vec<usize>, SqlError> {
    exprs.iter().map(|e| column_index(e, schema)).collect()
}

/// Require an identity selection: every expression is a bare column (or an
/// `Alias` of one) that resolves in `schema`. Used to unwrap the projection
/// DataFusion puts over an `Aggregate` without losing transforms.
pub(crate) fn ensure_identity_projection(
    exprs: &[Expr],
    schema: &DFSchema,
) -> Result<(), SqlError> {
    for expr in exprs {
        let col = match expr {
            Expr::Column(c) => c,
            Expr::Alias(a) => match a.expr.as_ref() {
                Expr::Column(c) => c,
                other => return Err(unsupported("identity projection", other)),
            },
            other => return Err(unsupported("identity projection", other)),
        };
        schema
            .index_of_column(col)
            .map_err(|e| SqlError::Unsupported(format!("unknown column `{col}`: {e}")))?;
    }
    Ok(())
}

/// Translate a filter expression into an IR predicate (`Eq`/`Gt` only).
pub(crate) fn predicate(expr: &Expr, schema: &DFSchema) -> Result<Predicate, SqlError> {
    let Expr::BinaryExpr(b) = expr else {
        return Err(unsupported("filter", expr));
    };
    match b.op {
        Operator::Eq => {
            let (col, lit) = column_literal(b, true)?;
            Ok(Predicate::Eq(column_index(col, schema)?, scalar(lit)?))
        }
        Operator::Gt => {
            let (col, lit) = column_literal(b, false)?;
            match scalar(lit)? {
                Scalar::I64(n) => Ok(Predicate::Gt(column_index(col, schema)?, n)),
                other => Err(SqlError::Unsupported(format!(
                    "`>` expects an Int64 literal, got {other:?}"
                ))),
            }
        }
        op => Err(SqlError::Unsupported(format!(
            "unsupported predicate operator: {op:?}"
        ))),
    }
}

/// Is this scalar function a tumbling-window call?
pub(crate) fn is_tumble(sf: &ScalarFunction) -> bool {
    sf.name().eq_ignore_ascii_case("tumble")
}

/// Extract `(time_col, size)` from a `tumble(ts, size)` call.
pub(crate) fn parse_tumble(
    sf: &ScalarFunction,
    schema: &DFSchema,
) -> Result<(usize, i64), SqlError> {
    let [ts, size] = sf.args.as_slice() else {
        return Err(SqlError::Unsupported(
            "`tumble` expects exactly two arguments".into(),
        ));
    };
    Ok((column_index(ts, schema)?, int_literal(size)?))
}

/// Require the aggregate list to be a single plain `count(*)`: no `DISTINCT`,
/// `FILTER`, `ORDER BY`, `NULL TREATMENT`, or per-column argument.
pub(crate) fn ensure_count_only(aggr: &[Expr]) -> Result<(), SqlError> {
    let [expr] = aggr else {
        return Err(SqlError::Unsupported(format!(
            "expected exactly one aggregate, got {}",
            aggr.len()
        )));
    };
    match expr {
        Expr::AggregateFunction(af) if is_plain_count(af) => Ok(()),
        other => Err(SqlError::Unsupported(format!(
            "only `count(*)` is supported, got `{other}`"
        ))),
    }
}

/// A `count(*)` with no modifiers; anything else would be silently dropped.
fn is_plain_count(af: &AggregateFunction) -> bool {
    af.func.name().eq_ignore_ascii_case("count")
        && !af.params.distinct
        && af.params.filter.is_none()
        && af.params.order_by.is_empty()
        && af.params.null_treatment.is_none()
        && is_count_star(&af.params.args)
}

/// `count(*)` is normalized to `count(1)`; bare `count()` is also accepted.
fn is_count_star(args: &[Expr]) -> bool {
    matches!(args, [] | [Expr::Literal(ScalarValue::Int64(Some(1)), _)])
}

fn column_literal(b: &BinaryExpr, swap_ok: bool) -> Result<(&Expr, &Expr), SqlError> {
    let left_col = matches!(b.left.as_ref(), Expr::Column(_));
    let right_col = matches!(b.right.as_ref(), Expr::Column(_));
    match (left_col, right_col) {
        (true, false) => Ok((&b.left, &b.right)),
        (false, true) if swap_ok => Ok((&b.right, &b.left)),
        _ => Err(SqlError::Unsupported(format!(
            "expected `column <op> literal`, got `{b}`"
        ))),
    }
}

fn int_literal(expr: &Expr) -> Result<i64, SqlError> {
    match scalar(expr)? {
        Scalar::I64(v) => Ok(v),
        other => Err(SqlError::Unsupported(format!(
            "expected an Int64 literal, got {other:?}"
        ))),
    }
}

fn scalar(expr: &Expr) -> Result<Scalar, SqlError> {
    let Expr::Literal(value, _) = expr else {
        return Err(unsupported("literal", expr));
    };
    match value {
        ScalarValue::Int64(Some(v)) => Ok(Scalar::I64(*v)),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => Ok(Scalar::Str(v.clone())),
        ScalarValue::Boolean(Some(v)) => Ok(Scalar::Bool(*v)),
        ScalarValue::Int64(None) => Ok(Scalar::Null),
        other => Err(SqlError::Unsupported(format!(
            "unsupported literal `{other:?}`"
        ))),
    }
}

fn unsupported(what: &str, expr: &Expr) -> SqlError {
    SqlError::Unsupported(format!("unsupported {what}: `{expr}`"))
}
