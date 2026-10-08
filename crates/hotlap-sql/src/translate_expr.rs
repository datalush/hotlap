//! Expression helpers for `translate`: columns, literals and `tumble`.

use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use hotlap::{AggFunc, AggSpec, Scalar};

use crate::error::SqlError;
use crate::translate_predicate::scalar;

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

/// Parse a logical aggregate list into kernel `AggSpec`s.
///
/// Each entry must be a plain `count`/`sum`/`avg` over `*` or one column. The
/// `min`/`max` functions are reserved for a later change; unsupported functions
/// and modifiers (`DISTINCT`, `FILTER`, `ORDER BY`, `NULL TREATMENT`) are
/// rejected rather than silently dropped.
pub(crate) fn parse_aggs(aggr: &[Expr], schema: &DFSchema) -> Result<Vec<AggSpec>, SqlError> {
    if aggr.is_empty() {
        return Err(SqlError::Unsupported(
            "at least one aggregate is required".into(),
        ));
    }
    aggr.iter().map(|expr| parse_agg(expr, schema)).collect()
}

/// Parse one aggregate expression into an `AggSpec`.
fn parse_agg(expr: &Expr, schema: &DFSchema) -> Result<AggSpec, SqlError> {
    let expr = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(af) = expr else {
        return Err(SqlError::Unsupported(format!(
            "expected an aggregate function, got `{expr}`"
        )));
    };
    ensure_plain(af)?;
    let name = af.func.name().to_ascii_lowercase();
    match name.as_str() {
        "count" => Ok(AggSpec {
            func: AggFunc::Count,
            input: count_input(&af.params.args, schema)?,
        }),
        "sum" => Ok(AggSpec::sum(column_arg(&af.params.args, schema)?)),
        "avg" => Ok(AggSpec::avg(column_arg(&af.params.args, schema)?)),
        "min" | "max" => Err(SqlError::Unsupported(format!(
            "`{name}` aggregates are not implemented yet"
        ))),
        other => Err(SqlError::Unsupported(format!(
            "aggregate `{other}` is not supported"
        ))),
    }
}

/// Reject modifiers that would change the aggregate's meaning if dropped.
fn ensure_plain(af: &AggregateFunction) -> Result<(), SqlError> {
    let modified = af.params.distinct
        || af.params.filter.is_some()
        || !af.params.order_by.is_empty()
        || af.params.null_treatment.is_some();
    if modified {
        return Err(SqlError::Unsupported(
            "aggregate modifiers (`DISTINCT`, `FILTER`, `ORDER BY`) are not supported".into(),
        ));
    }
    Ok(())
}

/// `count(*)` is normalized to `count(1)`; a bare column counts non-nulls.
fn count_input(args: &[Expr], schema: &DFSchema) -> Result<Option<usize>, SqlError> {
    match args {
        [] | [Expr::Literal(ScalarValue::Int64(Some(1)), _)] => Ok(None),
        [Expr::Column(_)] => Ok(Some(column_index(&args[0], schema)?)),
        _ => Err(SqlError::Unsupported(
            "`count` expects `*` or exactly one column".into(),
        )),
    }
}

/// The single column argument of `sum`/`avg`/`min`/`max`.
fn column_arg(args: &[Expr], schema: &DFSchema) -> Result<usize, SqlError> {
    match args {
        [Expr::Column(_)] => column_index(&args[0], schema),
        _ => Err(SqlError::Unsupported(
            "aggregate expects exactly one column".into(),
        )),
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

fn unsupported(what: &str, expr: &Expr) -> SqlError {
    SqlError::Unsupported(format!("unsupported {what}: `{expr}`"))
}
