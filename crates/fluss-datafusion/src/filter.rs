// SPDX-License-Identifier: Apache-2.0
//! Conservative DataFusion -> Fluss batch-pruning predicate translation.
//!
//! Only integral comparisons whose literal is representable in the source
//! column are pushed. Fluss returns whole surviving batches, so DataFusion
//! always evaluates the original expression exactly after the scan.

use arrow::datatypes::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};
use fluss::predicate::{Predicate, col};

pub(crate) fn translate(expr: &Expr, schema: &Schema) -> Option<Predicate> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op == Operator::And {
        return match (
            translate(&binary.left, schema),
            translate(&binary.right, schema),
        ) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
            (None, None) => None,
        };
    }
    let (field, data_type) = column(&binary.left, schema)?;
    let literal = literal(&binary.right)?;
    let value = match data_type {
        DataType::Int32 => i32::try_from(literal)
            .ok()
            .map(fluss::predicate::Literal::from),
        DataType::Int64 => Some(fluss::predicate::Literal::from(literal)),
        _ => None,
    }?;
    let column = col(field);
    match binary.op {
        Operator::Eq => Some(column.eq(value)),
        Operator::NotEq => Some(column.ne(value)),
        Operator::Lt => Some(column.lt(value)),
        Operator::LtEq => Some(column.le(value)),
        Operator::Gt => Some(column.gt(value)),
        Operator::GtEq => Some(column.ge(value)),
        _ => None,
    }
}

fn column<'a>(expr: &'a Expr, schema: &'a Schema) -> Option<(&'a str, &'a DataType)> {
    match expr {
        Expr::Column(column) => schema
            .field_with_name(&column.name)
            .ok()
            .map(|field| (field.name().as_str(), field.data_type())),
        // Widening an Int32 column to Int64 preserves its ordering exactly.
        Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => {
            let (name, data_type) = column(&cast.expr, schema)?;
            (data_type == &DataType::Int32).then_some((name, data_type))
        }
        _ => None,
    }
}

fn literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => literal(&cast.expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::logical_expr::{col as df_col, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
        ])
    }

    #[test]
    fn integral_comparisons_and_safe_conjunction_only() {
        assert!(translate(&df_col("id").gt(lit(5_i32)), &schema()).is_some());
        assert!(translate(&df_col("id").lt(lit(5_i64)), &schema()).is_some());
        assert!(translate(&df_col("id").lt(lit(i64::MAX)), &schema()).is_none());
        assert!(translate(&df_col("text").eq(lit("abc")), &schema()).is_none());
        assert!(
            translate(
                &df_col("id").gt(lit(5_i32)).and(df_col("text").eq(lit("a"))),
                &schema()
            )
            .is_some()
        );
        assert!(
            translate(
                &df_col("id").gt(lit(5_i32)).or(df_col("text").eq(lit("a"))),
                &schema()
            )
            .is_none()
        );
    }
}
