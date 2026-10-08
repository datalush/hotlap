//! Filter predicates over Arrow batches with SQL three-valued (Kleene) logic.

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::kernels::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::error::CoreError;

mod eval;

/// A scalar literal used by [`Predicate`].
///
/// `PartialEq` is implemented manually because the `F64` payload is not `Eq`.
/// Floats compare with [`f64::total_cmp`], so equality is reflexive even for
/// `NaN` and distinguishes `-0.0` from `0.0`. That keeps `Predicate`/`Plan`
/// equality (and the change detection built on it) well defined.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    I64(i64),
    I32(i32),
    F64(f64),
    Str(String),
    Bool(bool),
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Scalar::Null, Scalar::Null) => true,
            (Scalar::I64(a), Scalar::I64(b)) => a == b,
            (Scalar::I32(a), Scalar::I32(b)) => a == b,
            (Scalar::F64(a), Scalar::F64(b)) => a.total_cmp(b).is_eq(),
            (Scalar::Str(a), Scalar::Str(b)) => a == b,
            (Scalar::Bool(a), Scalar::Bool(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Scalar {}

/// The comparison operator of [`Predicate::Cmp`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Filter predicate over a row.
///
/// Comparisons follow SQL three-valued logic: a null operand yields a null
/// (unknown) result. [`Predicate::And`]/[`Predicate::Or`] use Kleene logic, so
/// `NULL AND TRUE` is null while `NULL OR TRUE` is true, and `NOT NULL` is
/// null. The filter operator treats a null mask entry as `false`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Predicate {
    /// Compares the column at `col` against `scalar` with `op`.
    Cmp {
        op: CmpOp,
        col: usize,
        scalar: Scalar,
    },
    /// Logical `AND` of two predicates.
    And(Box<Predicate>, Box<Predicate>),
    /// Logical `OR` of two predicates.
    Or(Box<Predicate>, Box<Predicate>),
    /// Logical `NOT` of a predicate.
    Not(Box<Predicate>),
    /// True where the column at `col` is null.
    IsNull(usize),
    /// True where the column at `col` is not null.
    IsNotNull(usize),
}

impl Predicate {
    /// Evaluates the predicate over `batch`, returning one boolean per row.
    ///
    /// Unknown results are null entries in the mask. An out-of-range column is
    /// `Unsupported`.
    pub fn eval(&self, batch: &RecordBatch) -> Result<BooleanArray, CoreError> {
        match self {
            Predicate::Cmp { op, col, scalar } => eval::cmp(column(batch, *col)?, *op, scalar),
            Predicate::And(l, r) => Ok(and_kleene(&l.eval(batch)?, &r.eval(batch)?)?),
            Predicate::Or(l, r) => Ok(or_kleene(&l.eval(batch)?, &r.eval(batch)?)?),
            Predicate::Not(p) => Ok(not(&p.eval(batch)?)?),
            Predicate::IsNull(col) => Ok(is_null(column(batch, *col)?.as_ref())?),
            Predicate::IsNotNull(col) => Ok(is_not_null(column(batch, *col)?.as_ref())?),
        }
    }
}

/// Returns the column at `col`, or `Unsupported` when it is out of range.
fn column(batch: &RecordBatch, col: usize) -> Result<&ArrayRef, CoreError> {
    batch.columns().get(col).ok_or_else(|| {
        CoreError::Unsupported(format!(
            "predicate column {col} out of range (arity {})",
            batch.num_columns()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::Scalar;

    #[test]
    fn scalar_equality_is_reflexive_for_nan() {
        let nan = Scalar::F64(f64::NAN);
        assert_eq!(nan, nan.clone());
        assert_ne!(Scalar::F64(-0.0), Scalar::F64(0.0));
    }
}
