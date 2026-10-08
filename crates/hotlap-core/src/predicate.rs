//! Filter predicates over Arrow batches with SQL three-valued (Kleene) logic.

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::kernels::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::error::CoreError;

mod eval;

/// A scalar literal used by [`Predicate`].
///
/// `Eq` is implemented manually because the `F64` payload is not `Eq`; the
/// equality itself still follows `f64` semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    I64(i64),
    I32(i32),
    F64(f64),
    Str(String),
    Bool(bool),
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
