//! Engine-owned incremental-core boundary.
//!
//! No `differential-dataflow` type crosses this boundary: the trait speaks only in
//! engine types (`Plan`, `ChangeBatch`, `Row`). The DD-backed implementation lives
//! behind [`differential_dataflow::DifferentialCore`].

pub mod differential_dataflow;

use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ViewId(pub u32);

#[derive(Debug)]
pub enum CoreError {
    /// The caller's plan or usage is not supported (bad plan, unknown view,
    /// out-of-range index, duplicate build).
    Unsupported(String),
    /// The backing worker failed: it is not running, its channel broke, or the
    /// dataflow stopped making progress.
    Infrastructure(String),
}

pub trait IncrementalCore {
    /// Compile a linear Scan->(Filter|Project)*->GroupCount plan into a live circuit.
    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError>;
    /// Feed a batch of changes to the view's input stream.
    fn push(&mut self, view_input: ViewId, batch: &ChangeBatch) -> Result<(), CoreError>;
    /// Current consolidated output as rows ([key..., count]).
    fn snapshot(&mut self, view: ViewId) -> Result<Vec<Row>, CoreError>;
}
