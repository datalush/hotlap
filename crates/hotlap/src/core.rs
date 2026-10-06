//! Engine-owned incremental-core boundary.
//!
//! No `differential-dataflow` type crosses this boundary: the trait speaks only in
//! engine types (`Plan`, `ChangeBatch`, `Row`). The DD-backed implementation lives
//! behind [`differential_dataflow::DifferentialCore`].

pub mod differential_dataflow;

use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Handle for a source declared to the core. One input feeds many views.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InputId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ViewId(pub u32);

#[derive(Debug)]
pub enum CoreError {
    /// The caller's plan or usage is not supported (bad plan, unknown view/input,
    /// out-of-range index, duplicate declaration, post-freeze mutation).
    Unsupported(String),
    /// The backing worker failed: it is not running, its channel broke, or the
    /// dataflow stopped making progress.
    Infrastructure(String),
}

pub trait IncrementalCore {
    /// Declare a source. Must happen before the first [`push`](Self::push).
    fn register_input(&mut self, input: InputId) -> Result<(), CoreError>;
    /// Compile a `Source`-rooted plan into a live view. Also pre-freeze only.
    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError>;
    /// Feed a batch to the given source, consolidating every consuming view.
    fn push(&mut self, input: InputId, batch: &ChangeBatch) -> Result<(), CoreError>;
    /// Current consolidated output of a view as rows.
    fn snapshot(&mut self, view: ViewId) -> Result<Vec<Row>, CoreError>;
}
