//! Error type of the differential-dataflow-free contract.

use std::fmt;

/// Errors produced at the [`IncrementalCore`] boundary.
///
/// [`IncrementalCore`]: crate::core::IncrementalCore
#[derive(Debug)]
pub enum CoreError {
    /// The caller's plan or usage is not supported (bad plan, unknown view or
    /// input, out-of-range index, duplicate declaration, post-freeze mutation).
    Unsupported(String),
    /// The backing engine failed (an Arrow failure or an internal invariant).
    Infrastructure(String),
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoreError::Unsupported(message) => write!(f, "unsupported: {message}"),
            CoreError::Infrastructure(message) => write!(f, "infrastructure: {message}"),
        }
    }
}

impl std::error::Error for CoreError {}

impl From<arrow::error::ArrowError> for CoreError {
    fn from(error: arrow::error::ArrowError) -> Self {
        CoreError::Infrastructure(error.to_string())
    }
}
