use std::fmt;

/// Errors produced by the Arrow-native engine kernel.
#[derive(Debug)]
pub enum EngineError {
    /// The requested operation is not supported for the given input.
    Unsupported(String),
    /// An underlying Arrow or infrastructure failure.
    Infrastructure(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Unsupported(message) => write!(f, "unsupported: {message}"),
            EngineError::Infrastructure(message) => write!(f, "infrastructure: {message}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<arrow::error::ArrowError> for EngineError {
    fn from(error: arrow::error::ArrowError) -> Self {
        EngineError::Infrastructure(error.to_string())
    }
}
