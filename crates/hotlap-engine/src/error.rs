use std::fmt;

/// Errors produced by the Arrow-native engine kernel.
#[derive(Debug)]
pub enum EngineError {
    /// The requested operation is not supported for the given input.
    Unsupported(String),
    /// An underlying Arrow or infrastructure failure.
    Infrastructure(String),
    /// A signed multiplicity operation exceeded the supported `i64` range.
    NumericOverflow(&'static str),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Unsupported(message) => write!(f, "unsupported: {message}"),
            EngineError::Infrastructure(message) => write!(f, "infrastructure: {message}"),
            EngineError::NumericOverflow(operation) => write!(f, "numeric overflow: {operation}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<arrow::error::ArrowError> for EngineError {
    fn from(error: arrow::error::ArrowError) -> Self {
        EngineError::Infrastructure(error.to_string())
    }
}

impl From<hotlap_core::CoreError> for EngineError {
    fn from(error: hotlap_core::CoreError) -> Self {
        match error {
            hotlap_core::CoreError::Unsupported(message) => EngineError::Unsupported(message),
            hotlap_core::CoreError::Infrastructure(message) => EngineError::Infrastructure(message),
            hotlap_core::CoreError::NumericOverflow(operation) => {
                EngineError::NumericOverflow(operation)
            }
        }
    }
}

impl From<EngineError> for hotlap_core::CoreError {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::Unsupported(message) => hotlap_core::CoreError::Unsupported(message),
            EngineError::Infrastructure(message) => hotlap_core::CoreError::Infrastructure(message),
            EngineError::NumericOverflow(operation) => {
                hotlap_core::CoreError::NumericOverflow(operation)
            }
        }
    }
}
