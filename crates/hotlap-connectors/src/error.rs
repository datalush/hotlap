//! Connector error type.

/// Errors raised by connector sources and sinks.
#[derive(Debug)]
pub enum ConnectorError {
    /// A Fluss client failure.
    Fluss(String),
    /// An Arrow conversion failure.
    Arrow(String),
    /// The caller asked for something outside the supported subset.
    Unsupported(String),
    /// The runtime failed (worker/channel stopped, etc.).
    Infrastructure(String),
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fluss(m) => write!(f, "fluss error: {m}"),
            Self::Arrow(m) => write!(f, "arrow error: {m}"),
            Self::Unsupported(m) => write!(f, "unsupported: {m}"),
            Self::Infrastructure(m) => write!(f, "infrastructure: {m}"),
        }
    }
}

impl std::error::Error for ConnectorError {}
