//! Unified error type for the embedded [`Session`](crate::Session) API.

use hotlap_connectors::error::ConnectorError;
use hotlap_sql::error::SqlError;

/// Errors surfaced by the embedded session API.
#[derive(Debug)]
pub enum SessionError {
    /// SQL parsing, planning or catalog failure.
    Sql(SqlError),
    /// Engine, runtime or connector failure.
    Engine(String),
    /// The session was used in an invalid state.
    State(String),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(error) => write!(f, "sql: {error}"),
            Self::Engine(message) => write!(f, "engine: {message}"),
            Self::State(message) => write!(f, "state: {message}"),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Engine(_) | Self::State(_) => None,
        }
    }
}

impl From<SqlError> for SessionError {
    fn from(error: SqlError) -> Self {
        Self::Sql(error)
    }
}

impl From<ConnectorError> for SessionError {
    fn from(error: ConnectorError) -> Self {
        Self::Engine(error.to_string())
    }
}
