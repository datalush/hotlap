//! SQL layer error type.

/// Errors raised by the SQL/DDL layer.
#[derive(Debug)]
pub enum SqlError {
    /// The SQL/DDL text could not be parsed.
    Parse(String),
    /// The statement is valid SQL but outside the supported subset.
    Unsupported(String),
    /// The catalog rejected the operation (unknown name, duplicate, ...).
    Catalog(String),
    /// The engine/runtime failed.
    Engine(String),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(m) => write!(f, "parse: {m}"),
            Self::Unsupported(m) => write!(f, "unsupported: {m}"),
            Self::Catalog(m) => write!(f, "catalog: {m}"),
            Self::Engine(m) => write!(f, "engine: {m}"),
        }
    }
}
impl std::error::Error for SqlError {}
