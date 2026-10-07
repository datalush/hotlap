//! Fluss source connector: a narrow seam over `fluss-rs` plus the `Source` impl.

use fluss::metadata::TablePath;

use crate::error::ConnectorError;

pub mod assemble;
pub mod log_reader;
pub mod source;

/// Map a `fluss-rs` error into the connector error type.
pub(crate) fn fluss_err(error: fluss::error::Error) -> ConnectorError {
    ConnectorError::Fluss(error.to_string())
}

/// Parse `<database>/<table>` (a leading `/` is tolerated).
pub(crate) fn parse_path(path: &str) -> Result<TablePath, ConnectorError> {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((database, table)) if !database.is_empty() && !table.is_empty() => {
            Ok(TablePath::new(database, table))
        }
        _ => Err(ConnectorError::Unsupported(format!(
            "expected `<database>/<table>`, got `{path}`"
        ))),
    }
}
