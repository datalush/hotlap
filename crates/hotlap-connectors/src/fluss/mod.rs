//! Fluss source connector: a narrow seam over `fluss-rs` plus the `Source` impl.

use std::sync::{Mutex, MutexGuard};

use fluss::metadata::TablePath;

use crate::error::ConnectorError;
use crate::source::SourceState;

pub mod assemble;
pub mod log_reader;
pub mod sink;
mod sink_convert;
pub mod source;
pub mod stream;

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

/// Lock the resumable state, reporting a poisoned lock as infrastructure.
pub(crate) fn lock_progress(
    progress: &Mutex<SourceState>,
) -> Result<MutexGuard<'_, SourceState>, ConnectorError> {
    progress
        .lock()
        .map_err(|_| ConnectorError::Infrastructure("source state lock poisoned".into()))
}
