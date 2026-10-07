//! Default Fluss `SourceFactory` for `CREATE SOURCE ... connector='fluss'`.

use std::collections::BTreeMap;

use hotlap_connectors::fluss::source::FlussSource;
use hotlap_connectors::source::Source;

use super::SourceFactory;
use crate::error::SqlError;

/// Builds a [`FlussSource`] from the DDL options (`bootstrap`, `table`).
#[derive(Debug, Default)]
pub struct FlussSourceFactory;

#[async_trait::async_trait]
impl SourceFactory for FlussSourceFactory {
    async fn create(
        &self,
        _name: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        require_connector(options)?;
        // The source name is unused: the Fluss table path identifies the data.
        let bootstrap = required(options, "bootstrap")?;
        // `table` is a `<database>/<table>` path, per `open_from_bootstrap`.
        let table = required(options, "table")?;
        let source = FlussSource::open_from_bootstrap(bootstrap, table)
            .await
            .map_err(|error| SqlError::Engine(error.to_string()))?;
        Ok(Box::new(source))
    }
}

fn require_connector(options: &BTreeMap<String, String>) -> Result<(), SqlError> {
    match options.get("connector").map(String::as_str) {
        Some("fluss") => Ok(()),
        other => Err(SqlError::Unsupported(format!(
            "unsupported connector `{}`; expected `fluss`",
            other.unwrap_or("")
        ))),
    }
}

fn required<'a>(options: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str, SqlError> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| SqlError::Unsupported(format!("CREATE SOURCE requires `{key}`")))
}
