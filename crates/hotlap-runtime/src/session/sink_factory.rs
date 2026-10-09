//! `SinkFactory` abstraction and its default Fluss implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use hotlap_connectors::fluss::sink::FlussSink;
use hotlap_connectors::sink::Sink;
use hotlap_sql::error::SqlError;

/// Builds engine sinks from `CREATE SINK` options and the target view schema.
#[async_trait::async_trait]
pub trait SinkFactory: Send + Sync {
    /// Create the sink named `name`, writing rows shaped by `schema`.
    async fn create(
        &self,
        name: &str,
        options: &BTreeMap<String, String>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError>;

    /// Whether the sinks this factory builds apply retractions (negative
    /// diffs). Defaults to `false`: a factory that has not declared support is
    /// treated as append-only, so a retracting plan is refused before `create`
    /// opens any writer. The created sink is still re-checked, so a factory
    /// cannot understate what it builds.
    fn accepts_retractions(&self, _options: &BTreeMap<String, String>) -> bool {
        false
    }
}

/// Builds a [`FlussSink`] from the DDL options (`bootstrap`, `table`).
#[derive(Debug, Default)]
pub struct FlussSinkFactory;

#[async_trait::async_trait]
impl SinkFactory for FlussSinkFactory {
    async fn create(
        &self,
        _name: &str,
        options: &BTreeMap<String, String>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        require_connector(options)?;
        // The sink name is unused: the Fluss table path identifies the target.
        let bootstrap = required(options, "bootstrap")?;
        let table = required(options, "table")?;
        let sink = FlussSink::open_from_bootstrap(bootstrap, table, schema)
            .await
            .map_err(|error| SqlError::Engine(error.to_string()))?;
        Ok(Arc::new(sink))
    }

    fn accepts_retractions(&self, _options: &BTreeMap<String, String>) -> bool {
        // Fluss append and upsert writers both reject negative diffs.
        false
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
        .ok_or_else(|| SqlError::Unsupported(format!("CREATE SINK requires `{key}`")))
}
