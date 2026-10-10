//! `SinkFactory` abstraction and its default Fluss implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use hotlap_connectors::fluss::sink::FlussSink;
use hotlap_connectors::sink::Sink;
use hotlap_sql::error::SqlError;

use super::SqlSession;
use crate::runtime::pipeline::SinkSpec;

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

    /// Whether this factory may produce a transactional sink for `options`.
    ///
    /// Unknown factories default to `true`, so recovery rejects an unsafe
    /// pending checkpoint before opening a writer unless the factory can prove
    /// it only creates replay-safe non-transactional sinks.
    fn may_create_transactional(&self, _options: &BTreeMap<String, String>) -> bool {
        true
    }

    /// Whether a sink created for `options` can durably re-drive an interrupted
    /// commit after process restart. Unknown factories default to `false`.
    fn may_redrive_commit(&self, _options: &BTreeMap<String, String>) -> bool {
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

    fn may_create_transactional(&self, _options: &BTreeMap<String, String>) -> bool {
        false
    }
}

impl SqlSession {
    /// Open every declared sink via the factory, resolving its view schema.
    pub(super) async fn build_sinks(&self) -> Result<Vec<SinkSpec>, SqlError> {
        let mut sinks = Vec::with_capacity(self.sinks.len());
        for def in &self.sinks {
            let schema = self
                .mv_schemas
                .get(&def.view)
                .cloned()
                .ok_or_else(|| SqlError::Catalog(format!("unknown view: {}", def.view)))?;
            let sink = self
                .sink_factory
                .create(&def.name, &def.options, schema)
                .await?;
            sinks.push(SinkSpec {
                view: def.view.clone(),
                sink,
            });
        }
        Ok(sinks)
    }

    /// Ensure the opened sink did not contradict its preflight declaration.
    pub(super) fn validate_recovery_declarations(
        &self,
        sinks: &[SinkSpec],
    ) -> Result<(), SqlError> {
        for (definition, sink) in self.sinks.iter().zip(sinks) {
            if !self
                .sink_factory
                .may_create_transactional(&definition.options)
                && sink.sink.capabilities()
                    == hotlap_connectors::sink::SinkCapabilities::Transactional
            {
                return Err(SqlError::Unsupported(format!(
                    "sink factory declared `{}` non-transactional but created a transactional sink",
                    definition.name
                )));
            }
            if self.sink_factory.may_redrive_commit(&definition.options)
                && !sink.sink.commit_redriable()
            {
                return Err(SqlError::Unsupported(format!(
                    "sink factory declared `{}` re-drivable but created a non-re-drivable sink",
                    definition.name
                )));
            }
        }
        Ok(())
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
