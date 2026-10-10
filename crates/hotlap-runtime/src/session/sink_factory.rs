//! `SinkFactory` abstraction and its default Fluss implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use hotlap_connectors::fluss::sink::FlussSink;
use hotlap_connectors::sink::Sink;
use hotlap_sql::error::SqlError;

use super::SqlSession;
use crate::runtime::pipeline::SinkDescription;
use crate::runtime::pipeline::SinkSpec;

/// Builds engine sinks from `CREATE SINK` options and the target view schema.
#[async_trait::async_trait]
pub trait SinkFactory: Send + Sync {
    /// Resolve the real target and delivery metadata without creating a writer.
    ///
    /// Durable sessions reject factories that do not provide this proof before
    /// calling [`Self::create`]. Non-durable sessions do not require it.
    async fn describe(
        &self,
        _binding_name: &str,
        _options: &BTreeMap<String, String>,
        _schema: SchemaRef,
        _view: &str,
    ) -> Result<Option<SinkDescription>, SqlError> {
        Ok(None)
    }

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
    async fn describe(
        &self,
        binding_name: &str,
        options: &BTreeMap<String, String>,
        _schema: SchemaRef,
        view: &str,
    ) -> Result<Option<SinkDescription>, SqlError> {
        require_connector(options)?;
        let bootstrap = required(options, "bootstrap")?;
        let table = required(options, "table")?;
        let metadata = FlussSink::describe_target(bootstrap, table)
            .await
            .map_err(|error| SqlError::Engine(error.to_string()))?;
        Ok(Some(SinkDescription {
            binding_name: binding_name.to_owned(),
            view: view.to_owned(),
            physical_identity: metadata.physical_identity,
            capabilities: metadata.capabilities,
            accepts_retractions: false,
            commit_redriable: false,
        }))
    }

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
    pub(super) async fn describe_sinks(&self) -> Result<Vec<SinkDescription>, SqlError> {
        let mut descriptions = Vec::with_capacity(self.sinks.len());
        for def in &self.sinks {
            let schema = self
                .mv_schemas
                .get(&def.view)
                .cloned()
                .ok_or_else(|| SqlError::Catalog(format!("unknown view: {}", def.view)))?;
            let description = self
                .sink_factory
                .describe(&def.name, &def.options, schema, &def.view)
                .await?
                .ok_or_else(|| {
                    SqlError::Unsupported(format!(
                        "sink factory cannot resolve durable target metadata for `{}`",
                        def.name
                    ))
                })?;
            description.validate().map_err(to_sql_error)?;
            if description.binding_name != def.name || description.view != def.view {
                return Err(SqlError::Unsupported(format!(
                    "sink factory metadata binding for `{}` does not match its declaration",
                    def.name
                )));
            }
            descriptions.push(description);
        }
        Ok(descriptions)
    }

    /// Open every declared sink via the factory, verifying each created target
    /// against the metadata-only description captured before creation.
    pub(super) async fn build_sinks(
        &self,
        descriptions: Option<&[SinkDescription]>,
    ) -> Result<Vec<SinkSpec>, SqlError> {
        let mut sinks = Vec::with_capacity(self.sinks.len());
        for (index, def) in self.sinks.iter().enumerate() {
            let schema = self
                .mv_schemas
                .get(&def.view)
                .cloned()
                .ok_or_else(|| SqlError::Catalog(format!("unknown view: {}", def.view)))?;
            let sink = self
                .sink_factory
                .create(&def.name, &def.options, schema)
                .await?;
            let spec = SinkSpec::named(&def.name, &def.view, sink);
            if let Some(descriptions) = descriptions {
                let actual = spec.description().map_err(to_sql_error)?;
                let expected = descriptions.get(index).ok_or_else(|| {
                    SqlError::Unsupported("sink factory description count changed".into())
                })?;
                verify_sink_description(&def.name, expected, &actual)?;
            }
            sinks.push(spec);
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

fn to_sql_error(error: hotlap_connectors::error::ConnectorError) -> SqlError {
    match error {
        hotlap_connectors::error::ConnectorError::Unsupported(message) => {
            SqlError::Unsupported(message)
        }
        other => SqlError::Engine(other.to_string()),
    }
}

fn verify_sink_description(
    name: &str,
    expected: &SinkDescription,
    actual: &SinkDescription,
) -> Result<(), SqlError> {
    if expected != actual {
        return Err(SqlError::Unsupported(format!(
            "created sink `{name}` differs from its preflight target metadata"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FactoryWithoutDescription;

    #[async_trait::async_trait]
    impl SinkFactory for FactoryWithoutDescription {
        async fn create(
            &self,
            _name: &str,
            _options: &BTreeMap<String, String>,
            _schema: SchemaRef,
        ) -> Result<Arc<dyn Sink>, SqlError> {
            unreachable!("a metadata-only factory must not create a sink")
        }
    }

    #[tokio::test]
    async fn factory_without_metadata_description_is_explicitly_unavailable() {
        let description = FactoryWithoutDescription
            .describe(
                "sink_binding",
                &BTreeMap::new(),
                Arc::new(arrow::datatypes::Schema::empty()),
                "view_binding",
            )
            .await
            .unwrap();

        assert!(description.is_none());
    }

    #[test]
    fn sink_description_verification_rejects_binding_view_and_target_changes() {
        let expected = SinkDescription {
            binding_name: "sink_a".into(),
            view: "view_a".into(),
            physical_identity: "fluss:cluster:db/table:table-7".into(),
            capabilities: hotlap_connectors::sink::SinkCapabilities::Idempotent,
            accepts_retractions: false,
            commit_redriable: false,
        };

        for actual in [
            SinkDescription {
                binding_name: "renamed".into(),
                ..expected.clone()
            },
            SinkDescription {
                view: "view_b".into(),
                ..expected.clone()
            },
            SinkDescription {
                physical_identity: "fluss:cluster:db/other:table-8".into(),
                ..expected.clone()
            },
        ] {
            assert!(verify_sink_description("sink_a", &expected, &actual).is_err());
        }
        assert!(verify_sink_description("sink_a", &expected, &expected).is_ok());
    }
}
