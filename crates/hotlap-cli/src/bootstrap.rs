//! Fluss factories that fill an omitted `bootstrap` option with a CLI default.
//!
//! `CREATE SOURCE`/`CREATE SINK` carry their own Fluss options; when the REPL
//! is started with `--bootstrap`, that address becomes the default so users can
//! omit it in DDL.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use hotlap_connectors::sink::Sink;
use hotlap_connectors::source::Source;
use hotlap_runtime::{FlussSinkFactory, FlussSourceFactory, SinkFactory, SourceFactory};
use hotlap_sql::SqlError;

/// Source factory applying a default `bootstrap` to Fluss DDL options.
pub struct BootstrapSourceFactory {
    bootstrap: String,
    inner: FlussSourceFactory,
}

impl BootstrapSourceFactory {
    /// Wrap the Fluss source factory with `bootstrap` as the default.
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            inner: FlussSourceFactory,
        }
    }
}

#[async_trait::async_trait]
impl SourceFactory for BootstrapSourceFactory {
    async fn create(
        &self,
        name: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        self.inner
            .create(name, &with_bootstrap(options, &self.bootstrap))
            .await
    }
}

/// Sink factory applying a default `bootstrap` to Fluss DDL options.
pub struct BootstrapSinkFactory {
    bootstrap: String,
    inner: FlussSinkFactory,
}

impl BootstrapSinkFactory {
    /// Wrap the Fluss sink factory with `bootstrap` as the default.
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            inner: FlussSinkFactory,
        }
    }
}

#[async_trait::async_trait]
impl SinkFactory for BootstrapSinkFactory {
    async fn create(
        &self,
        name: &str,
        options: &BTreeMap<String, String>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        self.inner
            .create(name, &with_bootstrap(options, &self.bootstrap), schema)
            .await
    }
}

/// Clone `options`, adding `bootstrap` when the DDL did not set one.
fn with_bootstrap(options: &BTreeMap<String, String>, bootstrap: &str) -> BTreeMap<String, String> {
    let mut filled = options.clone();
    filled
        .entry("bootstrap".to_string())
        .or_insert_with(|| bootstrap.to_string());
    filled
}
