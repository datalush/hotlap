//! DDL statement handlers for [`SqlSession`](super::SqlSession).

use std::sync::Arc;

use hotlap::InputId;
use hotlap_connectors::datafusion::provider::SourceTableProvider;
use hotlap_connectors::runtime::pipeline::Watermark;
use hotlap_connectors::source::Source;

use super::{QueryResult, SinkDef, SqlSession, to_engine};
use crate::catalog::{MvDef, SourceDef};
use crate::ddl::{CreateSink, CreateSource, CreateView};
use crate::error::SqlError;
use crate::mv_schema::mv_schema;
use crate::translate::to_kernel_plan;
use crate::tumble::normalize_tumble_intervals;

impl SqlSession {
    pub(super) async fn create_source(
        &mut self,
        cs: CreateSource,
    ) -> Result<QueryResult, SqlError> {
        self.reject_after_start("CREATE SOURCE")?;
        // v1 owns a single source; a second declaration would overwrite the live
        // source/watermark while the first name stays registered.
        if self.source.is_some() {
            return Err(SqlError::Unsupported(
                "only one source is supported in v1".into(),
            ));
        }
        let source: Arc<dyn Source> = self.factory.create(&cs.name, &cs.options).await?.into();
        crate::watermark::column_index(&source.schema(), &cs.time_col)?;
        let connector = cs.options.get("connector").cloned().unwrap_or_default();
        self.catalog.add_source(&cs.name, SourceDef { connector })?;
        self.ctx
            .register_table(
                &cs.name,
                Arc::new(SourceTableProvider::new(Arc::clone(&source))),
            )
            .map_err(to_engine)?;
        self.source = Some(source);
        self.source_name = Some(cs.name);
        self.watermark = Some(Watermark { lag: cs.lag_ms });
        Ok(QueryResult::Ack("CREATE SOURCE".into()))
    }

    pub(super) async fn create_view(&mut self, cv: CreateView) -> Result<QueryResult, SqlError> {
        self.reject_after_start("CREATE MATERIALIZED VIEW")?;
        let source_schema = self
            .source
            .as_ref()
            .ok_or_else(|| SqlError::Catalog("view declared before its source".into()))?
            .schema();
        let query = normalize_tumble_intervals(&cv.query)?;
        let df = self.ctx.sql(&query).await.map_err(to_engine)?;
        let plan = to_kernel_plan(df.logical_plan(), InputId(0))?;
        let schema = mv_schema(&plan, source_schema.as_ref())?;
        // Reject unrepresentable output types here so the view fails at DDL
        // time rather than later, when a `SELECT` reads the consolidated rows.
        crate::convert::ensure_kernel_types(&schema)?;
        // Register after validation; a rejected view must not poison its name.
        self.catalog.add_view(
            &cv.name,
            MvDef {
                query: cv.query.clone(),
            },
        )?;
        self.mv_schemas.insert(cv.name.clone(), schema);
        self.views.push((cv.name, plan));
        Ok(QueryResult::Ack("CREATE MATERIALIZED VIEW".into()))
    }

    /// Register a sink to be opened against its view at `START`.
    pub(super) async fn create_sink(&mut self, cs: CreateSink) -> Result<QueryResult, SqlError> {
        self.reject_after_start("CREATE SINK")?;
        if self.catalog.view(&cs.view).is_none() {
            return Err(SqlError::Catalog(format!("unknown view: {}", cs.view)));
        }
        if self.sinks.iter().any(|s| s.name == cs.name) {
            return Err(SqlError::Catalog(format!(
                "sink already exists: {}",
                cs.name
            )));
        }
        // A view's changelog is drained per `take_changes`, so a second sink on
        // the same view would silently receive nothing; reject it in v1.
        if let Some(existing) = self.sinks.iter().find(|s| s.view == cs.view) {
            return Err(SqlError::Unsupported(format!(
                "view `{}` already has sink `{}`; v1 supports one sink per view",
                cs.view, existing.name
            )));
        }
        // The sink is opened at START so the factory can be async and the view
        // schema is already validated.
        self.sinks.push(SinkDef {
            name: cs.name,
            options: cs.options,
            view: cs.view,
        });
        Ok(QueryResult::Ack("CREATE SINK".into()))
    }
}
