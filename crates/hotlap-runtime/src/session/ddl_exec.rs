//! DDL statement handlers for [`SqlSession`](super::SqlSession).

use std::sync::Arc;

use datafusion::common::TableReference;
use hotlap_connectors::datafusion::provider::SourceTableProvider;
use hotlap_connectors::source::Source;
use hotlap_sql::bindings::canonical_relation;
use hotlap_sql::catalog::{MvDef, SourceDef};
use hotlap_sql::ddl::{CreateSink, CreateSource, CreateView};
use hotlap_sql::error::SqlError;
use hotlap_sql::mv_schema::mv_schema;
use hotlap_sql::translate::to_kernel_plan;
use hotlap_sql::tumble::normalize_tumble_intervals;

use super::sources::SessionSource;
use super::view_plans::ViewPlan;
use super::{QueryResult, SinkDef, SqlSession, to_engine};
use crate::runtime::pipeline::Watermark;

impl SqlSession {
    pub(super) async fn create_source(
        &mut self,
        cs: CreateSource,
    ) -> Result<QueryResult, SqlError> {
        self.reject_after_start("CREATE SOURCE")?;
        // Bind by the name DataFusion reports in a plan, so `Src` and `"Src"`
        // land on the same key the translator will look up.
        let name = canonical_relation(&cs.name);
        if self.sources.contains_key(&name) {
            return Err(SqlError::Catalog(format!("source already exists: {name}")));
        }
        let source: Arc<dyn Source> = self.factory.create(&cs.name, &cs.options).await?.into();
        // The declared watermark column must exist before any state is published.
        hotlap_sql::watermark::column_index(&source.schema(), &cs.time_col)?;
        let connector = cs.options.get("connector").cloned().unwrap_or_default();
        self.catalog.add_source(&name, SourceDef { connector })?;
        self.sources.insert(
            name.clone(),
            SessionSource {
                source: Arc::clone(&source),
                watermark: Some(Watermark { lag: cs.lag_ms }),
            },
        );
        // Register the DataFusion table last: a failure undoes only this
        // operation and never leaves a live provider without its registry.
        // `bare` keeps the canonical name verbatim (case and literal dots),
        // unlike a `&str`, which would reparse it as a (schema.)table path.
        let provider = Arc::new(SourceTableProvider::new(source));
        let table = TableReference::bare(name.clone());
        if let Err(error) = self.ctx.register_table(table, provider) {
            self.sources.remove(&name);
            self.catalog.remove_source(&name);
            return Err(to_engine(error));
        }
        Ok(QueryResult::Ack("CREATE SOURCE".into()))
    }

    pub(super) async fn create_view(&mut self, cv: CreateView) -> Result<QueryResult, SqlError> {
        // The frozen map after START, or the current one while declaring.
        let bindings = self.active_bindings()?;
        let schemas = self.input_schemas(&bindings)?;
        let query = normalize_tumble_intervals(&cv.query)?;
        let df = self.ctx.sql(&query).await.map_err(to_engine)?;
        let logical = df.logical_plan().clone();
        let plan = to_kernel_plan(&logical, &bindings)?;
        let schema = mv_schema(&plan, &schemas)?;
        // Reject unrepresentable output types here so the view fails at DDL
        // time rather than later, when a `SELECT` reads the consolidated rows.
        hotlap_sql::convert::ensure_kernel_types(&schema)?;
        if self.started {
            // Post-start: the engine replays retained inputs, or rejects when
            // retention does not cover the run.
            self.build_view_late(&cv.name, plan, schema, cv.query)
                .await?;
        } else {
            // Register after validation; a rejected view must not poison its
            // name, and the logical plan is kept for the START recompilation.
            self.catalog.add_view(
                &cv.name,
                MvDef {
                    query: cv.query.clone(),
                },
            )?;
            self.mv_schemas.insert(cv.name.clone(), schema);
            self.view_plans.push(ViewPlan {
                name: cv.name,
                logical,
            });
        }
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
