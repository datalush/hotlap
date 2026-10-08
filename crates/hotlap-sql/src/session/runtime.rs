//! Engine lifecycle for [`SqlSession`](super::SqlSession).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use hotlap::Plan;
use hotlap_runtime::runtime::handle::EngineHandle;
use hotlap_runtime::runtime::pipeline::{Pipeline, SinkSpec};

use super::{
    MvTableProvider, QueryResult, SharedSource, Snapshotter, SqlError, SqlSession, to_engine,
};
use crate::catalog::MvDef;

impl SqlSession {
    /// Start the engine with the declared source and views.
    ///
    /// Runs the blocking engine startup on a worker thread, since the engine
    /// handle must not block an async executor.
    pub async fn start(&mut self) -> Result<QueryResult, SqlError> {
        if self.started {
            return Err(SqlError::Unsupported("session already started".into()));
        }
        let pipeline = self.build_pipeline().await?;
        let engine = tokio::task::spawn_blocking(move || EngineHandle::start(pipeline))
            .await
            .map_err(to_engine)?
            .map_err(to_engine)?;
        self.register_mv_providers(&engine)?;
        self.engine = Some(engine);
        self.started = true;
        Ok(QueryResult::Ack("START".into()))
    }

    /// Stop the engine and join its worker thread.
    pub async fn shutdown(mut self) -> Result<(), SqlError> {
        if let Some(engine) = self.engine.take() {
            tokio::task::spawn_blocking(move || engine.shutdown())
                .await
                .map_err(to_engine)?
                .map_err(to_engine)?;
        }
        Ok(())
    }

    /// Build the pipeline, opening each declared sink against its view schema.
    async fn build_pipeline(&self) -> Result<Pipeline, SqlError> {
        let name = self
            .source_name
            .clone()
            .ok_or_else(|| SqlError::Catalog("START before CREATE SOURCE".into()))?;
        let source = self
            .source
            .clone()
            .ok_or_else(|| SqlError::Catalog("START before CREATE SOURCE".into()))?;
        let sinks = self.build_sinks().await?;
        Ok(Pipeline {
            input: name,
            source: Box::new(SharedSource(source)),
            watermark: self.watermark,
            views: self.views.clone(),
            sinks,
            checkpoint: None,
            retention: self.retention,
        })
    }

    /// Open every declared sink via the factory, resolving its view schema.
    async fn build_sinks(&self) -> Result<Vec<SinkSpec>, SqlError> {
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

    /// Build a view on the running engine and expose it as an MV table.
    ///
    /// The engine replays retained inputs (or rejects when retention does not
    /// cover the run), then the view is registered so `SELECT` can read it.
    pub(super) async fn build_view_late(
        &mut self,
        name: &str,
        plan: Plan,
        schema: SchemaRef,
        query: String,
    ) -> Result<(), SqlError> {
        if self.catalog.view(name).is_some() {
            return Err(SqlError::Catalog(format!("view already exists: {name}")));
        }
        // Gate on the session's own config: the engine's frozen flag is a race
        // with source ingestion, so it cannot decide this deterministically.
        if self.retention.is_none() {
            return Err(SqlError::Unsupported(
                "CREATE MATERIALIZED VIEW after START requires input retention".into(),
            ));
        }
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| SqlError::Unsupported("CREATE MATERIALIZED VIEW after START".into()))?;
        let handle = engine.snapshot_handle();
        let view = name.to_string();
        tokio::task::spawn_blocking(move || handle.build_view(&view, plan))
            .await
            .map_err(to_engine)?
            .map_err(to_engine)?;
        let snapshotter: Arc<dyn Snapshotter> = Arc::new(engine.snapshot_handle());
        let provider = MvTableProvider::new(name.to_string(), schema.clone(), snapshotter);
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(to_engine)?;
        self.catalog.add_view(name, MvDef { query })?;
        self.mv_schemas.insert(name.to_string(), schema);
        Ok(())
    }

    fn register_mv_providers(&self, engine: &EngineHandle) -> Result<(), SqlError> {
        let snapshotter: Arc<dyn Snapshotter> = Arc::new(engine.snapshot_handle());
        for (name, schema) in &self.mv_schemas {
            let provider =
                MvTableProvider::new(name.clone(), schema.clone(), Arc::clone(&snapshotter));
            self.ctx
                .register_table(name, Arc::new(provider))
                .map_err(to_engine)?;
        }
        Ok(())
    }
}
