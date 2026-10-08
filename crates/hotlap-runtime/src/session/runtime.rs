//! Engine lifecycle for [`SqlSession`](super::SqlSession).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use hotlap::{InputId, Plan, ZSetBatch};
use hotlap_sql::catalog::MvDef;

use super::mv_provider::MvTableProvider;
use super::{QueryResult, Snapshotter, SqlError, SqlSession, to_engine};
use crate::runtime::handle::EngineHandle;
use crate::runtime::pipeline::{Pipeline, SinkSpec};
use crate::runtime::sources::{InputSource, Sources};

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

    /// Read the consolidated output of `view` from the running engine.
    ///
    /// Routed through the same [`Snapshotter`] guard as the MV table providers,
    /// so a declared view reads as empty before the dataflow is built instead of
    /// erroring from the kernel.
    pub fn snapshot(&self, view: &str) -> Result<ZSetBatch, SqlError> {
        let engine = self.engine.as_ref().ok_or_else(not_started)?;
        Snapshotter::snapshot(&engine.snapshot_handle(), view)
    }

    /// Take a checkpoint now and return its id.
    pub fn checkpoint(&self) -> Result<u64, SqlError> {
        let engine = self.engine.as_ref().ok_or_else(not_started)?;
        engine.checkpoint().map_err(to_engine)
    }

    /// A point-in-time copy of the engine metrics; empty before `START`.
    pub fn metrics(&self) -> BTreeMap<String, u64> {
        self.engine
            .as_ref()
            .map(|engine| engine.metrics().snapshot())
            .unwrap_or_default()
    }

    /// Build the pipeline, opening each declared sink against its view schema.
    ///
    /// Until the multi-source session lands (T5), the session declares a single
    /// input with the stable id `InputId(0)`; the pipeline already consumes the
    /// generic [`Sources`] set, so no mono-source runtime path is kept.
    async fn build_pipeline(&mut self) -> Result<Pipeline, SqlError> {
        let name = self
            .source_name
            .clone()
            .ok_or_else(|| SqlError::Catalog("START before CREATE SOURCE".into()))?;
        let source = self
            .source
            .clone()
            .ok_or_else(|| SqlError::Catalog("START before CREATE SOURCE".into()))?;
        let sinks = self.build_sinks().await?;
        let sources = Sources::new(vec![InputSource {
            id: InputId(0),
            name,
            source,
            watermark: self.watermark,
        }])
        .map_err(to_engine)?;
        Ok(Pipeline {
            sources,
            views: self.views.clone(),
            sinks,
            checkpoint: self.checkpoint.take(),
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

fn not_started() -> SqlError {
    SqlError::Engine("session not started".into())
}
