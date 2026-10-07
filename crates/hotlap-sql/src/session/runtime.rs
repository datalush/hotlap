//! Engine lifecycle for [`SqlSession`](super::SqlSession).

use std::sync::Arc;

use hotlap_connectors::runtime::handle::EngineHandle;
use hotlap_connectors::runtime::pipeline::Pipeline;

use super::{
    MvTableProvider, QueryResult, SharedSource, Snapshotter, SqlError, SqlSession, to_engine,
};

impl SqlSession {
    /// Start the engine with the declared source and views.
    ///
    /// Runs the blocking engine startup on a worker thread, since the engine
    /// handle must not block an async executor.
    pub async fn start(&mut self) -> Result<QueryResult, SqlError> {
        if self.started {
            return Err(SqlError::Unsupported("session already started".into()));
        }
        let pipeline = self.build_pipeline()?;
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

    fn build_pipeline(&self) -> Result<Pipeline, SqlError> {
        let name = self
            .source_name
            .clone()
            .ok_or_else(|| SqlError::Catalog("START before CREATE SOURCE".into()))?;
        let source = self
            .source
            .clone()
            .ok_or_else(|| SqlError::Catalog("START before CREATE SOURCE".into()))?;
        Ok(Pipeline {
            input: name,
            source: Box::new(SharedSource(source)),
            watermark: self.watermark,
            views: self.views.clone(),
        })
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
