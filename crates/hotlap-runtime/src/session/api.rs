//! Embedded [`Session`]: the public, synchronous API over the SQL session.

use std::collections::BTreeMap;

use hotlap::ZSetBatch;

use crate::error::SessionError;
use crate::runtime::checkpoint::CheckpointConfig;
use crate::session::{QueryResult, SqlSession};
use crate::session_config::SessionConfig;

/// A point-in-time copy of the engine metrics, ordered by name.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    /// Metric name to value.
    pub entries: BTreeMap<String, u64>,
}

/// A synchronous embedded session: DDL, `START`, queries, metrics and
/// checkpoints over one source and its materialized views.
///
/// Owns a multi-threaded Tokio runtime, so callers need not be inside one.
/// `enable_all` provides the IO and time drivers the Fluss connectors need
/// (`TcpStream::connect`, retry timers), and the worker thread keeps driving the
/// connector tasks spawned during `CREATE SOURCE` while the engine thread reads
/// from the source on its own runtime.
pub struct Session {
    sql: SqlSession,
    runtime: tokio::runtime::Runtime,
}

impl Session {
    /// Open a session from `config`; the engine starts on `START`.
    pub fn open(config: SessionConfig) -> Result<Self, SessionError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| SessionError::Engine(error.to_string()))?;
        let (source_factory, sink_factory, checkpoint) = config.into_parts();
        let mut sql = SqlSession::open_with_factories(source_factory, sink_factory);
        if let Some(spec) = checkpoint {
            sql = sql.with_checkpoint(CheckpointConfig {
                interval: spec.interval,
                backend: spec.backend,
                retain: spec.retain,
            });
        }
        Ok(Self { sql, runtime })
    }

    /// Execute one statement: DDL, `START`, or a query.
    pub fn sql(&mut self, sql: &str) -> Result<QueryResult, SessionError> {
        self.runtime.block_on(self.sql.sql(sql)).map_err(Into::into)
    }

    /// Start the engine with the declared source, views and sinks.
    pub fn start(&mut self) -> Result<(), SessionError> {
        self.runtime.block_on(self.sql.start())?;
        Ok(())
    }

    /// Read the consolidated output of `view`.
    pub fn snapshot(&self, view: &str) -> Result<ZSetBatch, SessionError> {
        self.sql.snapshot(view).map_err(Into::into)
    }

    /// A point-in-time copy of the engine metrics.
    pub fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            entries: self.sql.metrics(),
        }
    }

    /// Take a checkpoint now and return its id.
    pub fn checkpoint(&self) -> Result<u64, SessionError> {
        self.sql.checkpoint().map_err(Into::into)
    }

    /// Stop the engine and join its worker thread.
    pub fn shutdown(self) -> Result<(), SessionError> {
        let Self { sql, runtime } = self;
        runtime.block_on(sql.shutdown())?;
        Ok(())
    }
}
