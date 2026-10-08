//! Embedded SQL session: DDL, START and materialized-view queries.

mod api;
mod ddl_exec;
mod fluss_factory;
mod mv_provider;
mod runtime;
mod sink_factory;
mod sources;
mod view_plans;

pub use api::{MetricsSnapshot, Session};
pub use fluss_factory::FlussSourceFactory;
pub use sink_factory::{FlussSinkFactory, SinkFactory};

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use hotlap::ZSetBatch;
use hotlap_connectors::source::Source;
use hotlap_sql::bindings::SourceBindings;
use hotlap_sql::catalog::Catalog;
use hotlap_sql::ddl::{self, Statement};
use hotlap_sql::error::SqlError;
use hotlap_sql::translate::register_tumble_udf;

use crate::runtime::SnapshotHandle;
use crate::runtime::checkpoint::CheckpointConfig;
use crate::runtime::handle::EngineHandle;

use sources::SessionSource;
use view_plans::ViewPlan;

/// Builds engine sources from `CREATE SOURCE` options.
#[async_trait::async_trait]
pub trait SourceFactory: Send + Sync {
    /// Create the source named `name` from its DDL `options`.
    async fn create(
        &self,
        name: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError>;
}

/// Reads consolidated view output for the MV table providers.
pub trait Snapshotter: Send + Sync {
    /// Return the current consolidated Z-set of `view`.
    fn snapshot(&self, view: &str) -> Result<ZSetBatch, SqlError>;
}

/// Outcome of a statement: an acknowledgement or a result set.
pub enum QueryResult {
    /// A DDL/START statement was accepted.
    Ack(String),
    /// A query returned record batches.
    Rows(Vec<RecordBatch>),
}

impl Snapshotter for SnapshotHandle {
    fn snapshot(&self, view: &str) -> Result<ZSetBatch, SqlError> {
        // Before the first push the kernel has no built dataflow, so a declared
        // view reads as empty rather than erroring. `is_built` is set by the
        // engine after the first successful push, avoiding any dependence on
        // the kernel's error text.
        if !self.is_built() {
            return Ok(ZSetBatch::empty(Arc::new(Schema::empty())));
        }
        SnapshotHandle::snapshot(self, view).map_err(to_engine)
    }
}

/// A declared sink, opened against its view when the engine starts.
struct SinkDef {
    name: String,
    options: BTreeMap<String, String>,
    view: String,
}

/// A single embedded session over one or more sources and its materialized
/// views.
pub struct SqlSession {
    ctx: SessionContext,
    catalog: Catalog,
    factory: Arc<dyn SourceFactory>,
    sink_factory: Arc<dyn SinkFactory>,
    /// Declared sources keyed by canonical relation name.
    sources: BTreeMap<String, SessionSource>,
    /// Views declared before `START`, kept as logical plans for recompilation.
    view_plans: Vec<ViewPlan>,
    mv_schemas: HashMap<String, SchemaRef>,
    sinks: Vec<SinkDef>,
    engine: Option<EngineHandle>,
    started: bool,
    /// Input id assignment frozen at `START`.
    frozen: Option<SourceBindings>,
    /// Input deltas to retain for views created after `START`; `None` disables
    /// retention, so those views are rejected.
    retention: Option<usize>,
    /// Periodic checkpoint settings, moved into the engine at `START`.
    checkpoint: Option<CheckpointConfig>,
}

impl SqlSession {
    /// Open an empty session using the default Fluss factories.
    pub fn open() -> Self {
        Self::open_with_factories(Arc::new(FlussSourceFactory), Arc::new(FlussSinkFactory))
    }

    /// Open an empty session with caller-supplied source and sink factories.
    ///
    /// The `tumble` planning function is registered on the fresh context.
    pub fn open_with_factories(
        source_factory: Arc<dyn SourceFactory>,
        sink_factory: Arc<dyn SinkFactory>,
    ) -> Self {
        let ctx = SessionContext::new();
        register_tumble_udf(&ctx);
        Self {
            ctx,
            catalog: Catalog::default(),
            factory: source_factory,
            sink_factory,
            sources: BTreeMap::new(),
            view_plans: Vec::new(),
            mv_schemas: HashMap::new(),
            sinks: Vec::new(),
            engine: None,
            started: false,
            frozen: None,
            retention: None,
            checkpoint: None,
        }
    }

    /// Configure periodic checkpointing, moved into the engine at `START`.
    pub fn with_checkpoint(mut self, config: CheckpointConfig) -> Self {
        self.checkpoint = Some(config);
        self
    }

    /// Retain the last `events` input deltas so a materialized view can be
    /// created after `START`. Must be set before `START`; off by default.
    pub fn with_input_retention(mut self, events: usize) -> Self {
        self.retention = Some(events);
        self
    }

    /// Execute one statement: DDL, `START`, or an ad-hoc query.
    ///
    /// A statement that is not recognized as DDL is planned and run as a query;
    /// a malformed DDL statement surfaces as a parse error instead.
    pub async fn sql(&mut self, sql: &str) -> Result<QueryResult, SqlError> {
        match ddl::parse(sql) {
            Ok(stmt) => self.run_ddl(stmt).await,
            Err(error) if looks_like_ddl(sql) => Err(error),
            Err(_) => self.query(sql).await,
        }
    }

    async fn run_ddl(&mut self, stmt: Statement) -> Result<QueryResult, SqlError> {
        match stmt {
            Statement::CreateSource(cs) => self.create_source(cs).await,
            Statement::CreateView(cv) => self.create_view(cv).await,
            Statement::CreateSink(cs) => self.create_sink(cs).await,
            Statement::Start => self.start().await,
        }
    }

    async fn query(&mut self, sql: &str) -> Result<QueryResult, SqlError> {
        let df = self.ctx.sql(sql).await.map_err(to_engine)?;
        let batches = df.collect().await.map_err(to_engine)?;
        Ok(QueryResult::Rows(batches))
    }

    fn reject_after_start(&self, what: &str) -> Result<(), SqlError> {
        if self.started {
            return Err(SqlError::Unsupported(format!("{what} after START")));
        }
        Ok(())
    }
}

fn looks_like_ddl(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("CREATE ") || upper == "START" || upper == "START;"
}

fn to_engine(error: impl std::fmt::Display) -> SqlError {
    SqlError::Engine(error.to_string())
}
