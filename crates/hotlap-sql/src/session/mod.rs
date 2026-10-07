//! Embedded SQL session: DDL, START and materialized-view queries.

mod runtime;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use hotlap::{InputId, Plan, Row};
use hotlap_connectors::datafusion::provider::SourceTableProvider;
use hotlap_connectors::runtime::handle::{EngineHandle, SnapshotHandle};
use hotlap_connectors::runtime::pipeline::Watermark;
use hotlap_connectors::source::Source;

use crate::catalog::{Catalog, MvDef, SourceDef};
use crate::ddl::{self, CreateSource, CreateView, Statement};
use crate::error::SqlError;
use crate::mv_provider::MvTableProvider;
use crate::mv_schema::mv_schema;
use crate::session_source::SharedSource;
use crate::translate::{register_tumble_udf, to_kernel_plan};
use crate::tumble::normalize_tumble_intervals;

/// Builds engine sources from `CREATE SOURCE` options.
pub trait SourceFactory: Send + Sync {
    /// Create the source named `name` from its DDL `options`.
    fn create(
        &self,
        name: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError>;
}

/// Reads consolidated view output for the MV table providers.
pub trait Snapshotter: Send + Sync {
    /// Return the current rows of `view`.
    fn snapshot(&self, view: &str) -> Result<Vec<Row>, SqlError>;
}

/// Outcome of a statement: an acknowledgement or a result set.
pub enum QueryResult {
    /// A DDL/START statement was accepted.
    Ack(String),
    /// A query returned record batches.
    Rows(Vec<RecordBatch>),
}

impl Snapshotter for SnapshotHandle {
    fn snapshot(&self, view: &str) -> Result<Vec<Row>, SqlError> {
        // Before the first push the kernel has no built dataflow, so a declared
        // view reads as empty rather than erroring. `is_built` is set by the
        // engine after the first successful push, avoiding any dependence on
        // the kernel's error text.
        if !self.is_built() {
            return Ok(Vec::new());
        }
        SnapshotHandle::snapshot(self, view).map_err(to_engine)
    }
}

/// A single embedded session over one source and its materialized views.
pub struct SqlSession {
    ctx: SessionContext,
    catalog: Catalog,
    factory: Arc<dyn SourceFactory>,
    source: Option<Arc<dyn Source>>,
    source_name: Option<String>,
    watermark: Option<Watermark>,
    views: Vec<(String, Plan)>,
    mv_schemas: HashMap<String, SchemaRef>,
    engine: Option<EngineHandle>,
    started: bool,
}

impl SqlSession {
    /// Open an empty session with the planning-only `tumble` function registered.
    pub fn open(factory: Arc<dyn SourceFactory>) -> Self {
        let ctx = SessionContext::new();
        register_tumble_udf(&ctx);
        Self {
            ctx,
            catalog: Catalog::default(),
            factory,
            source: None,
            source_name: None,
            watermark: None,
            views: Vec::new(),
            mv_schemas: HashMap::new(),
            engine: None,
            started: false,
        }
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
            Statement::CreateSource(cs) => self.create_source(cs),
            Statement::CreateView(cv) => self.create_view(cv).await,
            Statement::Start => self.start().await,
        }
    }

    fn create_source(&mut self, cs: CreateSource) -> Result<QueryResult, SqlError> {
        self.reject_after_start("CREATE SOURCE")?;
        let source: Arc<dyn Source> = self.factory.create(&cs.name, &cs.options)?.into();
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

    async fn create_view(&mut self, cv: CreateView) -> Result<QueryResult, SqlError> {
        self.reject_after_start("CREATE MATERIALIZED VIEW")?;
        let source_schema = self
            .source
            .as_ref()
            .ok_or_else(|| SqlError::Catalog("view declared before its source".into()))?
            .schema();
        let query = normalize_tumble_intervals(&cv.query)?;
        let df = self.ctx.sql(&query).await.map_err(to_engine)?;
        let plan = to_kernel_plan(df.logical_plan(), InputId(0))?;
        self.catalog.add_view(
            &cv.name,
            MvDef {
                query: cv.query.clone(),
            },
        )?;
        self.mv_schemas
            .insert(cv.name.clone(), mv_schema(&plan, source_schema.as_ref())?);
        self.views.push((cv.name, plan));
        Ok(QueryResult::Ack("CREATE MATERIALIZED VIEW".into()))
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
