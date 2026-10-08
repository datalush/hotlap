//! DataFusion table provider that reads a materialized view from the engine.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::convert::zset_to_batch;
use crate::session::Snapshotter;

/// Exposes one engine view as a one-shot, in-memory DataFusion table.
pub struct MvTableProvider {
    name: String,
    schema: SchemaRef,
    snapshotter: Arc<dyn Snapshotter>,
}

impl MvTableProvider {
    /// Build a provider that reads view `name` with Arrow `schema`.
    pub fn new(name: String, schema: SchemaRef, snapshotter: Arc<dyn Snapshotter>) -> Self {
        Self {
            name,
            schema,
            snapshotter,
        }
    }
}

impl std::fmt::Debug for MvTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvTableProvider")
            .field("name", &self.name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for MvTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = snapshot_batch(
            Arc::clone(&self.snapshotter),
            self.name.clone(),
            self.schema.clone(),
        )
        .await?;
        let exec = MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            self.schema.clone(),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// Read the view snapshot on a blocking thread and convert it to one batch.
///
/// The engine handle blocks its caller, which must not be an async worker.
async fn snapshot_batch(
    snapshotter: Arc<dyn Snapshotter>,
    name: String,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let zset = tokio::task::spawn_blocking(move || snapshotter.snapshot(&name))
        .await
        .map_err(external)?
        .map_err(external)?;
    zset_to_batch(&schema, &zset).map_err(external)
}

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
