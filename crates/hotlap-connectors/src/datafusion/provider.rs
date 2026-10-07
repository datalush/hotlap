//! Expose an engine `Source` as a DataFusion `TableProvider`.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;

use crate::error::ConnectorError;
use crate::source::{Source, Split};

/// A `TableProvider` backed by an engine `Source`.
pub struct SourceTableProvider {
    source: Arc<dyn Source>,
}

impl SourceTableProvider {
    /// Wrap `source` as a DataFusion table.
    pub fn new(source: Arc<dyn Source>) -> Self {
        Self { source }
    }
}

impl fmt::Debug for SourceTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceTableProvider").finish()
    }
}

#[async_trait]
impl TableProvider for SourceTableProvider {
    fn schema(&self) -> SchemaRef {
        self.source.schema()
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
        let full = self.source.schema();
        let schema = match projection {
            Some(indices) => Arc::new(full.project(indices)?),
            None => full,
        };
        let partitions: Vec<Arc<dyn PartitionStream>> = self
            .source
            .splits()
            .map_err(to_df)?
            .into_iter()
            .map(|split| {
                Arc::new(SourcePartition {
                    source: Arc::clone(&self.source),
                    split,
                    schema: schema.clone(),
                    projection: projection.cloned(),
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        let exec = StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            [],
            self.source.is_unbounded(),
            None,
        )?;
        Ok(Arc::new(exec))
    }
}

struct SourcePartition {
    source: Arc<dyn Source>,
    split: Split,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl fmt::Debug for SourcePartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourcePartition")
            .field("split", &self.split)
            .finish()
    }
}

impl PartitionStream for SourcePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let batches = match self.source.read(&self.split) {
            Ok(stream) => stream
                .map(move |r| project_result(r, projection.as_deref()))
                .boxed(),
            Err(e) => {
                let error = to_df(e);
                futures::stream::once(async move { Err(error) }).boxed()
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

fn project_result(
    result: Result<crate::source::SourceBatch, ConnectorError>,
    projection: Option<&[usize]>,
) -> Result<arrow::record_batch::RecordBatch> {
    let batch = result.map_err(to_df)?;
    match projection {
        Some(indices) => Ok(batch.batch.project(indices)?),
        None => Ok(batch.batch),
    }
}

fn to_df(e: ConnectorError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
