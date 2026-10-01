// SPDX-License-Identifier: Apache-2.0
//! A bounded SQL scan of the current rows in a non-partitioned KV table.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::streaming::StreamingTableExec;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss::record::to_arrow_schema;

use crate::execution::FlussScanExec;
use crate::kv_scan::KvScanSpec;
use crate::log_table::bucket_groups;

/// Each execution opens a fresh snapshot per bucket. There is no global
/// cross-bucket transaction or shared snapshot across repeated SQL queries.
pub struct FlussKvTable {
    connection: Arc<FlussConnection>,
    path: TablePath,
    schema: SchemaRef,
    table_id: i64,
    buckets: i32,
    timeout: Duration,
    max_partitions: Option<usize>,
}

impl FlussKvTable {
    /// Validate the table and capture its schema without fetching any rows.
    pub async fn open(
        connection: Arc<FlussConnection>,
        path: TablePath,
        timeout: Duration,
    ) -> Result<Self> {
        if timeout.is_zero() {
            return Err(DataFusionError::Plan(
                "Fluss scan timeout must be positive".into(),
            ));
        }
        let table = connection.get_table(&path).await.map_err(fluss_error)?;
        let info = table.get_table_info();
        if !info.has_primary_key() || info.is_partitioned() {
            return Err(DataFusionError::Plan(
                "Only non-partitioned Fluss primary-key tables are supported by the KV provider"
                    .into(),
            ));
        }
        let schema = to_arrow_schema(info.get_row_type()).map_err(fluss_error)?;
        let table_id = info.table_id;
        let buckets = info.get_num_buckets();
        if buckets < 1 {
            return Err(DataFusionError::Plan("Fluss table has no buckets".into()));
        }
        drop(table);
        Ok(Self {
            connection,
            path,
            schema,
            table_id,
            buckets,
            timeout,
            max_partitions: None,
        })
    }

    /// Cap physical partitions; the default uses DataFusion's target partitions.
    pub fn with_max_partitions(mut self, max_partitions: usize) -> Result<Self> {
        if max_partitions == 0 {
            return Err(DataFusionError::Plan(
                "Fluss max partitions must be positive".into(),
            ));
        }
        self.max_partitions = Some(max_partitions);
        Ok(self)
    }
}

#[async_trait]
impl TableProvider for FlussKvTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The snapshot RPC offers no filter pushdown. DataFusion applies exact
        // filters and global limits after reading each live row.
        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        let target = state.config_options().execution.target_partitions.max(1);
        let parallelism = self.max_partitions.map_or(target, |max| target.min(max));
        let groups = bucket_groups(self.buckets, parallelism);
        let metrics = ExecutionPlanMetricsSet::new();
        let spec = Arc::new(KvScanSpec {
            connection: Arc::clone(&self.connection),
            path: self.path.clone(),
            schema: Arc::clone(&schema),
            full_schema: Arc::clone(&self.schema),
            projection: projection.cloned(),
            table_id: self.table_id,
            buckets: self.buckets,
            timeout: self.timeout,
            metrics: metrics.clone(),
        });
        let inner =
            StreamingTableExec::try_new(schema, spec.partitions(&groups), None, [], false, None)?;
        Ok(Arc::new(FlussScanExec::new(inner, metrics, groups)))
    }
}

impl fmt::Debug for FlussKvTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussKvTable")
            .field("path", &self.path)
            .finish()
    }
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
