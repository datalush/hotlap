// SPDX-License-Identifier: Apache-2.0
//! Validate an append-only table and plan its parallel bounded scan.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::streaming::StreamingTableExec;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;

use crate::execution::FlussScanExec;
use crate::filter;
use crate::offsets::OffsetCaptures;
use crate::scan::ScanSpec;

/// Append-only log table with bounded, parallel physical scan partitions.
pub struct FlussLogTable {
    connection: Arc<FlussConnection>,
    path: TablePath,
    schema: SchemaRef,
    table_id: i64,
    buckets: i32,
    timeout: Duration,
    max_partitions: Option<usize>,
}

impl FlussLogTable {
    /// Validate the table and capture its schema. No rows are fetched yet.
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
        if info.has_primary_key() || info.is_partitioned() {
            return Err(DataFusionError::Plan(
                "Only non-partitioned append-only Fluss log tables are supported".into(),
            ));
        }
        let schema = table
            .new_scan()
            .create_record_batch_log_scanner()
            .map_err(fluss_error)?
            .schema();
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

    fn projected_schema(&self, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
        match projection {
            Some(indices) => Ok(Arc::new(self.schema.project(indices)?)),
            None => Ok(Arc::clone(&self.schema)),
        }
    }

    fn parallelism(&self, state: &dyn Session) -> usize {
        let target = state.config_options().execution.target_partitions.max(1);
        self.max_partitions.map_or(target, |max| target.min(max))
    }
}

#[async_trait]
impl TableProvider for FlussLogTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if filter::translate(filter, &self.schema).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Fluss prunes whole batches; DataFusion retains exact filters and
        // global limits. Do not push a SQL limit into each bucket.
        let schema = self.projected_schema(projection)?;
        let groups = bucket_groups(self.buckets, self.parallelism(state));
        let metrics = ExecutionPlanMetricsSet::new();
        let spec = Arc::new(ScanSpec {
            connection: Arc::clone(&self.connection),
            path: self.path.clone(),
            schema: Arc::clone(&schema),
            full_schema: Arc::clone(&self.schema),
            projection: projection.cloned(),
            filter: fluss::predicate::Predicate::and_all(
                filters
                    .iter()
                    .filter_map(|expr| filter::translate(expr, &self.schema)),
            ),
            table_id: self.table_id,
            buckets: self.buckets,
            timeout: self.timeout,
            captures: Arc::new(OffsetCaptures::default()),
            metrics: metrics.clone(),
        });
        let inner =
            StreamingTableExec::try_new(schema, spec.partitions(&groups), None, [], false, None)?;
        Ok(Arc::new(FlussScanExec::new(inner, metrics, groups)))
    }
}

impl fmt::Debug for FlussLogTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussLogTable")
            .field("path", &self.path)
            .finish()
    }
}

impl fmt::Display for FlussLogTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlussLogTable({})", self.path)
    }
}

fn bucket_groups(buckets: i32, parallelism: usize) -> Vec<Vec<i32>> {
    let count = (buckets as usize).min(parallelism.max(1));
    let mut groups = vec![Vec::new(); count];
    for bucket in 0..buckets {
        groups[bucket as usize % count].push(bucket);
    }
    groups
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::bucket_groups;

    #[test]
    fn grouping_covers_each_bucket_once_at_requested_parallelism() {
        let groups = bucket_groups(19, 8);
        assert_eq!(groups.len(), 8);
        let mut buckets: Vec<_> = groups.into_iter().flatten().collect();
        buckets.sort_unstable();
        assert_eq!(buckets, (0..19).collect::<Vec<_>>());
        assert_eq!(bucket_groups(2, 1), vec![vec![0, 1]]);
        assert_eq!(bucket_groups(2, 20), vec![vec![0], vec![1]]);
    }
}
