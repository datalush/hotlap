// SPDX-License-Identifier: Apache-2.0
//! Validate an append-only table and plan a bounded or continuous scan.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::streaming::StreamingTableExec;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;

use crate::execution::FlussScanExec;
use crate::filter;
use crate::log_options::{LogReadMode, LogReadOptions};
use crate::log_progress::LogDelivery;
use crate::offsets::{OffsetWindow, SharedCaptures};
use crate::partitions::{DEFAULT_MAX_ASSIGNED_BUCKETS, PartitionFilter};
use crate::scan::{PartitionedOffsets, ScanSpec};
use crate::write::{FlussWriteOptions, FlussWriteTarget, WriteKind, plan_write};

/// Append-only log table with parallel batch or continuous physical partitions.
pub struct FlussLogTable {
    connection: Arc<FlussConnection>,
    path: TablePath,
    schema: SchemaRef,
    table_id: i64,
    schema_id: i32,
    initial_schema: bool,
    buckets: i32,
    partitioned: bool,
    partition_keys: Vec<String>,
    options: LogReadOptions,
    max_partitions: Option<usize>,
    max_assigned_buckets: usize,
    deliveries: tokio::sync::broadcast::Sender<LogDelivery>,
    write_options: FlussWriteOptions,
}

impl FlussLogTable {
    /// Support of this provider's selected read mode and append writes.
    /// This does not establish authorization or unchanged destination metadata.
    pub fn capabilities(&self) -> crate::FlussCapabilities {
        crate::FlussCapabilities {
            read: crate::FlussReadCapability::Log(self.options.mode),
            insert: crate::FlussInsertCapability::Append,
            delete: false,
            merge: false,
        }
    }

    /// Legacy bounded source. Use `open_with_options` to select streaming or
    /// an explicit start position; this constructor stays batch for existing
    /// consumers that expect `collect()` to finish.
    pub async fn open(
        connection: Arc<FlussConnection>,
        path: TablePath,
        timeout: Duration,
    ) -> Result<Self> {
        Self::open_with_options(connection, path, LogReadOptions::batch(timeout)).await
    }

    /// Validate the table and capture its schema. No rows are fetched yet.
    /// `LogReadOptions::default()` follows new log records indefinitely.
    pub async fn open_with_options(
        connection: Arc<FlussConnection>,
        path: TablePath,
        options: LogReadOptions,
    ) -> Result<Self> {
        if options.mode == LogReadMode::Batch && options.batch_timeout.is_zero() {
            return Err(DataFusionError::Plan(
                "Fluss scan timeout must be positive".into(),
            ));
        }
        let table = connection.get_table(&path).await.map_err(fluss_error)?;
        let info = table.get_table_info();
        if info.has_primary_key() {
            return Err(DataFusionError::Plan(
                "Only append-only Fluss log tables are supported".into(),
            ));
        }
        let schema = table
            .new_scan()
            .create_record_batch_log_scanner()
            .map_err(fluss_error)?
            .schema();
        let table_id = info.table_id;
        let schema_id = info.get_schema_id();
        // Older log batches may predate newly added columns. The server
        // rejects projection/filtering of those columns against old batches.
        // Version 1 is the initial schema in Fluss; later versions must
        // project locally after decoding a full row under the pinned schema.
        let initial_schema = info.get_schema_id() == 1;
        let buckets = info.get_num_buckets();
        let partitioned = info.is_partitioned();
        let partition_keys = info.get_partition_keys().iter().cloned().collect();
        if buckets < 1 {
            return Err(DataFusionError::Plan("Fluss table has no buckets".into()));
        }
        drop(table);
        Ok(Self {
            connection,
            path,
            schema,
            table_id,
            schema_id,
            initial_schema,
            buckets,
            partitioned,
            partition_keys,
            options,
            max_partitions: None,
            max_assigned_buckets: DEFAULT_MAX_ASSIGNED_BUCKETS,
            deliveries: tokio::sync::broadcast::channel(1024).0,
            write_options: FlussWriteOptions::default(),
        })
    }

    /// Subscribe before executing to observe source-side batches. A lagged
    /// receiver is an error for checkpoint consumers: do not skip events.
    /// Multiple concurrent queries share the provider but have distinct IDs.
    pub fn subscribe_deliveries(&self) -> tokio::sync::broadcast::Receiver<LogDelivery> {
        self.deliveries.subscribe()
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

    /// Fail rather than planning an unbounded number of partition/bucket pairs.
    pub fn with_max_assigned_buckets(mut self, limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(DataFusionError::Plan(
                "Fluss max assigned buckets must be positive".into(),
            ));
        }
        self.max_assigned_buckets = limit;
        Ok(self)
    }

    /// Bound per-execution ACK waits and writer retries for DataFusion INSERT.
    pub fn with_write_options(mut self, options: FlussWriteOptions) -> Result<Self> {
        self.write_options = options.validate()?;
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
                if (self.initial_schema && filter::translate(filter, &self.schema).is_some())
                    || (self.partitioned
                        && !PartitionFilter::from_expr(filter, &self.schema, &self.partition_keys)
                            .is_empty())
                {
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
        if !self.partitioned && self.buckets as usize > self.max_assigned_buckets {
            return Err(DataFusionError::Plan(
                "Fluss table exceeds max_assigned_buckets".into(),
            ));
        }
        let groups = bucket_groups(self.buckets, self.parallelism(state));
        let metrics = ExecutionPlanMetricsSet::new();
        let mut partition_filter = PartitionFilter::default();
        for expr in filters {
            partition_filter.extend(PartitionFilter::from_expr(
                expr,
                &self.schema,
                &self.partition_keys,
            ));
        }
        let predicate = self
            .initial_schema
            .then(|| {
                fluss::predicate::Predicate::and_all(
                    filters
                        .iter()
                        .filter_map(|expr| filter::translate(expr, &self.schema)),
                )
            })
            .flatten();
        let description = format!(
            "kind=log, mode={:?}, table={}, projection={}, projected_columns={:?}, batch_pruning={predicate:?}, partition_pruning={partition_filter:?}",
            self.options.mode,
            self.path,
            if projection.is_some_and(Vec::is_empty) {
                "full_rows_for_count"
            } else if self.initial_schema {
                "server"
            } else {
                "client_evolved_schema"
            },
            schema
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>()
        );
        let spec = Arc::new(ScanSpec {
            connection: Arc::clone(&self.connection),
            path: self.path.clone(),
            schema: Arc::clone(&schema),
            full_schema: Arc::clone(&self.schema),
            projection: projection.cloned(),
            filter: predicate,
            table_id: self.table_id,
            schema_id: self.schema_id,
            project_at_source: self.initial_schema,
            buckets: self.buckets,
            partitioned: self.partitioned,
            partition_filter,
            max_assigned_buckets: self.max_assigned_buckets,
            partition_captures: Arc::new(SharedCaptures::<PartitionedOffsets>::default()),
            options: self.options.clone(),
            captures: Arc::new(SharedCaptures::<OffsetWindow>::default()),
            metrics: metrics.clone(),
            deliveries: self.deliveries.clone(),
            execution_ids: Arc::new(SharedCaptures::<u64>::default()),
            deadlines: Arc::new(SharedCaptures::default()),
        });
        let inner = StreamingTableExec::try_new(
            schema,
            spec.partitions(&groups),
            None,
            [],
            self.options.mode == LogReadMode::Streaming,
            None,
        )?;
        Ok(Arc::new(FlussScanExec::new(
            inner,
            metrics,
            groups,
            description,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_write(
            FlussWriteTarget {
                connection: Arc::clone(&self.connection),
                path: self.path.clone(),
                table_id: self.table_id,
                schema_id: self.schema_id,
                schema: Arc::clone(&self.schema),
                kind: WriteKind::Log,
                options: self.write_options,
            },
            input,
            insert_op,
        )
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

pub(crate) fn bucket_groups(buckets: i32, parallelism: usize) -> Vec<Vec<i32>> {
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
