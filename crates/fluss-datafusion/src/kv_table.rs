// SPDX-License-Identifier: Apache-2.0
//! A bounded SQL snapshot scan and writes for partitioned or unpartitioned KV.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchemaRef, DataFusionError, Result};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::dml::{InsertOp, MergeIntoClause};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::streaming::StreamingTableExec;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss::record::to_arrow_schema;

use crate::execution::FlussScanExec;
use crate::kv_scan::KvScanSpec;
use crate::log_table::bucket_groups;
use crate::offsets::SharedCaptures;
use crate::partitions::{DEFAULT_MAX_ASSIGNED_BUCKETS, PartitionFilter};
use crate::write::{
    FlussWriteOptions, FlussWriteTarget, WriteKind, plan_write, validate_delete_policy,
};

/// Each execution opens a fresh snapshot per bucket. There is no global
/// cross-bucket transaction or shared snapshot across repeated SQL queries.
#[derive(Clone)]
pub struct FlussKvTable {
    connection: Arc<FlussConnection>,
    path: TablePath,
    schema: SchemaRef,
    table_id: i64,
    schema_id: i32,
    buckets: i32,
    partitioned: bool,
    partition_keys: Vec<String>,
    timeout: Duration,
    max_partitions: Option<usize>,
    max_assigned_buckets: usize,
    write_options: FlussWriteOptions,
    delete_allowed: bool,
}

impl FlussKvTable {
    /// Support based on the table policy observed at opening time.
    /// Execution revalidates policy, identity/schema and permissions.
    pub fn capabilities(&self) -> crate::FlussCapabilities {
        crate::FlussCapabilities {
            read: crate::FlussReadCapability::KvSnapshot,
            insert: crate::FlussInsertCapability::FullRowUpsert,
            delete: self.delete_allowed,
            merge: true,
        }
    }

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
        if !info.has_primary_key() {
            return Err(DataFusionError::Plan(
                "Only Fluss primary-key tables are supported by the KV provider".into(),
            ));
        }
        let schema = to_arrow_schema(info.get_row_type()).map_err(fluss_error)?;
        let table_id = info.table_id;
        let schema_id = info.get_schema_id();
        let buckets = info.get_num_buckets();
        let partitioned = info.is_partitioned();
        let partition_keys = info.get_partition_keys().iter().cloned().collect();
        let delete_allowed = validate_delete_policy(info.get_properties()).is_ok();
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
            buckets,
            partitioned,
            partition_keys,
            timeout,
            max_partitions: None,
            max_assigned_buckets: DEFAULT_MAX_ASSIGNED_BUCKETS,
            write_options: FlussWriteOptions::default(),
            delete_allowed,
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
}

#[async_trait]
impl TableProvider for FlussKvTable {
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
            .map(|expr| {
                if self.partitioned
                    && !PartitionFilter::from_expr(expr, &self.schema, &self.partition_keys)
                        .is_empty()
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
        // The snapshot RPC offers no filter pushdown. DataFusion applies exact
        // filters and global limits after reading each live row.
        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        if !self.partitioned && self.buckets as usize > self.max_assigned_buckets {
            return Err(DataFusionError::Plan(
                "Fluss table exceeds max_assigned_buckets".into(),
            ));
        }
        let target = state.config_options().execution.target_partitions.max(1);
        let parallelism = self.max_partitions.map_or(target, |max| target.min(max));
        let groups = bucket_groups(self.buckets, parallelism);
        let mut partition_filter = PartitionFilter::default();
        for expr in filters {
            partition_filter.extend(PartitionFilter::from_expr(
                expr,
                &self.schema,
                &self.partition_keys,
            ));
        }
        let description = format!(
            "kind=kv_snapshot, table={}, projection={}, projected_columns={:?}, partition_pruning={partition_filter:?}",
            self.path,
            if projection.is_some_and(Vec::is_empty) {
                "full_rows_for_count"
            } else {
                "decoder"
            },
            schema
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>()
        );
        let metrics = ExecutionPlanMetricsSet::new();
        let spec = Arc::new(KvScanSpec {
            connection: Arc::clone(&self.connection),
            path: self.path.clone(),
            schema: Arc::clone(&schema),
            full_schema: Arc::clone(&self.schema),
            projection: projection.cloned(),
            table_id: self.table_id,
            buckets: self.buckets,
            partitioned: self.partitioned,
            partition_filter,
            max_assigned_buckets: self.max_assigned_buckets,
            partition_captures: Arc::new(SharedCaptures::default()),
            timeout: self.timeout,
            metrics: metrics.clone(),
            deadlines: Arc::new(SharedCaptures::default()),
        });
        let inner =
            StreamingTableExec::try_new(schema, spec.partitions(&groups), None, [], false, None)?;
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
                kind: WriteKind::Kv,
                options: self.write_options,
            },
            input,
            insert_op,
        )
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self
            .connection
            .get_table(&self.path)
            .await
            .map_err(fluss_error)?;
        validate_delete_policy(table.get_table_info().get_properties())?;
        // Let DataFusion evaluate SQL predicates on the finite KV snapshot;
        // pass full selected rows to the existing key-encoding delete writer.
        let mut builder = LogicalPlanBuilder::scan(
            self.path.table(),
            provider_as_source(Arc::new(self.clone())),
            None,
        )?;
        for filter in filters {
            let filter = filter
                .transform_up(|expr| {
                    if let Expr::Column(mut column) = expr {
                        column.relation = None;
                        Ok(Transformed::yes(Expr::Column(column)))
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })?
                .data;
            builder = builder.filter(filter)?;
        }
        let input = state.create_physical_plan(&builder.build()?).await?;
        plan_write(
            FlussWriteTarget {
                connection: Arc::clone(&self.connection),
                path: self.path.clone(),
                table_id: self.table_id,
                schema_id: self.schema_id,
                schema: Arc::clone(&self.schema),
                kind: WriteKind::DeleteKv,
                options: self.write_options,
            },
            input,
            InsertOp::Append,
        )
    }

    async fn merge_into(
        &self,
        state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: DFSchemaRef,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self
            .connection
            .get_table(&self.path)
            .await
            .map_err(fluss_error)?;
        let primary_keys = table.get_table_info().get_primary_keys().clone();
        let input = crate::merge::plan_merge_input(
            state,
            Arc::new(self.clone()),
            source,
            merge_schema,
            on,
            clauses,
            &primary_keys,
        )
        .await?;
        let schema = input.schema();
        plan_write(
            FlussWriteTarget {
                connection: Arc::clone(&self.connection),
                path: self.path.clone(),
                table_id: self.table_id,
                schema_id: self.schema_id,
                schema,
                kind: WriteKind::MergeKv,
                options: self.write_options,
            },
            input,
            InsertOp::Append,
        )
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
