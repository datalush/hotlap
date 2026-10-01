// SPDX-License-Identifier: Apache-2.0
//! TableProvider and deferred, finite Arrow stream for an append-only Fluss table.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use fluss::client::{
    BoundedLogReadRange, EARLIEST_OFFSET, FlussConnection, RecordBatchLogReader,
    RecordBatchReadOutcome,
};
use fluss::metadata::{TableBucket, TablePath};
use fluss::rpc::message::OffsetSpec;

use crate::offsets::{Capture, OffsetCaptures, Offsets, validate_offsets};

const MAX_SCAN_PARTITIONS: i32 = 8;

/// Append-only log table with bounded, parallel physical scan partitions.
pub struct FlussLogTable {
    connection: Arc<FlussConnection>,
    path: TablePath,
    schema: SchemaRef,
    table_id: i64,
    buckets: i32,
    timeout: Duration,
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
        })
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

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // No filter/limit pushdown: the log scanner's batch pruning is inexact,
        // and a per-bucket limit would not be a correct global SQL LIMIT.
        // An empty projection (COUNT(*)) still reads rows: Fluss rejects an
        // empty column projection, so strip columns from Arrow batches later.
        let projected_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        let captures = Arc::new(OffsetCaptures::default());
        let partitions: Vec<Arc<dyn PartitionStream>> = bucket_groups(self.buckets)
            .into_iter()
            .enumerate()
            .map(|(index, bucket_ids)| {
                Arc::new(FlussPartition {
                    connection: Arc::clone(&self.connection),
                    path: self.path.clone(),
                    schema: Arc::clone(&projected_schema),
                    full_schema: Arc::clone(&self.schema),
                    projection: projection.cloned(),
                    table_id: self.table_id,
                    buckets: self.buckets,
                    bucket_ids,
                    index,
                    captures: Arc::clone(&captures),
                    timeout: self.timeout,
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            projected_schema,
            partitions,
            None,
            [],
            false,
            None,
        )?))
    }
}

struct FlussPartition {
    connection: Arc<FlussConnection>,
    path: TablePath,
    schema: SchemaRef,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    table_id: i64,
    buckets: i32,
    bucket_ids: Vec<i32>,
    index: usize,
    captures: Arc<OffsetCaptures>,
    timeout: Duration,
}

impl fmt::Debug for FlussLogTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussLogTable")
            .field("path", &self.path)
            .finish()
    }
}

impl fmt::Debug for FlussPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussPartition")
            .field("path", &self.path)
            .finish()
    }
}

impl PartitionStream for FlussPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let state = ReadState {
            source: FlussPartition {
                connection: Arc::clone(&self.connection),
                path: self.path.clone(),
                schema: Arc::clone(&schema),
                full_schema: Arc::clone(&self.full_schema),
                projection: self.projection.clone(),
                table_id: self.table_id,
                buckets: self.buckets,
                bucket_ids: self.bucket_ids.clone(),
                index: self.index,
                captures: Arc::clone(&self.captures),
                timeout: self.timeout,
            },
            reader: None,
            deadline: Instant::now() + self.timeout,
            capture: self.captures.for_partition(&ctx, self.index),
            // Keep the execution identity alive until this stream finishes.
            _context: ctx,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            if state.reader.is_none() {
                let remaining = state.remaining()?;
                let reader = tokio::time::timeout(remaining, state.start())
                    .await
                    .map_err(|_| expired())??;
                state.reader = Some(reader);
            }
            loop {
                let remaining = state.remaining()?;
                let result = tokio::time::timeout(
                    remaining,
                    state
                        .reader
                        .as_mut()
                        .expect("reader initialized")
                        .next_batch_with_timeout(remaining),
                )
                .await
                .map_err(|_| expired())?
                .map_err(fluss_error)?;
                match result {
                    RecordBatchReadOutcome::Batch(batch) => {
                        let batch = batch.into_batch();
                        let batch = if state.source.projection.as_ref().is_some_and(Vec::is_empty) {
                            batch.project(&[])?
                        } else {
                            batch
                        };
                        return Ok(Some((batch, state)));
                    }
                    RecordBatchReadOutcome::TimedOut => continue,
                    RecordBatchReadOutcome::Finished => return Ok(None),
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

struct ReadState {
    source: FlussPartition,
    reader: Option<RecordBatchLogReader>,
    deadline: Instant,
    capture: Capture,
    _context: Arc<TaskContext>,
}

impl ReadState {
    fn remaining(&self) -> Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|time| !time.is_zero())
            .ok_or_else(expired)
    }

    async fn start(&self) -> Result<RecordBatchLogReader> {
        let offsets = self
            .capture
            .get_or_init(|| async {
                self.capture_offsets()
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            .as_ref()
            .map_err(|error| DataFusionError::Execution(error.clone()))?;
        let table = self
            .source
            .connection
            .get_table(&self.source.path)
            .await
            .map_err(fluss_error)?;
        let info = table.get_table_info();
        if info.table_id != self.source.table_id || info.get_num_buckets() != self.source.buckets {
            return Err(DataFusionError::Execution(
                "Fluss table topology changed; plan again".into(),
            ));
        }
        let scan = table.new_scan();
        let scan = match self.source.projection.as_deref() {
            Some(indices) if !indices.is_empty() => scan.project(indices).map_err(fluss_error)?,
            _ => scan,
        };
        let scanner = scan
            .create_record_batch_log_scanner()
            .map_err(fluss_error)?;
        let expected_schema = if self.source.projection.as_ref().is_some_and(Vec::is_empty) {
            &self.source.full_schema
        } else {
            &self.source.schema
        };
        if scanner.schema() != *expected_schema {
            return Err(DataFusionError::Execution(
                "Fluss table schema changed; plan again".into(),
            ));
        }
        let ranges = self
            .source
            .bucket_ids
            .iter()
            .map(|&bucket| BoundedLogReadRange {
                bucket: TableBucket::new(self.source.table_id, bucket),
                starting_offset: EARLIEST_OFFSET,
                stopping_offset: offsets[&bucket],
            })
            .collect();
        RecordBatchLogReader::new_from_ranges(scanner, ranges)
            .await
            .map_err(fluss_error)
    }

    async fn capture_offsets(&self) -> Result<Offsets> {
        let admin = self.source.connection.get_admin().map_err(fluss_error)?;
        let buckets: Vec<i32> = (0..self.source.buckets).collect();
        let offsets = admin
            .list_offsets(&self.source.path, &buckets, OffsetSpec::Latest)
            .await
            .map_err(fluss_error)?;
        validate_offsets(offsets, self.source.buckets)
    }
}

fn bucket_groups(buckets: i32) -> Vec<Vec<i32>> {
    let count = buckets.min(MAX_SCAN_PARTITIONS) as usize;
    let mut groups = vec![Vec::new(); count];
    for bucket in 0..buckets {
        groups[bucket as usize % count].push(bucket);
    }
    groups
}

fn expired() -> DataFusionError {
    DataFusionError::Execution("Fluss bounded log scan timed out; result is incomplete".into())
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

impl fmt::Display for FlussLogTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlussLogTable({})", self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::bucket_groups;

    #[test]
    fn grouping_covers_each_bucket_once_and_caps_parallelism() {
        let groups = bucket_groups(19);
        assert_eq!(groups.len(), 8);
        let mut buckets: Vec<_> = groups.into_iter().flatten().collect();
        buckets.sort_unstable();
        assert_eq!(buckets, (0..19).collect::<Vec<_>>());
    }
}
