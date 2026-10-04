// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::client::table::partition_getter::{PartitionGetter, get_physical_path};
use crate::client::{WriteRecord, WriteResultFuture, WriterClient};
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{PhysicalTablePath, TableInfo, TablePath};
use crate::record::{estimate_append_batch_size, prepare_append_record_batch};
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use crate::row::{ColumnarRow, InternalRow};
use arrow::array::{RecordBatch, UInt32Array};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TableAppend {
    table_path: Arc<TablePath>,
    table_info: Arc<TableInfo>,
    writer_client: Arc<WriterClient>,
}

impl TableAppend {
    pub(super) fn new(
        table_path: TablePath,
        table_info: Arc<TableInfo>,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        Self {
            table_path: Arc::new(table_path),
            table_info,
            writer_client,
        }
    }

    pub fn create_writer(&self) -> Result<AppendWriter> {
        let partition_getter = if self.table_info.is_partitioned() {
            Some(PartitionGetter::new(
                self.table_info.row_type(),
                Arc::clone(self.table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        let bucket_router = if self.table_info.has_bucket_key() {
            let data_lake_format = self.table_info.table_config.get_datalake_format()?;
            let encoder = KeyEncoderFactory::of_bucket_key_encoder(
                self.table_info.row_type(),
                self.table_info.get_bucket_keys(),
                &data_lake_format,
            )?;
            Some(Mutex::new(encoder))
        } else {
            None
        };

        Ok(AppendWriter {
            table_path: Arc::clone(&self.table_path),
            partition_getter,
            writer_client: self.writer_client.clone(),
            table_info: Arc::clone(&self.table_info),
            bucket_router,
        })
    }
}

pub struct AppendWriter {
    table_path: Arc<TablePath>,
    partition_getter: Option<PartitionGetter>,
    writer_client: Arc<WriterClient>,
    table_info: Arc<TableInfo>,
    bucket_router: Option<Mutex<Box<dyn KeyEncoder>>>,
}

impl AppendWriter {
    /// Conservative scratch admission hint, excluding retained input/gather
    /// payload. Indices scale with rows; group metadata scales with possible
    /// destinations. Encoded routing keys have their own selected-column hint.
    pub fn estimated_arrow_routing_bytes(&self, batch: &RecordBatch) -> Result<usize> {
        let cluster = self.writer_client.cluster();
        let current = cluster.get_table(&self.table_path)?;
        let rows = batch.num_rows();
        let destinations = if !current.is_partitioned() {
            rows.min(current.get_num_buckets() as usize)
        } else if current.bucket_count_epoch == 0 {
            // Unresolved partitions may still be created at first write.
            rows
        } else {
            cluster
                .get_partition_id_by_path()
                .iter()
                .filter(|(path, _)| path.get_table_path() == self.table_path.as_ref())
                .try_fold(0usize, |count, (_, id)| {
                    Ok::<_, crate::error::Error>(count.saturating_add(
                        cluster.routing_bucket_count(current.table_id, *id)? as usize,
                    ))
                })?
                .min(rows)
        };
        let mut keys = std::collections::HashSet::new();
        let mut key_bytes = 0usize;
        for name in self
            .table_info
            .get_bucket_keys()
            .iter()
            .chain(self.table_info.get_partition_keys().iter())
        {
            if keys.insert(name) {
                key_bytes = key_bytes.saturating_add(
                    batch
                        .column(batch.schema().index_of(name)?)
                        .get_buffer_memory_size(),
                );
            }
        }
        Ok(rows
            .saturating_mul(8)
            .saturating_add(destinations.saturating_mul(512))
            .saturating_add(key_bytes.saturating_mul(3)))
    }

    fn check_field_count<R: InternalRow>(&self, row: &R) -> Result<()> {
        let expected = self.table_info.get_row_type().fields().len();
        if row.get_field_count() != expected {
            return Err(IllegalArgument {
                message: format!(
                    "The field count of the row does not match the table schema. \
                     Expected: {}, Actual: {}",
                    expected,
                    row.get_field_count()
                ),
            });
        }
        Ok(())
    }

    /// Appends a row to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// # Arguments
    /// * row - the row to append.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn append<R: InternalRow>(&self, row: &R) -> Result<WriteResultFuture> {
        self.check_field_count(row)?;
        let physical_table_path = Arc::new(get_physical_path(
            &self.table_path,
            self.partition_getter.as_ref(),
            row,
        )?);
        let bucket_key = match &self.bucket_router {
            Some(router) => Some(router.lock().encode_key(row)?),
            None => None,
        };
        let record = WriteRecord::for_append(
            Arc::clone(&self.table_info),
            physical_table_path,
            self.table_info.schema_id,
            row,
        )
        .with_bucket_key(bucket_key);
        let result_handle = self.writer_client.send(&record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    /// Appends an Arrow RecordBatch to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// Mixed partitions and effective per-partition bucket layouts are routed
    /// through the same client assigners as row append. Contiguous destination
    /// groups are sliced without copying payload. Interleaved groups use Arrow
    /// take, preserving relative input order within each destination. Groups
    /// are split to the configured batch byte target; a singleton may exceed
    /// that target but still has to fit the client's buffer/request limits.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn append_arrow_batch(&self, batch: RecordBatch) -> Result<WriteResultFuture> {
        self.append_arrow_batch_with_retainer(batch, Ok)
    }

    /// Apply caller-owned resource accounting to newly materialized cast/gather
    /// buffers before the client retains them. Compatible slices keep their
    /// original Arrow owners. The callback may attach buffer leases or reject
    /// admission; routing, encoding, buffer waits and ACKs remain client-owned.
    /// This is post-allocation admission, not an Arrow allocator hook.
    /// The callback must preserve schema, values, row count and row order.
    pub fn append_arrow_batch_with_retainer<F>(
        &self,
        batch: RecordBatch,
        mut retain: F,
    ) -> Result<WriteResultFuture>
    where
        F: FnMut(RecordBatch) -> Result<RecordBatch>,
    {
        if batch.num_rows() == 0 {
            // Nothing to write; also avoids a keyless send to a bucket-key table.
            return Ok(WriteResultFuture::join(Vec::new()));
        }
        if batch.num_rows() > i32::MAX as usize {
            return Err(IllegalArgument {
                message: "Arrow append exceeds the Fluss record count limit".into(),
            });
        }
        self.writer_client.check_open()?;
        let prepared = prepare_append_record_batch(&batch, self.table_info.row_type())?;
        let converted = batch
            .columns()
            .iter()
            .zip(prepared.columns())
            .any(|(old, new)| !Arc::ptr_eq(old, new));
        let batch = if converted {
            retain(prepared)?
        } else {
            prepared
        };
        let cluster = self.writer_client.cluster();
        let batch_arc = Arc::new(batch);
        let row_type = Arc::new(self.table_info.row_type.clone());
        let mut row = ColumnarRow::new(Arc::clone(&batch_arc), Arc::clone(&row_type), 0, None)?;
        let mut handles = Vec::new();
        let mut destinations = HashMap::new();
        let mut groups: Vec<ArrowGroup> = Vec::new();
        for i in 0..batch_arc.num_rows() {
            row.set_row_id(i);
            let path = Arc::new(get_physical_path(
                &self.table_path,
                self.partition_getter.as_ref(),
                &row,
            )?);
            let key = self
                .bucket_router
                .as_ref()
                .map(|encoder| encoder.lock().encode_key(&row))
                .transpose()?;
            let (assigner, bucket) = self.writer_client.assign_bucket(
                &self.table_info,
                key.as_ref(),
                &path,
                &cluster,
            )?;
            let group_index = *destinations
                .entry((Arc::clone(&path), bucket))
                .or_insert_with(|| {
                    groups.push(ArrowGroup {
                        path,
                        key,
                        assigner,
                        bucket,
                        indices: Vec::new(),
                    });
                    groups.len() - 1
                });
            groups[group_index].indices.push(i as u32);
        }
        for group in groups {
            self.writer_client.check_open()?;
            let (selected, materialized) = select_rows(&batch_arc, group.indices)?;
            let selected = if materialized {
                retain(selected)?
            } else {
                selected
            };
            self.send_arrow_run(
                &selected,
                group.path,
                group.key,
                &cluster,
                group.assigner,
                group.bucket,
                &mut handles,
            )?;
        }
        Ok(WriteResultFuture::join(handles))
    }

    #[allow(clippy::too_many_arguments)]
    fn send_arrow_run(
        &self,
        batch: &RecordBatch,
        physical_table_path: Arc<PhysicalTablePath>,
        bucket_key: Option<Bytes>,
        cluster: &Arc<crate::cluster::Cluster>,
        assigner: Arc<dyn crate::client::write::bucket_assigner::BucketAssigner>,
        bucket: crate::BucketId,
        handles: &mut Vec<crate::client::ResultHandle>,
    ) -> Result<()> {
        let mut start = 0;
        while start < batch.num_rows() {
            self.writer_client.check_open()?;
            let length = fitting_prefix(batch, start, self.writer_client.arrow_batch_size())?;
            let record = WriteRecord::for_append_record_batch(
                Arc::clone(&self.table_info),
                Arc::clone(&physical_table_path),
                self.table_info.schema_id,
                batch.slice(start, length),
            )
            .with_bucket_key(bucket_key.clone());
            handles.push(self.writer_client.send_assigned(
                &record,
                cluster,
                Arc::clone(&assigner),
                bucket,
            )?);
            start += length;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }
}

struct ArrowGroup {
    path: Arc<PhysicalTablePath>,
    key: Option<Bytes>,
    assigner: Arc<dyn crate::client::write::bucket_assigner::BucketAssigner>,
    bucket: crate::BucketId,
    indices: Vec<u32>,
}

fn select_rows(batch: &RecordBatch, indices: Vec<u32>) -> Result<(RecordBatch, bool)> {
    let start = indices[0] as usize;
    if indices
        .iter()
        .enumerate()
        .all(|(offset, &index)| index as usize == start + offset)
    {
        return Ok((batch.slice(start, indices.len()), false));
    }
    let indices = UInt32Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column, &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((RecordBatch::try_new(batch.schema(), columns)?, true))
}

/// Binary search avoids scanning/copying variable-width payload to choose a
/// bounded slice. The singleton case preserves the client's oversized-row
/// behavior; client admission still enforces its buffer capacity.
fn fitting_prefix(batch: &RecordBatch, start: usize, target: usize) -> Result<usize> {
    let mut low = 1;
    let mut high = batch.num_rows() - start;
    if high == 1 || estimate_append_batch_size(&batch.slice(start, high))? <= target {
        return Ok(high);
    }
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if estimate_append_batch_size(&batch.slice(start, middle))? <= target {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int32Array, ListArray, StringArray};
    use arrow::datatypes::Int32Type;

    #[test]
    fn contiguous_selection_shares_buffers_and_gather_preserves_nulls_and_order() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec![
                    Some("zero"),
                    None,
                    Some("two"),
                    Some("three"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "nested",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                    Some(vec![Some(0)]),
                    None,
                    Some(vec![Some(2), None]),
                    Some(vec![]),
                    None,
                ])) as ArrayRef,
            ),
        ])
        .unwrap()
        .slice(1, 4);
        let (sliced, materialized) = select_rows(&batch, vec![0, 1, 2]).unwrap();
        assert!(!materialized);
        for (original, selected) in batch.columns().iter().zip(sliced.columns()) {
            for (old, new) in original
                .to_data()
                .buffers()
                .iter()
                .zip(selected.to_data().buffers())
            {
                assert_eq!(old.as_ptr(), new.as_ptr());
            }
        }
        let (gathered, materialized) = select_rows(&batch, vec![0, 2, 3]).unwrap();
        assert!(materialized);
        assert_eq!(
            gathered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 3, 4]
        );
        let strings = gathered
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(strings.is_null(0) && strings.is_null(2));
        assert_eq!(strings.value(1), "three");
        let lists = gathered
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(lists.is_null(0) && lists.is_null(2));
        assert_eq!(lists.value_length(1), 0);
    }

    #[test]
    fn byte_slices_split_a_large_backing_without_copy_or_losing_rows() {
        let batch = RecordBatch::try_from_iter(vec![(
            "text",
            Arc::new(StringArray::from(vec!["x".repeat(200_000); 20])) as ArrayRef,
        )])
        .unwrap();
        let target = 1024 * 1024;
        assert!(estimate_append_batch_size(&batch).unwrap() > target);
        let mut start = 0;
        let mut slices = 0;
        while start < batch.num_rows() {
            let count = fitting_prefix(&batch, start, target).unwrap();
            let slice = batch.slice(start, count);
            assert!(count > 0 && count < batch.num_rows());
            assert!(estimate_append_batch_size(&slice).unwrap() <= target);
            assert_eq!(
                slice.column(0).to_data().buffers()[1].as_ptr(),
                batch.column(0).to_data().buffers()[1].as_ptr()
            );
            start += count;
            slices += 1;
        }
        assert_eq!(start, 20);
        assert_eq!(slices, 4);
        assert_eq!(fitting_prefix(&batch, 0, 1).unwrap(), 1);
    }
}
