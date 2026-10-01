// SPDX-License-Identifier: Apache-2.0
//! Expose Fluss scan metrics through DataFusion's EXPLAIN ANALYZE.

use arrow::record_batch::RecordBatch;
use datafusion::common::format::{MetricCategory, MetricType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};

#[derive(Clone)]
pub(crate) struct PartitionMetrics {
    pub(crate) output_rows: Count,
    pub(crate) arrow_output_bytes: Count,
    pub(crate) arrow_decoded_bytes: Count,
    pub(crate) output_batches: Count,
    pub(crate) capture_time: Time,
    pub(crate) read_time: Time,
    pub(crate) peak_batch_bytes: Gauge,
    pub(crate) active_streams: Gauge,
}

impl PartitionMetrics {
    pub(crate) fn new(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        bucket_ids: &[i32],
    ) -> Self {
        let ids = bucket_ids
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let builder = || {
            MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .with_new_label("buckets", ids.clone())
        };
        builder()
            .with_category(MetricCategory::Rows)
            .counter("fluss_buckets_assigned", partition)
            .add(bucket_ids.len());
        Self {
            output_rows: builder().output_rows(partition),
            arrow_output_bytes: builder()
                .with_category(MetricCategory::Bytes)
                .counter("arrow_output_bytes", partition),
            arrow_decoded_bytes: builder()
                .with_category(MetricCategory::Bytes)
                .counter("arrow_decoded_bytes", partition),
            output_batches: builder().output_batches(partition),
            capture_time: builder().subset_time("fluss_offset_capture_time", partition),
            read_time: builder().subset_time("fluss_read_time", partition),
            peak_batch_bytes: builder()
                .peak_memory_usage("fluss_peak_decoded_arrow_batch_bytes", partition),
            active_streams: builder()
                .with_category(MetricCategory::Rows)
                .gauge("fluss_active_partition_streams", partition),
        }
    }

    pub(crate) fn record_decoded_batch(&self, batch: &RecordBatch) {
        let bytes = batch.get_array_memory_size();
        self.arrow_decoded_bytes.add(bytes);
        self.peak_batch_bytes.set_max(bytes);
    }

    pub(crate) fn record_output_batch(&self, batch: &RecordBatch) {
        self.arrow_output_bytes.add(batch.get_array_memory_size());
        self.output_rows.add(batch.num_rows());
        self.output_batches.add(1);
    }
}

/// The stream owns this guard; dropping a cancelled/failed stream releases it.
pub(crate) struct ReaderLifetime(Gauge);

impl ReaderLifetime {
    pub(crate) fn new(active: Gauge) -> Self {
        active.add(1);
        Self(active)
    }
}

impl Drop for ReaderLifetime {
    fn drop(&mut self) {
        self.0.sub(1);
    }
}
