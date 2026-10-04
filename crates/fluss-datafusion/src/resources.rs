// SPDX-License-Identifier: Apache-2.0
//! Source and columnar sink reservations follow Arrow buffer ownership.

use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayData, make_array};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_plan::metrics::Gauge;

pub(crate) const DEFAULT_MAX_RETAINED_BATCH_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn validate_batch_limit(bytes: usize) -> Result<usize> {
    if bytes == 0 {
        return Err(DataFusionError::Plan(
            "Fluss max retained batch bytes must be positive".into(),
        ));
    }
    Ok(bytes)
}

struct BatchLease {
    _reservation: Mutex<MemoryReservation>,
    retained_bytes: Gauge,
    bytes: usize,
}
impl Drop for BatchLease {
    fn drop(&mut self) {
        self.retained_bytes.sub(self.bytes);
    }
}

/// The original buffer deallocates its storage. Mutex supplies the unwind-safe
/// owner required by Arrow; the immutable reservation guard is never locked.
struct ReservedBuffer {
    _buffer: Buffer,
    _reservation: Arc<BatchLease>,
}

fn reserve_buffer(buffer: &Buffer, reservation: &Arc<BatchLease>) -> Buffer {
    let owner = Arc::new(ReservedBuffer {
        _buffer: buffer.clone(),
        _reservation: Arc::clone(reservation),
    });
    // SAFETY: owner retains the original immutable Buffer at this exact pointer
    // and length. Arrow only drops the owner; there is no second deallocator.
    unsafe {
        Buffer::from_custom_allocation(
            NonNull::new(buffer.as_ptr().cast_mut()).expect("Arrow buffer pointer is non-null"),
            buffer.len(),
            owner,
        )
    }
}

fn reserve_data(data: ArrayData, reservation: &Arc<BatchLease>) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(|b| reserve_buffer(b, reservation))
        .collect();
    let nulls = data.nulls().map(|nulls| {
        NullBuffer::new(BooleanBuffer::new(
            reserve_buffer(nulls.buffer(), reservation),
            nulls.offset(),
            nulls.len(),
        ))
    });
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(|child| reserve_data(child, reservation))
        .collect::<Result<Vec<_>>>()?;
    Ok(data
        .into_builder()
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children)
        .build()?)
}

/// Charge original backing buffers without copying their contents. Remaining
/// cloned/sliced/projected buffers retain the conservative whole-batch lease.
/// Decode precedes admission; this is not a total RSS/pre-allocation limit.
pub(crate) fn reserve_batch(
    batch: RecordBatch,
    consumer: &MemoryReservation,
    max_bytes: usize,
    retained_bytes: &Gauge,
) -> Result<RecordBatch> {
    let bytes = batch_backing_bytes(&batch)?;
    if bytes > max_bytes {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "Fluss source batch retains {bytes} backing bytes, exceeding max_retained_batch_bytes={max_bytes}; decoding precedes this admission limit"
        )));
    }
    let reservation = consumer.new_empty();
    reservation.try_grow(bytes)?;
    retained_bytes.add(bytes);
    let reservation = Arc::new(BatchLease {
        _reservation: Mutex::new(reservation),
        retained_bytes: retained_bytes.clone(),
        bytes,
    });
    let columns = batch
        .columns()
        .iter()
        .map(|column| reserve_data(column.to_data(), &reservation).map(make_array))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

pub(crate) fn batch_backing_bytes(batch: &RecordBatch) -> Result<usize> {
    batch
        .columns()
        .iter()
        .map(|column| column.get_buffer_memory_size())
        .try_fold(0usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted("Fluss Arrow backing byte count overflowed".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray};
    use arrow::datatypes::Int32Type;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};

    fn buffer_locations(data: &ArrayData) -> Vec<(usize, usize, usize)> {
        let mut locations = vec![(0, data.offset(), data.len())];
        locations.extend(
            data.buffers()
                .iter()
                .map(|buffer| (buffer.as_ptr() as usize, buffer.len(), 0)),
        );
        if let Some(nulls) = data.nulls() {
            locations.push((
                nulls.buffer().as_ptr() as usize,
                nulls.offset(),
                nulls.len(),
            ));
        }
        for child in data.child_data() {
            locations.extend(buffer_locations(child));
        }
        locations
    }

    #[test]
    fn lease_survives_projection_slice_and_source_drop_without_payload_copy() {
        let original = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("one"), None, Some("three")])) as ArrayRef,
            ),
            (
                "nested",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                    Some(vec![Some(1), None]),
                    None,
                    Some(vec![Some(3)]),
                ])) as ArrayRef,
            ),
        ])
        .unwrap()
        .slice(1, 2);
        let pointers: Vec<_> = original
            .columns()
            .iter()
            .map(|array| buffer_locations(&array.to_data()))
            .collect();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let consumer = MemoryConsumer::new("source").register(&pool);
        let retained_bytes = Gauge::new();
        let first =
            reserve_batch(original.clone(), &consumer, usize::MAX, &retained_bytes).unwrap();
        let second = reserve_batch(original, &consumer, usize::MAX, &retained_bytes).unwrap();
        let reserved = pool.reserved();
        assert!(reserved > 0);
        assert_eq!(retained_bytes.value(), reserved);
        for (column, expected) in first.columns().iter().zip(pointers) {
            assert_eq!(buffer_locations(&column.to_data()), expected);
        }
        let retained = first.project(&[2]).unwrap().slice(1, 1);
        drop(first);
        drop(consumer);
        assert_eq!(pool.reserved(), reserved);
        drop(second);
        assert_eq!(pool.reserved(), reserved / 2);
        let nested = retained
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            nested
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        drop(retained);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(retained_bytes.value(), 0);
    }

    #[test]
    fn failed_admission_preserves_other_live_leases() {
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let size = batch.column(0).get_buffer_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(size));
        let consumer = MemoryConsumer::new("source").register(&pool);
        let retained_bytes = Gauge::new();
        let first = reserve_batch(batch.clone(), &consumer, usize::MAX, &retained_bytes).unwrap();
        assert!(reserve_batch(batch.clone(), &consumer, usize::MAX, &retained_bytes).is_err());
        assert!(reserve_batch(batch, &consumer, size - 1, &retained_bytes).is_err());
        assert_eq!(pool.reserved(), size);
        assert_eq!(retained_bytes.value(), size);
        drop(consumer);
        assert_eq!(pool.reserved(), size);
        drop(first);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(retained_bytes.value(), 0);
    }

    #[test]
    fn batch_ceiling_checks_backing_storage_independently_of_pool_capacity() {
        let original = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let bytes = original.column(0).get_buffer_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes * 10));
        let consumer = MemoryConsumer::new("source").register(&pool);
        let retained = Gauge::new();
        assert!(validate_batch_limit(0).is_err());
        assert!(reserve_batch(original.slice(0, 1), &consumer, bytes - 1, &retained).is_err());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(retained.value(), 0);
        let admitted = reserve_batch(original, &consumer, bytes, &retained).unwrap();
        assert_eq!(pool.reserved(), bytes);
        assert_eq!(retained.value(), bytes);
        drop(admitted);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(retained.value(), 0);
    }
}
