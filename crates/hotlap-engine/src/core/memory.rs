//! Read-only memory reachability report for a live engine core.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayRef};
use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;

use crate::error::EngineError;

/// Memory categories measured by a query-local, non-mutating traversal.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct MemoryReport {
    /// Logical payload byte sum for covered families; owners can retain equal
    /// bytes separately.
    pub logical_payload_bytes: usize,
    /// Known capacity of covered owned byte vectors and Arrow buffers.
    pub known_backing_capacity_bytes: usize,
    /// Subtotal of known Arrow backing-buffer capacities; included above.
    pub arrow_buffer_capacity_bytes: usize,
    /// Sum of reachable Arrow buffer view lengths; shared views may repeat bytes.
    pub arrow_visible_buffer_bytes: usize,
    /// Reachable byte ranges for Arrow buffers whose external capacity is unknown.
    pub opaque_external_bytes_lower_bound: usize,
    /// Logical encoded keys and payload bytes retained by initialized joins.
    pub join_logical_payload_bytes: usize,
    /// Known Vec capacities for join key indices and arrangement keys; subtotal.
    pub join_known_key_capacity_bytes: usize,
    /// Logical key bytes duplicated in the current open-window map and row.
    pub window_logical_payload_bytes: usize,
    /// Known Vec capacities for window key indices/open keys; subtotal.
    pub window_known_key_capacity_bytes: usize,
    /// Distinct retained values across MIN/MAX multisets.
    pub minmax_distinct_entries: usize,
    /// Structural lower-level bookkeeping estimate; not allocated heap size.
    pub estimated_structural_bytes: usize,
    /// State families traversed, even when allocator details remain unknown.
    pub covered_categories: Vec<String>,
    /// Live state families whose backing storage is not yet visited.
    pub unknown_categories: Vec<String>,
    /// Opaque, external, or zero-capacity storage which cannot be sized safely.
    pub opaque_external_categories: Vec<String>,
    /// Whether every current core category is traversed.
    pub complete: bool,
    /// Exact retained heap size is unavailable from the safe interfaces here.
    pub exact_heap_bytes: Option<usize>,
}

pub(crate) struct GroupMemory {
    pub payload: usize,
    pub capacity: usize,
    pub estimate: usize,
    pub minmax_entries: usize,
    pub minmax_estimate: usize,
}

impl MemoryReport {
    pub(crate) fn add_capacity(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.known_backing_capacity_bytes =
            checked_add(self.known_backing_capacity_bytes, bytes, "capacity")?;
        Ok(())
    }

    pub(crate) fn add_estimate(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.estimated_structural_bytes =
            checked_add(self.estimated_structural_bytes, bytes, "estimate")?;
        Ok(())
    }

    pub(crate) fn add_group(&mut self, group: GroupMemory) -> Result<(), EngineError> {
        self.logical_payload_bytes =
            checked_add(self.logical_payload_bytes, group.payload, "payload")?;
        self.minmax_distinct_entries = checked_add(
            self.minmax_distinct_entries,
            group.minmax_entries,
            "MIN/MAX entry count",
        )?;
        self.add_capacity(group.capacity)?;
        self.add_estimate(checked_add(
            group.estimate,
            group.minmax_estimate,
            "MIN/MAX estimate",
        )?)
    }

    pub(crate) fn add_output(
        &mut self,
        payload: usize,
        capacities: impl IntoIterator<Item = (usize, usize)>,
        structural_estimate: usize,
        allocations: &mut HashSet<(usize, usize)>,
    ) -> Result<(), EngineError> {
        self.logical_payload_bytes = checked_add(self.logical_payload_bytes, payload, "payload")?;
        for (identity, capacity) in capacities {
            if allocations.insert((identity, capacity)) {
                self.add_capacity(capacity)?;
            }
        }
        self.add_estimate(structural_estimate)
    }

    pub(crate) fn add_join(
        &mut self,
        payload: usize,
        key_capacity: usize,
        structural_estimate: usize,
    ) -> Result<(), EngineError> {
        self.join_logical_payload_bytes =
            checked_add(self.join_logical_payload_bytes, payload, "join payload")?;
        self.logical_payload_bytes = checked_add(self.logical_payload_bytes, payload, "payload")?;
        self.join_known_key_capacity_bytes = checked_add(
            self.join_known_key_capacity_bytes,
            key_capacity,
            "join capacity",
        )?;
        self.add_capacity(key_capacity)?;
        self.add_estimate(structural_estimate)
    }

    pub(crate) fn add_window(
        &mut self,
        payload: usize,
        key_capacity: usize,
        structural_estimate: usize,
    ) -> Result<(), EngineError> {
        self.window_logical_payload_bytes =
            checked_add(self.window_logical_payload_bytes, payload, "window payload")?;
        self.logical_payload_bytes = checked_add(self.logical_payload_bytes, payload, "payload")?;
        self.window_known_key_capacity_bytes = checked_add(
            self.window_known_key_capacity_bytes,
            key_capacity,
            "window capacity",
        )?;
        self.add_capacity(key_capacity)?;
        self.add_estimate(structural_estimate)
    }

    pub(crate) fn add_arrow_batch(
        &mut self,
        batch: &RecordBatch,
        allocations: &mut HashSet<(usize, usize)>,
        external_ranges: &mut HashMap<usize, usize>,
    ) -> Result<(), EngineError> {
        for column in batch.columns() {
            self.add_arrow_array(column, allocations, external_ranges)?;
        }
        Ok(())
    }

    pub(crate) fn add_arrow_array(
        &mut self,
        array: &ArrayRef,
        allocations: &mut HashSet<(usize, usize)>,
        external_ranges: &mut HashMap<usize, usize>,
    ) -> Result<(), EngineError> {
        self.add_arrow_data(&array.to_data(), allocations, external_ranges)
    }

    fn add_arrow_data(
        &mut self,
        data: &arrow::array::ArrayData,
        allocations: &mut HashSet<(usize, usize)>,
        external_ranges: &mut HashMap<usize, usize>,
    ) -> Result<(), EngineError> {
        self.mark_covered("arrow_buffers");
        for buffer in data.buffers() {
            self.arrow_visible_buffer_bytes = checked_add(
                self.arrow_visible_buffer_bytes,
                buffer.len(),
                "Arrow visible bytes",
            )?;
            self.add_arrow_buffer(buffer, allocations, external_ranges)?;
        }
        if let Some(nulls) = data.nulls() {
            self.arrow_visible_buffer_bytes = checked_add(
                self.arrow_visible_buffer_bytes,
                nulls.buffer().len(),
                "Arrow visible bytes",
            )?;
            self.add_arrow_buffer(nulls.buffer(), allocations, external_ranges)?;
        }
        for child in data.child_data() {
            self.add_arrow_data(child, allocations, external_ranges)?;
        }
        Ok(())
    }

    fn add_arrow_buffer(
        &mut self,
        buffer: &Buffer,
        allocations: &mut HashSet<(usize, usize)>,
        external_ranges: &mut HashMap<usize, usize>,
    ) -> Result<(), EngineError> {
        let allocation = (buffer.data_ptr().as_ptr() as usize, buffer.capacity());
        if buffer.capacity() == 0 {
            let previous = external_ranges.get(&allocation.0).copied().unwrap_or(0);
            if buffer.len() > previous {
                self.opaque_external_bytes_lower_bound = checked_add(
                    self.opaque_external_bytes_lower_bound,
                    buffer.len() - previous,
                    "external Arrow lower bound",
                )?;
                external_ranges.insert(allocation.0, buffer.len());
            }
            self.mark_opaque_external("arrow_capacity_unknown_buffers");
            return Ok(());
        }
        if !allocations.insert(allocation) {
            return Ok(());
        }
        self.arrow_buffer_capacity_bytes = checked_add(
            self.arrow_buffer_capacity_bytes,
            buffer.capacity(),
            "Arrow capacity",
        )?;
        self.add_capacity(buffer.capacity())
    }

    pub(crate) fn mark_unknown(&mut self, category: &str) {
        if !self.unknown_categories.iter().any(|item| item == category) {
            self.unknown_categories.push(category.to_owned());
        }
    }

    pub(crate) fn mark_covered(&mut self, category: &str) {
        if !self.covered_categories.iter().any(|item| item == category) {
            self.covered_categories.push(category.to_owned());
        }
    }

    pub(crate) fn mark_opaque_external(&mut self, category: &str) {
        if !self
            .opaque_external_categories
            .iter()
            .any(|item| item == category)
        {
            self.opaque_external_categories.push(category.to_owned());
        }
    }
}

pub(crate) fn checked_add(left: usize, right: usize, category: &str) -> Result<usize, EngineError> {
    left.checked_add(right)
        .ok_or_else(|| EngineError::Infrastructure(format!("memory report {category} overflow")))
}

pub(crate) fn checked_capacity_bytes(
    capacity: usize,
    element_size: usize,
) -> Result<usize, EngineError> {
    capacity
        .checked_mul(element_size)
        .ok_or_else(|| EngineError::Infrastructure("memory report capacity overflow".into()))
}

impl super::EngineCore {
    /// Measures memory reachable through the currently covered core state.
    /// This is a shared-reachable partial report, not exclusive heap or RSS.
    pub fn memory_report(&self) -> Result<MemoryReport, EngineError> {
        self.ensure_healthy().map_err(EngineError::from)?;
        let mut report = MemoryReport::default();
        let mut allocations = HashSet::new();
        let mut external_ranges = std::collections::HashMap::new();
        if !self.views.is_empty() {
            report.mark_unknown("view_plans_and_taps");
        }
        if !self.registered.is_empty() || !self.schemas.is_empty() {
            report.mark_unknown("core_registries_schemas_and_watermarks");
        }
        report.mark_unknown("core_metrics_handles_and_registry");
        self.retention
            .memory_usage(&mut report, &mut allocations, &mut external_ranges)?;
        for view in self.views.values() {
            view.graph
                .memory_usage(&mut report, &mut allocations, &mut external_ranges)?;
            let output_memory = view.output.memory_usage()?;
            report.mark_covered("consolidated_outputs");
            report.add_output(
                output_memory.payload,
                output_memory.capacities,
                output_memory.estimate,
                &mut allocations,
            )?;
            if output_memory.estimate > 0 {
                report.mark_unknown("output_hash_map_allocator_overhead");
            }
            if view.pending.is_some() {
                report.mark_covered("tap_pending_batches");
                report.mark_unknown("view_pending");
                if let Some(pending) = &view.pending {
                    report.add_arrow_batch(
                        &pending.batch,
                        &mut allocations,
                        &mut external_ranges,
                    )?;
                    report.add_arrow_array(
                        &pending.diff,
                        &mut allocations,
                        &mut external_ranges,
                    )?;
                }
            }
        }
        report.unknown_categories.sort();
        report.opaque_external_categories.sort();
        report.covered_categories.sort();
        report.complete =
            report.unknown_categories.is_empty() && report.opaque_external_categories.is_empty();
        drop(allocations);
        drop(external_ranges);
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};

    use super::MemoryReport;

    #[test]
    fn output_backing_capacity_deduplicates_same_allocation_but_not_equal_bytes() {
        let first = vec![7u8; 8];
        let equal_but_distinct = vec![7u8; 8];
        assert_eq!(first, equal_but_distinct);
        assert_ne!(first.as_ptr(), equal_but_distinct.as_ptr());

        let mut report = MemoryReport::default();
        let mut allocations = std::collections::HashSet::new();
        report
            .add_output(
                0,
                [
                    (first.as_ptr() as usize, first.capacity()),
                    (first.as_ptr() as usize, first.capacity()),
                    (
                        equal_but_distinct.as_ptr() as usize,
                        equal_but_distinct.capacity(),
                    ),
                ],
                0,
                &mut allocations,
            )
            .unwrap();
        assert_eq!(report.known_backing_capacity_bytes, 16);
        assert!(!format!("{report:?}").contains("0x"));
    }

    #[test]
    fn arrow_collector_walks_nullable_nested_arrays_and_deduplicates_slices() {
        let field = Field::new("value", DataType::Int64, true);
        let child: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]));
        let nested: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![field]),
            vec![child],
            None,
        ));
        let slice = nested.slice(1, 1);
        let equal_but_distinct_child: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]));
        let equal_but_distinct: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("value", DataType::Int64, true)]),
            vec![equal_but_distinct_child],
            None,
        ));

        let mut report = MemoryReport::default();
        let mut allocations = std::collections::HashSet::new();
        let mut external_ranges = std::collections::HashMap::new();
        report
            .add_arrow_array(&nested, &mut allocations, &mut external_ranges)
            .unwrap();
        let first = report.arrow_buffer_capacity_bytes;
        let first_visible = report.arrow_visible_buffer_bytes;
        assert!(first_visible > 0);
        report
            .add_arrow_array(&slice, &mut allocations, &mut external_ranges)
            .unwrap();
        assert_eq!(report.arrow_buffer_capacity_bytes, first);
        assert!(report.arrow_visible_buffer_bytes > first_visible);
        report
            .add_arrow_array(&equal_but_distinct, &mut allocations, &mut external_ranges)
            .unwrap();
        assert_eq!(report.arrow_buffer_capacity_bytes, first * 2);
        assert!(!format!("{report:?}").contains("0x"));
    }
}
