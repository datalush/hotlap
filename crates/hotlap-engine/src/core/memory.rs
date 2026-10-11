//! Read-only memory reachability report for a live engine core.

use std::collections::HashSet;

use crate::error::EngineError;

/// Memory categories measured by a query-local, non-mutating traversal.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct MemoryReport {
    /// Logical payload bytes in currently covered grouped keys/output keys.
    pub logical_payload_bytes: usize,
    /// Known capacity of currently covered owned byte vectors.
    pub known_backing_capacity_bytes: usize,
    /// Structural lower-level bookkeeping estimate; not allocated heap size.
    pub estimated_structural_bytes: usize,
    /// Live state families whose backing storage is not yet visited.
    pub unknown_categories: Vec<String>,
    /// Opaque or externally-owned storage which cannot safely be sized.
    pub opaque_external_categories: Vec<String>,
    /// Whether every current core category is traversed.
    pub complete: bool,
}

pub(crate) struct GroupMemory {
    pub payload: usize,
    pub capacity: usize,
    pub estimate: usize,
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
        self.add_capacity(group.capacity)?;
        self.add_estimate(group.estimate)
    }

    pub(crate) fn add_output(
        &mut self,
        payload: usize,
        capacities: impl IntoIterator<Item = (usize, usize)>,
        allocations: &mut HashSet<(usize, usize)>,
    ) -> Result<(), EngineError> {
        self.logical_payload_bytes = checked_add(self.logical_payload_bytes, payload, "payload")?;
        for (identity, capacity) in capacities {
            if allocations.insert((identity, capacity)) {
                self.add_capacity(capacity)?;
            }
        }
        Ok(())
    }

    pub(crate) fn mark_unknown(&mut self, category: &str) {
        if !self.unknown_categories.iter().any(|item| item == category) {
            self.unknown_categories.push(category.to_owned());
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
        if !self.views.is_empty() {
            report.mark_unknown("view_plans_and_taps");
        }
        if !self.registered.is_empty() || !self.schemas.is_empty() {
            report.mark_unknown("core_registries_schemas_and_watermarks");
        }
        if self.retention.enabled() {
            report.mark_unknown("input_retention");
        }
        if !self.views.is_empty() {
            report.mark_opaque_external("arrow_buffers");
            report.mark_unknown("arrow_buffers");
        }
        for view in self.views.values() {
            view.graph.memory_usage(&mut report)?;
            let (payload, capacities) = view.output.memory_usage()?;
            report.add_output(payload, capacities, &mut allocations)?;
            if view.pending.is_some() {
                report.mark_unknown("view_pending");
            }
        }
        report.unknown_categories.sort();
        report.opaque_external_categories.sort();
        report.complete =
            report.unknown_categories.is_empty() && report.opaque_external_categories.is_empty();
        drop(allocations);
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
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
                &mut allocations,
            )
            .unwrap();
        assert_eq!(report.known_backing_capacity_bytes, 16);
        assert!(!format!("{report:?}").contains("0x"));
    }
}
