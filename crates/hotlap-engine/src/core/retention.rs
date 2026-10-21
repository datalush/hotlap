//! Input Z-set retention, so a view can be built after `START`.
//!
//! A view created after the first push is compiled to a fresh graph and
//! evaluated over the retained input deltas in their original push order,
//! reproducing the state of a view that had been built before the first push.
//! Retention is bounded by a maximum number of deltas and an optional accounted
//! byte budget; once either bound drops history, post-start builds are rejected.

use std::collections::{HashMap, VecDeque};

use arrow::datatypes::SchemaRef;

use hotlap_core::{InputId, ZSetBatch};

use super::graph::ViewGraph;
use super::output::ViewOutput;
use crate::error::EngineError;

/// One input delta in global push order, plus the input's watermark right after
/// that push (replaying it reproduces a windowed view exactly).
struct Event {
    input: InputId,
    batch: ZSetBatch,
    watermark: i64,
}

/// A bounded, in-memory log of applied input deltas.
pub(crate) struct InputRetention {
    /// Maximum retained deltas; `None` disables retention entirely.
    capacity: Option<usize>,
    byte_capacity: Option<usize>,
    events: VecDeque<Event>,
    /// Set once an old delta was dropped: the log no longer covers the run
    /// start, so replaying it cannot rebuild a correct view.
    truncated: bool,
}

impl Default for InputRetention {
    fn default() -> Self {
        Self::disabled()
    }
}

impl InputRetention {
    pub(super) fn memory_usage(
        &self,
        report: &mut super::memory::MemoryReport,
        allocations: &mut std::collections::HashSet<(usize, usize)>,
        external_ranges: &mut HashMap<usize, usize>,
    ) -> Result<(), EngineError> {
        if self.events.capacity() == 0 {
            return Ok(());
        }
        report.mark_covered("input_retention_queue");
        report.add_estimate(
            self.events
                .capacity()
                .checked_mul(std::mem::size_of::<Event>())
                .ok_or_else(|| EngineError::Infrastructure("retention estimate overflow".into()))?,
        )?;
        report.mark_unknown("input_retention_queue_allocator_overhead");
        if self.events.is_empty() {
            return Ok(());
        }
        report.mark_covered("input_retention_arrow_buffers");
        report.mark_unknown("input_retention_batch_metadata");
        for event in &self.events {
            report.add_arrow_batch(&event.batch.batch, allocations, external_ranges)?;
            report.add_arrow_array(&event.batch.diff, allocations, external_ranges)?;
        }
        Ok(())
    }

    /// A disabled log: no input delta is retained.
    pub(super) fn disabled() -> Self {
        Self {
            capacity: None,
            byte_capacity: None,
            events: VecDeque::new(),
            truncated: false,
        }
    }

    /// Retains at most `events` deltas (at least one), plus optional bytes.
    pub(super) fn new(events: usize, byte_capacity: Option<usize>) -> Self {
        Self {
            capacity: Some(events.max(1)),
            byte_capacity,
            events: VecDeque::new(),
            truncated: false,
        }
    }

    /// Whether retention is on.
    pub(super) fn enabled(&self) -> bool {
        self.capacity.is_some()
    }

    /// Marks the log as no longer covering the run start, dropping its deltas.
    ///
    /// Used when state was restored from a checkpoint: the restored view state
    /// is correct, but the input history behind it was not persisted.
    pub(super) fn invalidate(&mut self) {
        self.truncated = true;
        self.events.clear();
        if self.byte_capacity.is_some() {
            self.events.shrink_to_fit();
        }
    }

    /// Records one applied input delta, dropping the oldest past the bound.
    pub(super) fn record(&mut self, input: InputId, batch: &ZSetBatch, watermark: i64) {
        let Some(capacity) = self.capacity else {
            return;
        };
        self.events.push_back(Event {
            input,
            batch: batch.clone(),
            watermark,
        });
        while self.events.len() > capacity {
            self.events.pop_front();
            self.truncated = true;
        }
        if let Some(limit) = self.byte_capacity {
            loop {
                if self.accounted_bytes().is_some_and(|bytes| bytes <= limit) {
                    break;
                }
                if self.events.pop_front().is_none() {
                    break;
                }
                self.truncated = true;
                // VecDeque retains its allocation after pop_front. Releasing it
                // is necessary for zero/tiny budgets to bound final accounting.
                self.events.shrink_to_fit();
            }
        }
    }

    pub(super) fn accounted_bytes(&self) -> Option<usize> {
        let mut report = super::memory::MemoryReport::default();
        self.memory_usage(
            &mut report,
            &mut Default::default(),
            &mut Default::default(),
        )
        .ok()?;
        Self::charged_report_bytes(&report)
    }

    pub(super) fn charged_report_bytes(report: &super::memory::MemoryReport) -> Option<usize> {
        report
            .arrow_buffer_capacity_bytes
            .checked_add(report.estimated_structural_bytes)
    }

    /// Rebuilds `graph`'s output by replaying the retained deltas in order.
    ///
    /// Errors when retention is off or no longer covers the whole run, so the
    /// caller rejects rather than returning a partial view.
    pub(super) fn replay(
        &self,
        graph: &mut ViewGraph,
        schemas: &HashMap<InputId, SchemaRef>,
        sources: &[InputId],
    ) -> Result<ViewOutput, EngineError> {
        if !self.enabled() {
            return Err(EngineError::Unsupported(
                "cannot build a view after START without input retention".into(),
            ));
        }
        if self.truncated {
            return Err(EngineError::Unsupported(
                "insufficient input retention to build a view after START".into(),
            ));
        }
        let mut watermarks: HashMap<InputId, i64> = HashMap::new();
        let mut output = ViewOutput::default();
        for event in &self.events {
            watermarks.insert(event.input, event.watermark);
            let watermark = watermark_for(sources, &watermarks);
            if let Some(delta) = graph.eval(
                event.input,
                &event.batch,
                schemas,
                watermark,
                0,
                0,
                false,
                None,
            )? {
                output.update(&delta)?;
            }
        }
        Ok(output)
    }
}

/// The view watermark during replay: the minimum over its sources' watermarks.
fn watermark_for(sources: &[InputId], watermarks: &HashMap<InputId, i64>) -> i64 {
    sources
        .iter()
        .map(|source| watermarks.get(source).copied().unwrap_or(0))
        .min()
        .unwrap_or(0)
}

#[cfg(test)]
mod quota_tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use hotlap_core::{InputId, ZSetBatch};

    use super::{Event, InputRetention};

    fn batch(rows: &[(i64, i64)]) -> ZSetBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let values = rows.iter().map(|row| row.0).collect::<Vec<_>>();
        let diffs = rows.iter().map(|row| row.1).collect::<Vec<_>>();
        ZSetBatch::new(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap(),
            Arc::new(Int64Array::from(diffs)),
        )
        .unwrap()
    }

    fn public_arrow_capacity(batch: &ZSetBatch) -> usize {
        batch
            .batch
            .columns()
            .iter()
            .flat_map(|array| {
                array
                    .to_data()
                    .buffers()
                    .iter()
                    .map(|buffer| buffer.capacity())
                    .collect::<Vec<_>>()
            })
            .chain(
                batch
                    .diff()
                    .to_data()
                    .buffers()
                    .iter()
                    .map(|buffer| buffer.capacity()),
            )
            .sum()
    }

    #[test]
    fn independently_calculated_byte_quota_is_exact_at_below_equal_and_above() {
        let input = batch(&[(17, 1)]);
        let mut unbounded = InputRetention::new(4, None);
        unbounded.record(InputId(0), &input, 0);
        let expected = unbounded.events.capacity() * std::mem::size_of::<Event>()
            + public_arrow_capacity(&input);

        for (limit, should_retain) in [
            (expected - 1, false),
            (expected, true),
            (expected + 1, true),
        ] {
            let mut bounded = InputRetention::new(4, Some(limit));
            bounded.record(InputId(0), &input, 0);
            assert_eq!(
                bounded.events.len(),
                usize::from(should_retain),
                "budget={limit}"
            );
            assert!(
                bounded
                    .accounted_bytes()
                    .is_some_and(|bytes| bytes <= limit)
            );
            if should_retain {
                assert_eq!(bounded.accounted_bytes(), Some(expected));
            }
        }
    }

    #[test]
    fn budget_retains_empty_owned_arrow_event_when_its_queue_fits() {
        let input = batch(&[]);
        let mut unbounded = InputRetention::new(4, None);
        unbounded.record(InputId(0), &input, 0);
        let queue_bytes = unbounded.events.capacity() * std::mem::size_of::<Event>();
        let mut bounded = InputRetention::new(4, Some(queue_bytes));

        bounded.record(InputId(0), &input, 0);

        assert_eq!(bounded.events.len(), 1);
        assert_eq!(bounded.accounted_bytes(), Some(queue_bytes));
    }
}
