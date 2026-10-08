//! Input Z-set retention, so a view can be built after `START`.
//!
//! A view created after the first push is compiled to a fresh graph and
//! evaluated over the retained input deltas in their original push order,
//! reproducing the state of a view that had been built before the first push.
//! Retention is bounded by a maximum number of deltas; once the bound is
//! exceeded the log no longer covers the start of the run and a post-start
//! build is rejected instead of returning a silently truncated view.

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
    /// A disabled log: no input delta is retained.
    pub(super) fn disabled() -> Self {
        Self {
            capacity: None,
            events: VecDeque::new(),
            truncated: false,
        }
    }

    /// Retains at most `events` deltas (at least one).
    pub(super) fn new(events: usize) -> Self {
        Self {
            capacity: Some(events.max(1)),
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
            if let Some(delta) = graph.eval(event.input, &event.batch, schemas, watermark)? {
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
