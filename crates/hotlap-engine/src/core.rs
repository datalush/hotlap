//! Arrow-native [`IncrementalCore`]: persistent per-view operator graphs.

mod checkpoint;
mod graph;
pub mod ipc;
mod output;
mod restore;
mod retention;
#[cfg(test)]
mod tests;
mod time;
mod traits;
mod watermark;

use std::collections::{HashMap, HashSet};

use arrow::datatypes::SchemaRef;

use hotlap_core::plan::has_window;
use hotlap_core::{CoreError, InputId, Plan, SplitId, ViewId, WatermarkSpec, ZSetBatch};

use graph::ViewGraph;
use output::ViewOutput;
use retention::InputRetention;

/// A view's persistent graph plus its incremental output and pending changes.
pub(super) struct ViewState {
    pub(super) graph: ViewGraph,
    pub(super) plan: Plan,
    pub(super) windowed: bool,
    pub(super) tapped: bool,
    pub(in crate::core) output: ViewOutput,
    /// Buffered changelog for a tapped view: every propagated output delta is
    /// appended (via [`graph::accumulate`]) until [`take_changes`] drains it.
    /// An untapped view leaves it `None`, so it never grows with history.
    ///
    /// [`take_changes`]: crate::IncrementalCore::take_changes
    pub(super) pending: Option<ZSetBatch>,
}

impl ViewState {
    /// Builds a view's state from its graph, plan and initial output.
    pub(in crate::core) fn new(graph: ViewGraph, plan: Plan, output: ViewOutput) -> Self {
        Self {
            windowed: has_window(&plan),
            graph,
            plan,
            tapped: false,
            output,
            pending: None,
        }
    }
}

/// Differential-dataflow-free engine kernel.
///
/// Each view compiles to a persistent [`ViewGraph`] at `build_view`. A push
/// propagates only the pushed delta through the graphs that read the input;
/// stateful operators retain their state, so per-push work does not grow with
/// accumulated history. A view's output is an incremental map updated only for
/// the push delta (O(delta)); snapshots materialize and sort it once (O(state)).
///
/// A push is **not** transactional across views: each view mutates its state in
/// turn, so an error after an earlier view applied leaves a partial state. Such
/// a core is marked failed (the `failed` flag) and must be recreated; see
/// `ensure_healthy`.
pub struct EngineCore {
    pub(super) views: HashMap<ViewId, ViewState>,
    pub(super) registered: HashSet<InputId>,
    pub(super) specs: HashMap<InputId, WatermarkSpec>,
    /// Effective input watermark: the minimum across each input's splits.
    ///
    /// Unlike a single stream's watermark this is **not monotonic**: when a
    /// lagging split is first seen, the minimum can drop so its records are not
    /// dropped as late. Window operators clamp the value with `max`, so a
    /// lowered minimum never reopens a closed window.
    pub(super) watermarks: HashMap<InputId, i64>,
    /// Monotonic watermark per `(input, split)`: `max(event_ts) - lag`, clamped
    /// at zero. Declared splits are seeded at zero before the first push so a
    /// not-yet-seen split holds the input minimum back; a split never declared
    /// joins the map on its first batch instead.
    pub(super) split_watermarks: HashMap<(InputId, SplitId), i64>,
    pub(super) schemas: HashMap<InputId, SchemaRef>,
    pub(super) late: HashMap<InputId, u64>,
    pub(super) frozen: bool,
    /// Logical epoch: the number of deltas pushed so far.
    pub(super) epoch: u64,
    /// Applied input deltas kept for building views after `START`.
    pub(super) retention: InputRetention,
    /// Set once a push may have applied partially: a later view errored after an
    /// earlier view's state was already mutated. A failed core is poisoned so
    /// partial state is never observable; the caller must recreate the engine.
    pub(super) failed: bool,
}

impl EngineCore {
    /// Creates an empty core with no inputs, views or watermarks.
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
            registered: HashSet::new(),
            specs: HashMap::new(),
            watermarks: HashMap::new(),
            split_watermarks: HashMap::new(),
            schemas: HashMap::new(),
            late: HashMap::new(),
            frozen: false,
            epoch: 0,
            retention: InputRetention::disabled(),
            failed: false,
        }
    }

    /// Marks the core failed after a push that may have applied partially.
    ///
    /// The engine cannot safely continue (an earlier view may have committed a
    /// delta the failed view did not), so every state-observing call after this
    /// returns an explicit error until the caller recreates the core.
    pub(super) fn fail(&mut self) {
        self.failed = true;
    }

    /// Rejects state-observing work once a push may have applied partially.
    ///
    /// A failed core holds a mix of applied and unapplied views, so a snapshot
    /// or a further push would silently expose or compound that partial state.
    /// Returning an error forces the caller to recreate the engine instead.
    pub(super) fn ensure_healthy(&self) -> Result<(), CoreError> {
        if self.failed {
            return Err(CoreError::Infrastructure(
                "engine failed after a partially-applied push; recreate the engine".into(),
            ));
        }
        Ok(())
    }

    /// Freezes the schema: rejects mixed watermark declarations and windowed
    /// views without event-time, then blocks further declarations.
    pub(super) fn freeze(&mut self) -> Result<(), CoreError> {
        if !self.specs.is_empty() && self.specs.len() != self.registered.len() {
            return Err(CoreError::Unsupported(
                "cannot mix inputs with and without a declared watermark".into(),
            ));
        }
        let event_time = !self.specs.is_empty();
        if !event_time && self.views.values().any(|view| view.windowed) {
            return Err(CoreError::Unsupported(
                "tumbling windows require event-time inputs (declare_watermark)".into(),
            ));
        }
        self.frozen = true;
        Ok(())
    }

    /// Propagates one input's delta through the graph of view `id`.
    pub(super) fn push_view(
        &mut self,
        id: ViewId,
        input: InputId,
        delta: &ZSetBatch,
    ) -> Result<(), CoreError> {
        let watermark = self.view_watermark(id);
        let view = self
            .views
            .get_mut(&id)
            .ok_or_else(|| CoreError::Unsupported(format!("unknown view {id:?}")))?;
        let output = view
            .graph
            .eval(input, delta, &self.schemas, watermark)
            .map_err(CoreError::from)?;
        if let Some(output) = output {
            view.output.update(&output).map_err(CoreError::from)?;
            if view.tapped {
                view.pending =
                    graph::accumulate(view.pending.take(), &output).map_err(CoreError::from)?;
            }
        }
        Ok(())
    }

    /// Watermark a view's window closes against: the minimum of its sources'.
    fn view_watermark(&self, id: ViewId) -> i64 {
        let Some(view) = self.views.get(&id) else {
            return 0;
        };
        view.graph
            .sources()
            .iter()
            .map(|source| self.watermarks.get(source).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }
}

impl Default for EngineCore {
    fn default() -> Self {
        Self::new()
    }
}
