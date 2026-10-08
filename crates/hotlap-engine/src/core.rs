//! Arrow-native [`IncrementalCore`]: persistent per-view operator graphs.

mod graph;
mod traits;

use std::collections::{HashMap, HashSet};

use arrow::array::BooleanArray;
use arrow::datatypes::SchemaRef;

use hotlap_core::{CoreError, InputId, ViewId, WatermarkSpec, ZSetBatch};

use crate::error::EngineError;
use crate::ops::filter;
use crate::zset::int64_diffs;

use graph::ViewGraph;

/// A view's persistent graph plus its accumulated output and pending changes.
pub(super) struct ViewState {
    pub(super) graph: ViewGraph,
    pub(super) windowed: bool,
    pub(super) tapped: bool,
    pub(super) output: Option<ZSetBatch>,
    pub(super) pending: Option<ZSetBatch>,
}

/// Differential-dataflow-free engine kernel.
///
/// Each view compiles to a persistent [`ViewGraph`] at `build_view`. A push
/// propagates only the pushed delta through the graphs that read the input;
/// stateful operators retain their state, so per-push work does not grow with
/// accumulated history. Snapshots consolidate the accumulated output deltas.
pub struct EngineCore {
    pub(super) views: HashMap<ViewId, ViewState>,
    pub(super) registered: HashSet<InputId>,
    pub(super) specs: HashMap<InputId, WatermarkSpec>,
    pub(super) watermarks: HashMap<InputId, i64>,
    pub(super) schemas: HashMap<InputId, SchemaRef>,
    pub(super) late: HashMap<InputId, u64>,
    pub(super) frozen: bool,
}

impl EngineCore {
    /// Creates an empty core with no inputs, views or watermarks.
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
            registered: HashSet::new(),
            specs: HashMap::new(),
            watermarks: HashMap::new(),
            schemas: HashMap::new(),
            late: HashMap::new(),
            frozen: false,
        }
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

    /// Drops late insertions and advances `input`'s logical watermark.
    ///
    /// Only insertions (`diff > 0`) below the current watermark are dropped; a
    /// retraction must always be applied or downstream state would be corrupted.
    pub(super) fn filter_late(
        &mut self,
        input: InputId,
        batch: &ZSetBatch,
    ) -> Result<ZSetBatch, EngineError> {
        if self.specs.is_empty() {
            return Ok(batch.clone());
        }
        let spec = self.specs.get(&input).copied().ok_or_else(|| {
            EngineError::Unsupported(format!("input {input:?} has no watermark"))
        })?;
        let current = *self.watermarks.get(&input).unwrap_or(&0);
        let times = time_values(&batch.batch, spec.time_col)?;
        let diffs = int64_diffs(batch.diff())?;
        let mut mask = Vec::with_capacity(batch.len());
        let mut max_ts = i64::MIN;
        for index in 0..batch.len() {
            let ts = times.value(index);
            max_ts = max_ts.max(ts);
            let late = diffs.value(index) > 0 && ts < current;
            if late {
                *self.late.entry(input).or_insert(0) += 1;
            }
            mask.push(!late);
        }
        let kept = filter(batch, &BooleanArray::from(mask))?;
        let next = if kept.is_empty() {
            current
        } else {
            current.max((max_ts - spec.lag).max(0))
        };
        self.watermarks.insert(input, next);
        Ok(kept)
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
            view.output = graph::accumulate(view.output.take(), &output).map_err(CoreError::from)?;
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

/// Reads an event-time column as non-negative `i64`; nulls map to zero.
fn time_values(
    batch: &arrow::record_batch::RecordBatch,
    col: usize,
) -> Result<arrow::array::Int64Array, EngineError> {
    use arrow::compute::cast;
    use arrow::datatypes::DataType;

    let column = batch.columns().get(col).ok_or_else(|| {
        EngineError::Unsupported(format!("time column {col} out of range"))
    })?;
    let casted = cast(column.as_ref(), &DataType::Int64)?;
    let ints = casted
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| EngineError::Infrastructure("int64 cast produced wrong type".to_string()))?;
    Ok(arrow::array::Int64Array::from(
        ints.iter()
            .map(|value| value.unwrap_or(0).max(0))
            .collect::<Vec<_>>(),
    ))
}
