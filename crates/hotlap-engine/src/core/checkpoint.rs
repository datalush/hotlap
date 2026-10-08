//! Capture a live engine's durable state as an [`EngineSnapshot`].

use hotlap_core::snapshot::{
    ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot, InputSnapshot, SnapshotTable, ViewSnapshot,
};
use hotlap_core::{InputId, SplitId, ViewId, ZSetBatch};

use super::EngineCore;
use super::ipc::{encode_schema, encode_zset};
use crate::error::EngineError;

impl EngineCore {
    /// Captures every registered input and built view into a versioned snapshot.
    ///
    /// Fails once a push may have applied partially: a snapshot of that state
    /// would capture a mix of applied and unapplied views, and recovery would
    /// faithfully restore the corruption.
    pub fn checkpoint(&self) -> Result<EngineSnapshot, EngineError> {
        self.ensure_healthy()?;
        let snapshot = EngineSnapshot {
            format_version: ENGINE_SNAPSHOT_FORMAT_VERSION,
            epoch: self.epoch,
            frozen: self.frozen,
            inputs: self.input_snapshots()?,
            views: self.view_snapshots()?,
        };
        self.metrics.registry.inc("checkpoints_taken");
        Ok(snapshot)
    }

    /// Snapshot of every registered input, ordered by id.
    fn input_snapshots(&self) -> Result<Vec<InputSnapshot>, EngineError> {
        let mut ids: Vec<InputId> = self.registered.iter().copied().collect();
        ids.sort();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let schema = match self.schemas.get(&id) {
                Some(schema) => Some(encode_schema(schema)?),
                None => None,
            };
            out.push(InputSnapshot {
                id,
                schema,
                spec: self.specs.get(&id).copied(),
                watermark: self.watermarks.get(&id).copied().unwrap_or(0),
                splits: self.split_snapshot(id),
                late: self.late.get(&id).copied().unwrap_or(0),
            });
        }
        Ok(out)
    }

    /// Per-split watermarks of `input`, ordered by split id.
    fn split_snapshot(&self, input: InputId) -> Vec<(SplitId, i64)> {
        let mut splits: Vec<(SplitId, i64)> = self
            .split_watermarks
            .iter()
            .filter(|((owner, _), _)| *owner == input)
            .map(|(&(_, split), &watermark)| (split, watermark))
            .collect();
        splits.sort();
        splits
    }

    /// Snapshot of every built view, ordered by id.
    fn view_snapshots(&self) -> Result<Vec<ViewSnapshot>, EngineError> {
        let mut ids: Vec<ViewId> = self.views.keys().copied().collect();
        ids.sort();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let state = &self.views[&id];
            out.push(ViewSnapshot {
                id,
                plan: state.plan.clone(),
                windowed: state.windowed,
                tapped: state.tapped,
                output: encode_optional(state.output.to_snapshot()?)?,
                pending: state.pending.as_ref().map(encode_zset).transpose()?,
                operators: state.graph.export_states()?,
            });
        }
        Ok(out)
    }
}

/// Encodes an optional Z-set, preserving `None`.
fn encode_optional(zset: Option<ZSetBatch>) -> Result<Option<SnapshotTable>, EngineError> {
    match zset {
        Some(zset) => Ok(Some(encode_zset(&zset)?)),
        None => Ok(None),
    }
}
