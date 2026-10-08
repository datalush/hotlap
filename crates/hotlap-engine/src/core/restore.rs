//! Restore a live engine from an [`EngineSnapshot`].

use hotlap_core::snapshot::{
    ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot, InputSnapshot, ViewSnapshot,
};

use super::graph::ViewGraph;
use super::ipc::{decode_schema, decode_zset};
use super::output::ViewOutput;
use super::{EngineCore, ViewState};
use crate::error::EngineError;

impl EngineCore {
    /// Rebuilds this engine's state from `snapshot`.
    ///
    /// Rejects an unknown format version. The engine is only mutated once the
    /// whole snapshot has been decoded, so a corrupt snapshot leaves it intact.
    pub fn restore(&mut self, snapshot: &EngineSnapshot) -> Result<(), EngineError> {
        if snapshot.format_version != ENGINE_SNAPSHOT_FORMAT_VERSION {
            return Err(EngineError::Unsupported(format!(
                "unknown engine snapshot format version {} (expected {})",
                snapshot.format_version, ENGINE_SNAPSHOT_FORMAT_VERSION
            )));
        }
        let mut core = EngineCore::new();
        core.frozen = snapshot.frozen;
        core.epoch = snapshot.epoch;
        for input in &snapshot.inputs {
            core.restore_input(input)?;
        }
        for view in &snapshot.views {
            core.restore_view(view)?;
        }
        // Retention settings survive, but the input history behind the restored
        // state was not persisted, so a post-start build must be rejected. The
        // take happens once decoding succeeded, keeping restore all-or-nothing.
        core.retention = std::mem::take(&mut self.retention);
        core.retention.invalidate();
        *self = core;
        Ok(())
    }

    /// Restores one source's registration, schema, watermark and counters.
    fn restore_input(&mut self, input: &InputSnapshot) -> Result<(), EngineError> {
        self.registered.insert(input.id);
        if let Some(bytes) = &input.schema {
            self.schemas.insert(input.id, decode_schema(bytes)?);
        }
        if let Some(spec) = input.spec {
            self.specs.insert(input.id, spec);
        }
        self.watermarks.insert(input.id, input.watermark);
        self.late.insert(input.id, input.late);
        Ok(())
    }

    /// Rebuilds one view's graph, output and pending changelog.
    fn restore_view(&mut self, view: &ViewSnapshot) -> Result<(), EngineError> {
        let mut graph = ViewGraph::build(&view.plan);
        graph.import_states(&view.operators)?;
        let mut output = ViewOutput::default();
        if let Some(table) = &view.output {
            output.update(&decode_zset(table)?)?;
        }
        let pending = match &view.pending {
            Some(table) => Some(decode_zset(table)?),
            None => None,
        };
        self.views.insert(
            view.id,
            ViewState {
                graph,
                plan: view.plan.clone(),
                windowed: view.windowed,
                tapped: view.tapped,
                output,
                pending,
            },
        );
        Ok(())
    }
}
