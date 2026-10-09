//! Recovery preflight: reject an incompatible view registry before any sink
//! writer or pump is started.

use hotlap::ViewId;
use hotlap_connectors::error::ConnectorError;
use std::sync::Arc;

use super::Pipeline;
use crate::runtime::checkpoint::{CheckpointConfig, Checkpointer};
use crate::runtime::sink::{SharedSink, SinkSync};
use crate::runtime::source_checkpoint::SavedView;

impl Pipeline {
    /// Validate the checkpoint's named views against the declared views.
    ///
    /// Views are declared in order, so a view's handle is its position. This
    /// borrows the checkpoint store without consuming the sources and puts the
    /// config back, so a rejected pipeline still owns its durable checkpoint and
    /// no sink pump or writer is ever started.
    pub fn preflight_recovery(&mut self) -> Result<(), ConnectorError> {
        let Some(config) = self.checkpoint.take() else {
            return Ok(());
        };
        let declared: Vec<SavedView> = self
            .views
            .iter()
            .enumerate()
            .map(|(index, (name, plan))| SavedView {
                name: name.clone(),
                id: ViewId(index as u32),
                plan: plan.clone(),
            })
            .collect();
        let checkpointer = Checkpointer::new(config.backend, config.retain);
        let checkpointer = checkpointer.with_sinks(
            self.sinks
                .iter()
                .map(|spec| SinkSync::sink_only(SharedSink::new(Arc::clone(&spec.sink))))
                .collect(),
        );
        let result = checkpointer.validate_views(&declared);
        self.checkpoint = Some(CheckpointConfig {
            interval: config.interval,
            retain: config.retain,
            backend: checkpointer.into_backend(),
        });
        result
    }
}
