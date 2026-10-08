//! Configuration for opening an embedded [`Session`](crate::Session).

use std::sync::Arc;
use std::time::Duration;

use hotlap::state::StateBackend;

use crate::session::{FlussSinkFactory, FlussSourceFactory, SinkFactory, SourceFactory};

/// Periodic checkpointing passed to the engine at `START`.
pub struct CheckpointSpec {
    /// Minimum time between periodic checkpoints.
    pub interval: Duration,
    /// Number of newest checkpoints to keep.
    pub retain: usize,
    /// Destination store, moved into the engine thread.
    pub backend: Box<dyn StateBackend + Send>,
}

/// Settings for [`Session::open`](crate::Session::open).
pub struct SessionConfig {
    source_factory: Arc<dyn SourceFactory>,
    sink_factory: Arc<dyn SinkFactory>,
    checkpoint: Option<CheckpointSpec>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionConfig {
    /// A config using the default Fluss factories and no checkpointing.
    pub fn new() -> Self {
        Self {
            source_factory: Arc::new(FlussSourceFactory),
            sink_factory: Arc::new(FlussSinkFactory),
            checkpoint: None,
        }
    }

    /// Build sources with `factory` (tests inject fakes here).
    pub fn with_source_factory(mut self, factory: Arc<dyn SourceFactory>) -> Self {
        self.source_factory = factory;
        self
    }

    /// Build sinks with `factory` (tests inject fakes here).
    pub fn with_sink_factory(mut self, factory: Arc<dyn SinkFactory>) -> Self {
        self.sink_factory = factory;
        self
    }

    /// Enable periodic checkpointing to `backend`, keeping `retain` newest.
    pub fn with_checkpoint(
        mut self,
        interval: Duration,
        retain: usize,
        backend: Box<dyn StateBackend + Send>,
    ) -> Self {
        self.checkpoint = Some(CheckpointSpec {
            interval,
            retain,
            backend,
        });
        self
    }

    /// Split into the pieces [`Session::open`](crate::Session::open) consumes.
    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<dyn SourceFactory>,
        Arc<dyn SinkFactory>,
        Option<CheckpointSpec>,
    ) {
        (self.source_factory, self.sink_factory, self.checkpoint)
    }
}
