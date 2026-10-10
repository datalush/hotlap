//! Source -> kernel pipeline: setup, stream merging and batch ingestion.

mod preflight;
mod sink_spec;

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use hotlap::{Hotlap, HotlapError, Plan};

use crate::runtime::checkpoint::CheckpointConfig;
use crate::runtime::sink::SinkPump;
use crate::runtime::sources::{SourceEvent, Sources};
use hotlap_connectors::convert;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::SplitId;

/// Event-time declaration for one input source.
#[derive(Clone, Copy, Debug)]
pub struct Watermark {
    pub lag: i64,
}

/// A view tapped into a sink's changelog channel.
pub use sink_spec::{SinkDescription, SinkSpec};

/// Everything needed to run a set of sources into the kernel.
pub struct Pipeline {
    pub sources: Sources,
    pub views: Vec<(String, Plan)>,
    pub sinks: Vec<SinkSpec>,
    /// Periodic checkpoint settings; `None` disables checkpointing.
    pub checkpoint: Option<CheckpointConfig>,
    /// Maximum input deltas retained for views created after `START`; `None`
    /// disables retention, so post-start views are rejected.
    pub retention: Option<usize>,
}

impl Pipeline {
    /// Validate the sink wiring before any writer, stream or tap starts.
    ///
    /// Returns an explicit error for two out-of-scope configurations: two sinks
    /// tapping the same view (the changelog is drained per view, so a second
    /// sink would silently receive nothing) and a sink that cannot apply
    /// retractions on a view whose plan may retract (the failure would
    /// otherwise surface per batch, after writes or ingestion began).
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let mut claimed: HashSet<&str> = HashSet::new();
        for spec in &self.sinks {
            if !claimed.insert(spec.view.as_str()) {
                return Err(ConnectorError::Unsupported(format!(
                    "view `{}` already has a sink; fan-out is not supported",
                    spec.view
                )));
            }
        }
        for spec in &self.sinks {
            let plan = self
                .views
                .iter()
                .find(|(name, _)| name == &spec.view)
                .map(|(_, plan)| plan);
            if let Some(plan) = plan
                && hotlap::plan::may_retract(plan)
                && !spec.sink.accepts_retractions()
            {
                return Err(ConnectorError::Unsupported(format!(
                    "sink on view `{}` cannot apply retractions; its plan may retract",
                    spec.view
                )));
            }
        }
        Ok(())
    }
}

/// Outcome of feeding one source item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedStatus {
    /// The item was applied; the loop can keep serving.
    Continue,
    /// The source produced no more items; ingestion ends without error.
    Exhausted,
    /// A read, ingest or ack failure stopped the runtime.
    Failed,
}

/// Register every input, optional watermark and views on `hotlap`.
///
/// The event-time column index is owned by each source, so it is derived from
/// `Source::event_time_column` rather than supplied by the caller. The inputs
/// are all registered before any view or tap is built, so every `Plan::Source`
/// is resolvable regardless of declaration order.
pub fn setup(hotlap: &mut Hotlap, pipeline: &Pipeline) -> Result<(), ConnectorError> {
    for entry in pipeline.sources.entries() {
        hotlap
            .register_input_with_id(&entry.name, entry.id)
            .map_err(hotlap_err)?;
        if let Some(w) = entry.watermark {
            let time_col = entry.source.event_time_column().ok_or_else(|| {
                ConnectorError::Unsupported("source has no event-time column".into())
            })?;
            hotlap
                .declare_watermark(&entry.name, time_col, w.lag)
                .map_err(hotlap_err)?;
            // Register every declared split before the first push so the input
            // watermark cannot advance past a split that has not produced a batch.
            let splits = entry.source.splits()?;
            let ids: Vec<SplitId> = splits.iter().map(|split| split.id).collect();
            hotlap
                .declare_splits(&entry.name, &ids)
                .map_err(hotlap_err)?;
        }
    }
    for (name, plan) in &pipeline.views {
        hotlap.create_view(name, plan.clone()).map_err(hotlap_err)?;
    }
    // Tapping must happen before the first push, so it belongs in setup.
    for spec in &pipeline.sinks {
        hotlap.tap_view(&spec.view).map_err(hotlap_err)?;
    }
    Ok(())
}

/// Convert one tagged event and push it into the input that produced it.
pub fn ingest_event(
    hotlap: &mut Hotlap,
    sources: &Sources,
    event: &SourceEvent,
) -> Result<(), ConnectorError> {
    let entry = sources.get(event.input)?;
    let zset = convert::to_zset(&event.batch.batch)?;
    hotlap
        .push_split(&entry.name, event.batch.split, &zset)
        .map_err(hotlap_err)
}

/// Ingest one source item and, if it succeeded, pump its deltas into the sinks.
///
/// A read or ingest error becomes [`FeedStatus::Failed`]; a closed source
/// becomes [`FeedStatus::Exhausted`]. Both stop the failing branch, but only a
/// failure forbids later checkpoints and view builds.
pub(crate) async fn feed_source(
    hotlap: &mut Hotlap,
    sources: &Sources,
    sinks: &SinkPump,
    item: Option<Result<SourceEvent, ConnectorError>>,
    last_error: &Mutex<Option<String>>,
    built: &AtomicBool,
) -> FeedStatus {
    match on_source_item(hotlap, sources, item, last_error, built) {
        FeedStatus::Failed => return FeedStatus::Failed,
        FeedStatus::Exhausted => return FeedStatus::Exhausted,
        FeedStatus::Continue => {}
    }
    match sinks.pump(hotlap).await {
        Ok(()) => FeedStatus::Continue,
        Err(error) => {
            record_error(last_error, error);
            FeedStatus::Failed
        }
    }
}

/// Ingest one source item, recording any error and reporting source completion.
fn on_source_item(
    hotlap: &mut Hotlap,
    sources: &Sources,
    item: Option<Result<SourceEvent, ConnectorError>>,
    last_error: &Mutex<Option<String>>,
    built: &AtomicBool,
) -> FeedStatus {
    match item {
        Some(Ok(event)) => match ingest_event(hotlap, sources, &event) {
            Ok(()) => {
                // The first successful push builds the dataflow, so views become
                // readable even if later pushes add no rows.
                built.store(true, Ordering::SeqCst);
                // Ack only now, so the source never reports a batch as applied
                // before the engine has actually ingested it, and only on the
                // source that produced it.
                match sources.get(event.input).and_then(|entry| {
                    entry
                        .source
                        .commit(event.batch.split, event.batch.next_offset)
                }) {
                    Ok(()) => FeedStatus::Continue,
                    Err(error) => {
                        record_error(last_error, error);
                        FeedStatus::Failed
                    }
                }
            }
            Err(error) => {
                record_error(last_error, error);
                FeedStatus::Failed
            }
        },
        Some(Err(error)) => {
            record_error(last_error, error);
            FeedStatus::Failed
        }
        None => FeedStatus::Exhausted,
    }
}

/// Store the first error written to `slot`, ignoring later ones.
pub(crate) fn record_error(slot: &Mutex<Option<String>>, error: ConnectorError) {
    if let Ok(mut value) = slot.lock() {
        value.get_or_insert_with(|| error.to_string());
    }
}

fn hotlap_err(e: HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(e.0)
}
