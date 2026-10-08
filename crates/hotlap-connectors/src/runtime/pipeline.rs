//! Source -> kernel pipeline: setup, stream merging and batch ingestion.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use hotlap::{Hotlap, HotlapError, Plan};

use crate::convert;
use crate::error::ConnectorError;
use crate::runtime::checkpoint::CheckpointConfig;
use crate::runtime::sink::SinkPump;
use crate::sink::Sink;
use crate::source::{Source, SourceBatch, SourceStream, Split};

/// Event-time declaration for the pipeline's input.
#[derive(Clone, Copy, Debug)]
pub struct Watermark {
    pub lag: i64,
}

/// A view tapped into a sink's changelog channel.
pub struct SinkSpec {
    pub view: String,
    pub sink: Arc<dyn Sink>,
}

/// Everything needed to run a source into the kernel.
pub struct Pipeline {
    pub input: String,
    pub source: Box<dyn Source>,
    pub watermark: Option<Watermark>,
    pub views: Vec<(String, Plan)>,
    pub sinks: Vec<SinkSpec>,
    /// Periodic checkpoint settings; `None` disables checkpointing.
    pub checkpoint: Option<CheckpointConfig>,
    /// Maximum input deltas retained for views created after `START`; `None`
    /// disables retention, so post-start views are rejected.
    pub retention: Option<usize>,
}

/// Register the input, optional watermark and views on `hotlap`.
///
/// The event-time column index is owned by the source, so it is derived from
/// `Source::event_time_column` rather than supplied by the caller.
pub fn setup(hotlap: &mut Hotlap, pipeline: &Pipeline) -> Result<(), ConnectorError> {
    hotlap.register_input(&pipeline.input).map_err(hotlap_err)?;
    if let Some(w) = pipeline.watermark {
        let time_col = pipeline
            .source
            .event_time_column()
            .ok_or_else(|| ConnectorError::Unsupported("source has no event-time column".into()))?;
        hotlap
            .declare_watermark(&pipeline.input, time_col, w.lag)
            .map_err(hotlap_err)?;
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

/// Merge the per-split streams into a single stream.
pub fn merged_stream(source: &dyn Source) -> Result<SourceStream, ConnectorError> {
    merged_stream_from(source, &source.splits()?)
}

/// Merge `splits` into a single stream, reading each one from its `start`.
pub fn merged_stream_from(
    source: &dyn Source,
    splits: &[Split],
) -> Result<SourceStream, ConnectorError> {
    let mut streams = Vec::with_capacity(splits.len());
    for split in splits {
        streams.push(source.read(split)?);
    }
    Ok(Box::pin(futures::stream::select_all(streams)))
}

/// Convert one source batch and push it, tagged with its source split, into the
/// input.
pub fn ingest(hotlap: &mut Hotlap, input: &str, sb: &SourceBatch) -> Result<(), ConnectorError> {
    let zset = convert::to_zset(&sb.batch)?;
    hotlap
        .push_split(input, sb.split, &zset)
        .map_err(hotlap_err)
}

/// Ingest one source item and, if it succeeded, pump its deltas into the sinks.
///
/// Returns whether the source branch is finished (exhausted, errored or the
/// sink channel broke), which disables that branch of the select loop.
pub(crate) async fn feed_source(
    hotlap: &mut Hotlap,
    pipeline: &Pipeline,
    sinks: &SinkPump,
    item: Option<Result<SourceBatch, ConnectorError>>,
    last_error: &Mutex<Option<String>>,
    built: &AtomicBool,
) -> bool {
    if on_source_item(hotlap, pipeline, item, last_error, built) {
        return true;
    }
    match sinks.pump(hotlap).await {
        Ok(()) => false,
        Err(error) => {
            record_error(last_error, error);
            true
        }
    }
}

/// Ingest one source item, recording any error and reporting source completion.
fn on_source_item(
    hotlap: &mut Hotlap,
    pipeline: &Pipeline,
    item: Option<Result<SourceBatch, ConnectorError>>,
    last_error: &Mutex<Option<String>>,
    built: &AtomicBool,
) -> bool {
    match item {
        Some(Ok(sb)) => match ingest(hotlap, &pipeline.input, &sb) {
            Ok(()) => {
                // The first successful push builds the dataflow, so views become
                // readable even if later pushes add no rows.
                built.store(true, Ordering::SeqCst);
                // Ack only now, so the source never reports a batch as applied
                // before the engine has actually ingested it.
                match pipeline.source.commit(sb.split, sb.next_offset) {
                    Ok(()) => false,
                    Err(error) => {
                        record_error(last_error, error);
                        true
                    }
                }
            }
            Err(error) => {
                record_error(last_error, error);
                true
            }
        },
        Some(Err(error)) => {
            record_error(last_error, error);
            true
        }
        None => true,
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
