//! Source -> kernel pipeline: setup, stream merging and batch ingestion.

use std::sync::Arc;

use hotlap::{Hotlap, HotlapError, Plan};

use crate::convert;
use crate::error::ConnectorError;
use crate::runtime::checkpoint::CheckpointConfig;
use crate::sink::Sink;
use crate::source::{Source, SourceBatch, SourceStream};

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
    let splits = source.splits()?;
    let mut streams = Vec::with_capacity(splits.len());
    for split in &splits {
        streams.push(source.read(split)?);
    }
    Ok(Box::pin(futures::stream::select_all(streams)))
}

/// Convert one source batch and push it into the input.
pub fn ingest(hotlap: &mut Hotlap, input: &str, sb: &SourceBatch) -> Result<(), ConnectorError> {
    let zset = convert::to_zset(&sb.batch)?;
    hotlap.push(input, &zset).map_err(hotlap_err)
}

fn hotlap_err(e: HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(e.0)
}
