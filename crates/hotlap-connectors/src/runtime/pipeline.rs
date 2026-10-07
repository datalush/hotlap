//! Source -> kernel pipeline: setup, stream merging and batch ingestion.

use hotlap::{ChangeBatch, Hotlap, HotlapError, Plan};

use crate::convert;
use crate::error::ConnectorError;
use crate::source::{Source, SourceBatch, SourceStream};

/// Event-time declaration for the pipeline's input.
#[derive(Clone, Copy, Debug)]
pub struct Watermark {
    pub time_col: usize,
    pub lag: i64,
}

/// Everything needed to run a source into the kernel.
pub struct Pipeline {
    pub input: String,
    pub source: Box<dyn Source>,
    pub watermark: Option<Watermark>,
    pub views: Vec<(String, Plan)>,
}

/// Register the input, optional watermark and views on `hotlap`.
pub fn setup(hotlap: &mut Hotlap, pipeline: &Pipeline) -> Result<(), ConnectorError> {
    hotlap.register_input(&pipeline.input).map_err(hotlap_err)?;
    if let Some(w) = pipeline.watermark {
        hotlap
            .declare_watermark(&pipeline.input, w.time_col, w.lag)
            .map_err(hotlap_err)?;
    }
    for (name, plan) in &pipeline.views {
        hotlap.create_view(name, plan.clone()).map_err(hotlap_err)?;
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
    convert::ensure_supported(sb.batch.schema().as_ref())?;
    let cb: ChangeBatch = convert::to_change_batch(&sb.batch)?;
    hotlap.push(input, &cb).map_err(hotlap_err)
}

fn hotlap_err(e: HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(e.0)
}
