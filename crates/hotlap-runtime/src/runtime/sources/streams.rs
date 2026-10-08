//! Identity-tagged event stream over the inputs of a [`Sources`] set.
//!
//! [`Sources`]: super::Sources

use std::pin::Pin;

use futures::{Stream, StreamExt};
use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{SourceBatch, Split};

use super::InputSource;

/// A source batch tagged with the input that produced it.
#[derive(Clone, Debug)]
pub struct SourceEvent {
    pub input: InputId,
    pub batch: SourceBatch,
}

/// Merged stream over every split of every source, tagged with its input.
pub type InputStream = Pin<Box<dyn Stream<Item = Result<SourceEvent, ConnectorError>> + Send>>;

/// Open each source's own splits and merge the streams with `select_all`.
pub(super) fn tagged_stream(entries: &[InputSource]) -> Result<InputStream, ConnectorError> {
    let mut splits = Vec::with_capacity(entries.len());
    for entry in entries {
        splits.push(entry.source.splits()?);
    }
    tagged_from(entries, &splits)
}

/// Merge explicitly resolved `splits` (one list per entry) with `select_all`.
///
/// Recovery resolves each source's resume offsets before calling this, so the
/// entry order and the splits slice must line up one-to-one.
///
/// A read failure is returned as an error and aborts the merge; it is never
/// turned into a normal end or dropped, so a missing stream surfaces loudly.
pub(super) fn tagged_from(
    entries: &[InputSource],
    splits: &[Vec<Split>],
) -> Result<InputStream, ConnectorError> {
    let mut streams = Vec::new();
    for (entry, splits) in entries.iter().zip(splits) {
        let input = entry.id;
        for split in splits {
            let read = entry.source.read(split)?;
            streams.push(read.map(move |item| item.map(|batch| SourceEvent { input, batch })));
        }
    }
    Ok(Box::pin(futures::stream::select_all(streams)))
}
