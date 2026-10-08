//! Identity-tagged event stream over the inputs of a [`Sources`] set.
//!
//! [`Sources`]: super::Sources

use std::pin::Pin;

use futures::{Stream, StreamExt};
use hotlap::InputId;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::SourceBatch;

use super::InputSource;

/// A source batch tagged with the input that produced it.
#[derive(Clone, Debug)]
pub struct SourceEvent {
    pub input: InputId,
    pub batch: SourceBatch,
}

/// Merged stream over every split of every source, tagged with its input.
pub type InputStream = Pin<Box<dyn Stream<Item = Result<SourceEvent, ConnectorError>> + Send>>;

/// Open each split and merge the resulting streams with `select_all`.
///
/// A read failure is returned as an error and aborts the merge; it is never
/// turned into a normal end or dropped, so a missing stream surfaces loudly.
pub(super) fn tagged_stream(entries: &[InputSource]) -> Result<InputStream, ConnectorError> {
    let mut streams = Vec::new();
    for entry in entries {
        let input = entry.id;
        for split in entry.source.splits()? {
            let read = entry.source.read(&split)?;
            streams.push(read.map(move |item| item.map(|batch| SourceEvent { input, batch })));
        }
    }
    Ok(Box::pin(futures::stream::select_all(streams)))
}
