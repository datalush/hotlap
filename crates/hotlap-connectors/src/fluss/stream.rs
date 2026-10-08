//! Lazy tailing read for one Fluss bucket: open on demand, poll repeatedly,
//! assemble one `SourceBatch` per non-empty poll.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use futures::stream;

use crate::error::ConnectorError;
use crate::source::{SourceBatch, SourceState, SourceStream};

use super::assemble::assemble;
use super::lock_progress;
use super::log_reader::FlussLogReader;

/// Poll window while waiting for new records; the stream polls repeatedly.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Build a stream that tail-reads `bucket` from `start` into assembled batches.
pub(crate) fn read_bucket(
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    full_schema: SchemaRef,
    bucket: i32,
    start: i64,
    progress: Arc<Mutex<SourceState>>,
) -> SourceStream {
    let task = ReadTask {
        connection,
        table_path,
        full_schema,
        bucket,
        start,
        reader: None,
    };
    let stream = stream::unfold(Some(task), move |state| {
        let progress = Arc::clone(&progress);
        async move { advance(state, progress, bucket).await }
    });
    Box::pin(stream)
}

/// Stream state: the scanner is opened lazily on the first poll.
struct ReadTask {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    full_schema: SchemaRef,
    bucket: i32,
    start: i64,
    reader: Option<FlussLogReader>,
}

/// One unfold step: open on demand, poll until records arrive, assemble a batch.
async fn advance(
    state: Option<ReadTask>,
    progress: Arc<Mutex<SourceState>>,
    bucket: i32,
) -> Option<(Result<SourceBatch, ConnectorError>, Option<ReadTask>)> {
    let mut task = state?;
    if task.reader.is_none() {
        let opened =
            FlussLogReader::open(&task.connection, &task.table_path, task.bucket, task.start).await;
        match opened {
            Ok(reader) => task.reader = Some(reader),
            Err(error) => return Some((Err(error), None)),
        }
    }
    loop {
        let polled = {
            let reader = task.reader.as_mut().expect("reader opened");
            reader.poll(POLL_TIMEOUT).await
        };
        match polled {
            Err(error) => return Some((Err(error), None)),
            // An empty poll means no new records yet: keep tailing this bucket.
            Ok(records) if records.is_empty() => continue,
            Ok(records) => {
                let base_schema = task.reader.as_ref().expect("reader opened").schema();
                let batch = match assemble(&records, &base_schema, &task.full_schema) {
                    Ok(batch) => batch,
                    Err(error) => return Some((Err(error), None)),
                };
                let base_offset = records.first().map_or(0, |record| record.offset);
                let last_offset = records.last().map_or(base_offset, |record| record.offset);
                // Advance the read position as the batch is produced. The engine
                // only snapshots `state()` between polls, after the previous
                // batch was ingested, so a checkpoint never persists an offset
                // ahead of the records it has applied.
                match lock_progress(&progress) {
                    Ok(mut guard) => {
                        guard.offsets.insert(bucket, last_offset + 1);
                    }
                    Err(error) => return Some((Err(error), None)),
                }
                return Some((Ok(SourceBatch { batch, base_offset }), Some(task)));
            }
        }
    }
}
