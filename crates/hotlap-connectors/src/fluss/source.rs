//! `Source` implementation backed by a Fluss log scanner.
//!
//! Records are assembled one-by-one from the per-record scanner so the broker
//! `timestamp` can be surfaced as an `Int64` `_event_time` column (ms).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use fluss::client::{EARLIEST_OFFSET, FlussConnection};
use fluss::config::Config;
use fluss::metadata::TablePath;
use futures::stream;

use crate::error::ConnectorError;
use crate::source::{Source, SourceBatch, SourceState, SourceStream, Split};

use super::assemble::{assemble, with_event_time};
use super::log_reader::FlussLogReader;
use super::{fluss_err, parse_path};

/// Poll window while waiting for new records; the stream polls repeatedly.
const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// A Fluss log-table source exposing per-record broker timestamps.
pub struct FlussSource {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    schema: SchemaRef,
    buckets: Vec<i32>,
    event_time_idx: usize,
    progress: Arc<Mutex<SourceState>>,
}

impl FlussSource {
    /// Connect to `bootstrap` and open `path` (`<database>/<table>`).
    pub async fn open_from_bootstrap(bootstrap: &str, path: &str) -> Result<Self, ConnectorError> {
        let config = Config {
            bootstrap_servers: bootstrap.to_string(),
            ..Config::default()
        };
        let connection = FlussConnection::new(config).await.map_err(fluss_err)?;
        Self::open(Arc::new(connection), parse_path(path)?).await
    }

    async fn open(
        connection: Arc<FlussConnection>,
        table_path: TablePath,
    ) -> Result<Self, ConnectorError> {
        let table = connection.get_table(&table_path).await.map_err(fluss_err)?;
        let info = table.get_table_info();
        if info.has_primary_key() {
            return Err(ConnectorError::Unsupported(
                "only append-only Fluss log tables are supported".into(),
            ));
        }
        if info.is_partitioned() {
            return Err(ConnectorError::Unsupported(
                "partitioned Fluss tables are not supported".into(),
            ));
        }
        let base_schema = table
            .new_scan()
            .create_record_batch_log_scanner()
            .map_err(fluss_err)?
            .schema();
        let buckets: Vec<i32> = (0..info.get_num_buckets()).collect();
        drop(table);

        let event_time_idx = base_schema.fields().len();
        let schema = with_event_time(&base_schema);
        let mut progress = SourceState::default();
        for &bucket in &buckets {
            progress.offsets.insert(bucket, EARLIEST_OFFSET);
        }
        Ok(Self {
            connection,
            table_path,
            schema,
            buckets,
            event_time_idx,
            progress: Arc::new(Mutex::new(progress)),
        })
    }
}

impl Source for FlussSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        let progress = self.progress.lock().expect("progress lock");
        Ok(self
            .buckets
            .iter()
            .map(|&id| Split {
                id,
                start: progress
                    .offsets
                    .get(&id)
                    .copied()
                    .unwrap_or(EARLIEST_OFFSET),
            })
            .collect())
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        if !self.buckets.contains(&split.id) {
            return Err(ConnectorError::Unsupported(format!(
                "unknown split {}",
                split.id
            )));
        }
        let task = ReadTask {
            connection: Arc::clone(&self.connection),
            table_path: self.table_path.clone(),
            full_schema: self.schema.clone(),
            bucket: split.id,
            start: split.start,
            reader: None,
        };
        let progress = Arc::clone(&self.progress);
        let bucket = split.id;
        let stream = stream::unfold(Some(task), move |state| {
            let progress = Arc::clone(&progress);
            async move { advance(state, progress, bucket).await }
        });
        Ok(Box::pin(stream))
    }

    fn state(&self) -> SourceState {
        self.progress.lock().expect("progress lock").clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(self.event_time_idx)
    }
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
        let subscription = [(task.bucket, task.start)];
        match FlussLogReader::open(&task.connection, &task.table_path, &subscription).await {
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
            Ok(records) if records.is_empty() => continue,
            Ok(records) => {
                let base_schema = task.reader.as_ref().expect("reader opened").schema();
                let batch = match assemble(&records, &base_schema, &task.full_schema) {
                    Ok(batch) => batch,
                    Err(error) => return Some((Err(error), None)),
                };
                let base_offset = records.first().map_or(0, |record| record.offset);
                let next = base_offset + batch.num_rows() as i64;
                progress
                    .lock()
                    .expect("progress lock")
                    .offsets
                    .insert(bucket, next);
                return Some((Ok(SourceBatch { batch, base_offset }), Some(task)));
            }
        }
    }
}
