//! `Source` implementation backed by a Fluss log scanner.
//!
//! Records are assembled one-by-one from the per-record scanner so the broker
//! `timestamp` can be surfaced as an `Int64` `_event_time` column (ms).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use fluss::client::{EARLIEST_OFFSET, FlussConnection};
use fluss::config::Config;
use fluss::metadata::TablePath;

use crate::error::ConnectorError;
use crate::source::{Offset, Source, SourceState, SourceStream, Split, SplitId};

use super::assemble::with_event_time;
use super::lock_progress;
use super::retention::fetch_earliest;
use super::{fluss_err, parse_path};

/// A Fluss log-table source exposing per-record broker timestamps.
pub struct FlussSource {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    schema: SchemaRef,
    buckets: Vec<i32>,
    event_time_idx: usize,
    progress: Arc<Mutex<SourceState>>,
    /// Earliest retained offset per bucket, or `None` when the broker probe
    /// failed. `None` makes recovery fail loudly rather than skip records.
    earliest: Option<HashMap<i32, i64>>,
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
        crate::convert::ensure_supported(&base_schema)?;
        let buckets: Vec<i32> = (0..info.get_num_buckets()).collect();
        // Probe retention now, while the table handle is open. A failed probe
        // is remembered as `None` so recovery can reject an unverifiable resume.
        let earliest = fetch_earliest(&connection, &table_path, &buckets).await;
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
            earliest,
        })
    }
}

impl Source for FlussSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        let progress = lock_progress(&self.progress)?;
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
        Ok(super::stream::read_bucket(
            Arc::clone(&self.connection),
            self.table_path.clone(),
            self.schema.clone(),
            split.id,
            split.start,
        ))
    }

    /// Advance the bucket's applied offset, never moving it backwards.
    fn commit(&self, split: SplitId, offset: Offset) -> Result<(), ConnectorError> {
        let mut progress = lock_progress(&self.progress)?;
        let entry = progress.offsets.entry(split).or_insert(offset);
        if offset > *entry {
            *entry = offset;
        }
        Ok(())
    }

    fn state(&self) -> SourceState {
        // `state()` cannot fail: recover the inner value on a poisoned lock.
        self.progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(self.event_time_idx)
    }

    fn is_unbounded(&self) -> bool {
        true
    }

    /// Reopen each bucket at its captured offset, checking it against the
    /// broker's earliest retained offset.
    ///
    /// A non-negative captured offset behind the earliest retained offset
    /// returns an explicit insufficient-retention error. When retention could
    /// not be probed the resume is rejected as unverified, never silently
    /// truncated. Negative offsets are symbolic (`EARLIEST_OFFSET`) and pass
    /// through unchecked.
    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        let mut splits = self.splits()?;
        for split in &mut splits {
            let Some(offset) = state.offsets.get(&split.id).copied() else {
                continue;
            };
            if offset >= 0 {
                let earliest = self.earliest.as_ref().ok_or_else(|| {
                    ConnectorError::Unsupported(format!(
                        "split {} checkpoint offset {offset} cannot be verified against broker retention",
                        split.id
                    ))
                })?;
                if let Some(&start) = earliest.get(&split.id)
                    && offset < start
                {
                    return Err(ConnectorError::Unsupported(format!(
                        "split {} checkpoint offset {offset} is older than the earliest retained offset {start}",
                        split.id
                    )));
                }
            }
            split.start = offset;
        }
        // Seed the applied position with the captured offsets. Otherwise a
        // checkpoint taken before the first post-recovery commit captures
        // EARLIEST, and a later restart replays the whole log over the restored
        // snapshot (double-apply).
        let mut progress = lock_progress(&self.progress)?;
        for split in &splits {
            if let Some(offset) = state.offsets.get(&split.id) {
                progress.offsets.insert(split.id, *offset);
            }
        }
        Ok(splits)
    }
}
