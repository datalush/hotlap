use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Offset, Source, SourceBatch, SourceState, SourceStream, Split};
use hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN;
use hotlap_runtime::{Session, SessionConfig, SourceFactory};
use hotlap_sql::SqlError;

#[path = "../common/backend.rs"]
mod backend;

pub use backend::SharedBackend;
#[path = "outputs.rs"]
mod outputs;
#[path = "pending.rs"]
mod pending;
pub use outputs::rows;
pub use pending::pending_body;

const ROWS: [i64; 5] = [1, 2, 1, 2, 3];
const SOURCE_SQL: &str = "CREATE SOURCE src WITH (connector='inmem') WATERMARK FOR \
     _event_time AS _event_time - INTERVAL '1 s';";

#[derive(Clone)]
pub struct Progress {
    limit: usize,
    state: Arc<(Mutex<SourceState>, Condvar)>,
    resumed: Arc<Mutex<Option<i64>>>,
}

impl Default for Progress {
    fn default() -> Self {
        Self::new(ROWS.len())
    }
}

impl Progress {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Arc::new((Mutex::new(SourceState::default()), Condvar::new())),
            resumed: Arc::new(Mutex::new(None)),
        }
    }

    fn wait_for(&self, offset: i64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let (state, changed) = &*self.state;
        let mut state = state.lock().unwrap();
        while state.offsets.get(&0).copied().unwrap_or_default() < offset {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "source did not reach offset {offset}");
            let (next, result) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(!result.timed_out(), "source did not reach offset {offset}");
        }
    }

    pub fn resumed(&self) -> Option<i64> {
        *self.resumed.lock().unwrap()
    }
}

struct FixtureSource(Progress);

impl Source for FixtureSource {
    fn physical_identity(&self) -> Option<String> {
        Some("test/session-pending-promotion/source".into())
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("_event_time", DataType::Int64, false),
        ]))
    }

    fn splits(&self) -> Result<Vec<Split>, ConnectorError> {
        let offset = self
            .0
            .state
            .0
            .lock()
            .unwrap()
            .offsets
            .get(&0)
            .copied()
            .unwrap_or(0);
        Ok(vec![Split {
            id: 0,
            start: offset,
        }])
    }

    fn read(&self, split: &Split) -> Result<SourceStream, ConnectorError> {
        let limit = self.0.limit;
        let batches = (split.start.max(0) as usize..limit).map(|index| {
            let schema = schema();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![ROWS[index]])),
                Arc::new(Int64Array::from(vec![index as i64])),
            ];
            let batch = RecordBatch::try_new(schema, columns).unwrap();
            Ok(SourceBatch {
                batch,
                base_offset: index as Offset,
                next_offset: index as Offset + 1,
                split: 0,
            })
        });
        Ok(Box::pin(futures::stream::iter(batches)))
    }

    fn commit(&self, split: i32, offset: Offset) -> Result<(), ConnectorError> {
        let (state, changed) = &*self.0.state;
        state.lock().unwrap().offsets.insert(split, offset);
        changed.notify_all();
        Ok(())
    }

    fn state(&self) -> SourceState {
        self.0.state.0.lock().unwrap().clone()
    }

    fn event_time_column(&self) -> Option<usize> {
        Some(1)
    }

    fn resume(&self, state: &SourceState) -> Result<Vec<Split>, ConnectorError> {
        let offset = state.offsets.get(&0).copied().unwrap_or(0);
        *self.0.resumed.lock().unwrap() = Some(offset);
        *self.0.state.0.lock().unwrap() = state.clone();
        Ok(vec![Split {
            id: 0,
            start: offset,
        }])
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("_event_time", DataType::Int64, false),
    ]))
}

struct FixtureFactory(Progress);

#[async_trait::async_trait]
impl SourceFactory for FixtureFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        Ok(Box::new(FixtureSource(self.0.clone())))
    }
}

pub fn config(backend: &SharedBackend, progress: Progress) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(FixtureFactory(progress)))
        .with_checkpoint(
            Duration::from_secs(3600),
            DEFAULT_RETAIN,
            Box::new(backend.clone()),
        )
}

pub fn declare(session: &mut Session, views: &[(&str, i64)]) {
    session.sql(SOURCE_SQL).unwrap();
    for (name, key) in views {
        session
            .sql(&format!(
                "CREATE MATERIALIZED VIEW {name} AS SELECT k FROM src WHERE k = {key};"
            ))
            .unwrap();
    }
}

pub fn seed(backend: &SharedBackend, views: &[(&str, i64)], consumed: usize) {
    let progress = Progress::new(consumed);
    let mut session = Session::open(config(backend, progress.clone())).unwrap();
    declare(&mut session, views);
    session.sql("START;").unwrap();
    progress.wait_for(consumed as i64);
    session.checkpoint().unwrap();
    session.shutdown().unwrap();
}
