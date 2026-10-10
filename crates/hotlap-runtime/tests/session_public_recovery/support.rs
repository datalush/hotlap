use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use hotlap_connectors::sink::{ChangeStream, Sink, SinkCapabilities};
use hotlap_connectors::source::{Source, SourceStream};
use hotlap_runtime::{SessionConfig, SinkFactory, SourceFactory};
use hotlap_sql::error::SqlError;

use super::backend::SharedBackend;
use super::resumable::{Dataset, ResumableSource};
use super::spy::SpySource;
use super::watermarked_spy::WatermarkedSpy;

pub type SourceGate = Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>;

struct Factory {
    spies: Arc<Mutex<Vec<Arc<SpySource>>>>,
    source_gate: Option<SourceGate>,
}

struct ReplaySafeFactory {
    first_change: Option<mpsc::Sender<()>>,
}

#[async_trait::async_trait]
impl SinkFactory for ReplaySafeFactory {
    async fn create(
        &self,
        _name: &str,
        _options: &std::collections::BTreeMap<String, String>,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn Sink>, SqlError> {
        Ok(Arc::new(ReplaySafeSink {
            first_change: Mutex::new(self.first_change.clone()),
        }))
    }

    fn may_create_transactional(
        &self,
        _options: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        false
    }
}

struct ReplaySafeSink {
    first_change: Mutex<Option<mpsc::Sender<()>>>,
}

#[async_trait::async_trait]
impl Sink for ReplaySafeSink {
    async fn write(
        &self,
        mut changes: ChangeStream,
    ) -> Result<(), hotlap_connectors::ConnectorError> {
        while changes.next().await.is_some() {
            if let Some(signal) = self.first_change.lock().unwrap().take() {
                let _ = signal.send(());
            }
        }
        Ok(())
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::AtLeastOnce
    }

    async fn commit(&self) -> Result<(), hotlap_connectors::ConnectorError> {
        Ok(())
    }

    async fn abort(&self) -> Result<(), hotlap_connectors::ConnectorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn create(
        &self,
        _name: &str,
        _options: &std::collections::BTreeMap<String, String>,
    ) -> Result<Box<dyn Source>, SqlError> {
        let source = SpySource::new(Arc::new(ResumableSource::new(
            Dataset::new(vec![vec![1]]).with_retention(0),
        )));
        self.spies.lock().unwrap().push(Arc::new(source.clone()));
        let source: Box<dyn Source> = Box::new(WatermarkedSpy(source));
        match &self.source_gate {
            Some(gate) => Ok(Box::new(GatedSource {
                inner: source,
                gate: Arc::clone(gate),
            })),
            None => Ok(source),
        }
    }
}

struct GatedSource {
    inner: Box<dyn Source>,
    gate: SourceGate,
}

impl Source for GatedSource {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn splits(
        &self,
    ) -> Result<Vec<hotlap_connectors::source::Split>, hotlap_connectors::ConnectorError> {
        self.inner.splits()
    }

    fn read(
        &self,
        split: &hotlap_connectors::source::Split,
    ) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
        let source = self.inner.read(split)?;
        let receiver = self.gate.lock().unwrap().take();
        match receiver {
            Some(receiver) => Ok(Box::pin(
                futures::stream::once(async move {
                    let _ = receiver.await;
                    source
                })
                .flatten(),
            )),
            None => Ok(source),
        }
    }

    fn commit(
        &self,
        split: hotlap_connectors::source::SplitId,
        offset: hotlap_connectors::source::Offset,
    ) -> Result<(), hotlap_connectors::ConnectorError> {
        self.inner.commit(split, offset)
    }

    fn state(&self) -> hotlap_connectors::source::SourceState {
        self.inner.state()
    }

    fn event_time_column(&self) -> Option<usize> {
        self.inner.event_time_column()
    }

    fn resume(
        &self,
        state: &hotlap_connectors::source::SourceState,
    ) -> Result<Vec<hotlap_connectors::source::Split>, hotlap_connectors::ConnectorError> {
        self.inner.resume(state)
    }
}

pub fn config(
    backend: &SharedBackend,
    spies: Arc<Mutex<Vec<Arc<SpySource>>>>,
    source_gate: Option<SourceGate>,
    first_change: Option<mpsc::Sender<()>>,
) -> SessionConfig {
    SessionConfig::new()
        .with_source_factory(Arc::new(Factory { spies, source_gate }))
        .with_sink_factory(Arc::new(ReplaySafeFactory { first_change }))
        .with_checkpoint(
            Duration::from_secs(3600),
            hotlap_runtime::runtime::checkpoint::DEFAULT_RETAIN,
            Box::new(backend.clone()),
        )
}

pub fn wait_for_output(output: mpsc::Receiver<()>) {
    output
        .recv_timeout(Duration::from_secs(5))
        .expect("sink did not consume the source changelog");
}
