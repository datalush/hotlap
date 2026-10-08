//! The dedicated engine thread: owns `Hotlap`, drives the source and commands.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use hotlap::Hotlap;
use hotlap_engine::{EngineCore, MetricsRegistry};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};
use tokio::time::{Instant, Interval, interval_at};

use crate::runtime::checkpoint::Checkpointer;
use crate::runtime::command::Command;
use crate::runtime::pipeline::{self, Pipeline};
use crate::runtime::recovery::Recovery;
use crate::runtime::sink::SinkPump;
use crate::runtime::sources::InputStream;
use hotlap_connectors::error::ConnectorError;

mod serve;

/// Run the engine loop until shutdown or channel close.
///
/// `ready` reports the outcome of startup (runtime, kernel open, setup and
/// source streams) exactly once, so `EngineHandle::start` can fail fast.
pub(crate) fn run(
    pipeline: Pipeline,
    mut rx: UnboundedReceiver<Command>,
    last_error: Arc<Mutex<Option<String>>>,
    checkpoint_error: Arc<Mutex<Option<String>>>,
    built: Arc<AtomicBool>,
    metrics: Arc<MetricsRegistry>,
    ready: oneshot::Sender<Result<(), ConnectorError>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(error) => {
            let _ = ready.send(Err(ConnectorError::Infrastructure(error.to_string())));
            return;
        }
    };
    rt.block_on(drive(
        pipeline,
        &mut rx,
        &last_error,
        &checkpoint_error,
        &built,
        metrics,
        ready,
    ));
}

/// Live state owned by the serving loop after startup.
struct Engine {
    hotlap: Hotlap,
    source: InputStream,
    sinks: SinkPump,
    checkpointer: Option<Checkpointer>,
    ticker: Option<Interval>,
}

/// Open the kernel, wire the pipeline and then serve source data and commands.
async fn drive(
    mut pipeline: Pipeline,
    rx: &mut UnboundedReceiver<Command>,
    last_error: &Mutex<Option<String>>,
    checkpoint_error: &Mutex<Option<String>>,
    built: &AtomicBool,
    metrics: Arc<MetricsRegistry>,
    ready: oneshot::Sender<Result<(), ConnectorError>>,
) {
    let engine = match prepare(&mut pipeline, metrics, checkpoint_error).await {
        Ok(engine) => engine,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    serve::serve(engine, pipeline, rx, last_error, checkpoint_error, built).await;
}

/// Open the kernel, merge the source streams and read the checkpoint config.
async fn prepare(
    pipeline: &mut Pipeline,
    metrics: Arc<MetricsRegistry>,
    checkpoint_error: &Mutex<Option<String>>,
) -> Result<Engine, ConnectorError> {
    let core = EngineCore::with_metrics(Arc::clone(&metrics));
    let mut hotlap = Hotlap::open_with(Box::new(core));
    if let Some(events) = pipeline.retention {
        hotlap
            .set_input_retention(events)
            .map_err(|error| ConnectorError::Unsupported(error.0))?;
    }
    pipeline::setup(&mut hotlap, pipeline)?;
    let sinks = SinkPump::start_with_metrics(&pipeline.sinks, Some(Arc::clone(&metrics)));
    let coordinated = sinks.coordinated();
    let (source, checkpointer, ticker) = match pipeline.checkpoint.take() {
        Some(config) => {
            let mut checkpointer =
                Checkpointer::new(config.backend, config.retain).with_sinks(coordinated);
            let source = Recovery::start(
                &mut hotlap,
                &pipeline.sources,
                &mut checkpointer,
                checkpoint_error,
                &metrics,
            )
            .await?;
            let tick = interval_at(Instant::now() + config.interval, config.interval);
            (source, Some(checkpointer), Some(tick))
        }
        None => (pipeline.sources.stream()?, None, None),
    };
    Ok(Engine {
        hotlap,
        source,
        sinks,
        checkpointer,
        ticker,
    })
}
