//! The dedicated engine thread: owns `Hotlap`, drives the source and commands.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap::Hotlap;
use hotlap_engine::{EngineCore, MetricsRegistry};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};
use tokio::time::{Instant, Interval, interval_at};

use crate::runtime::checkpoint::Checkpointer;
use crate::runtime::command::{self, Command};
use crate::runtime::pipeline::{self, Pipeline, feed_source, record_error};
use crate::runtime::recovery::Recovery;
use crate::runtime::sink::SinkPump;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceStream};

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
    source: SourceStream,
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
    let engine = match prepare(&mut pipeline, metrics) {
        Ok(engine) => engine,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    serve(engine, pipeline, rx, last_error, checkpoint_error, built).await;
}

/// Open the kernel, merge the source streams and read the checkpoint config.
fn prepare(
    pipeline: &mut Pipeline,
    metrics: Arc<MetricsRegistry>,
) -> Result<Engine, ConnectorError> {
    let core = EngineCore::with_metrics(Arc::clone(&metrics));
    let mut hotlap = Hotlap::open_with(Box::new(core));
    if let Some(events) = pipeline.retention {
        hotlap
            .set_input_retention(events)
            .map_err(|error| ConnectorError::Unsupported(error.0))?;
    }
    pipeline::setup(&mut hotlap, pipeline)?;
    let sinks = SinkPump::start_with_metrics(&pipeline.sinks, Some(metrics));
    let coordinated = sinks.coordinated();
    let (source, checkpointer, ticker) = match pipeline.checkpoint.take() {
        Some(config) => {
            let mut checkpointer =
                Checkpointer::new(config.backend, config.retain).with_sinks(coordinated);
            let source = recover(&mut hotlap, pipeline.source.as_ref(), &mut checkpointer)?;
            let tick = interval_at(Instant::now() + config.interval, config.interval);
            (source, Some(checkpointer), Some(tick))
        }
        None => (
            pipeline::merged_stream(pipeline.source.as_ref())?,
            None,
            None,
        ),
    };
    Ok(Engine {
        hotlap,
        source,
        sinks,
        checkpointer,
        ticker,
    })
}

/// Reopen the source at the newest valid checkpoint, or from the start when
/// the store holds none (a clean start).
fn recover(
    hotlap: &mut Hotlap,
    source: &dyn Source,
    checkpointer: &mut Checkpointer,
) -> Result<SourceStream, ConnectorError> {
    match Recovery::load(checkpointer)? {
        Some(checkpoint) => {
            checkpointer.resume_after(checkpoint.id);
            Recovery::resume(hotlap, source, &checkpoint)
        }
        None => pipeline::merged_stream(source),
    }
}

/// Serve the source stream and the command channel until shutdown.
async fn serve(
    mut engine: Engine,
    pipeline: Pipeline,
    rx: &mut UnboundedReceiver<Command>,
    last_error: &Mutex<Option<String>>,
    checkpoint_error: &Mutex<Option<String>>,
    built: &AtomicBool,
) {
    let mut source_done = false;
    loop {
        tokio::select! {
            maybe = engine.source.next(), if !source_done => {
                source_done = feed_source(
                    &mut engine.hotlap, &pipeline, &engine.sinks, maybe, last_error, built,
                )
                .await;
            }
            _ = tick(&mut engine.ticker) => {
                command::run_periodic(
                    &mut engine.checkpointer, &engine.hotlap, &pipeline, checkpoint_error,
                )
                .await;
            }
            cmd = rx.recv() => {
                if command::handle(cmd, &mut engine.hotlap, &pipeline, &mut engine.checkpointer)
                    .await
                {
                    close_sinks(engine.sinks, last_error).await;
                    return;
                }
            }
        }
    }
}

/// Await the next periodic tick, or stay pending when checkpointing is off.
async fn tick(ticker: &mut Option<Interval>) {
    match ticker {
        Some(interval) => {
            interval.tick().await;
        }
        None => futures::future::pending::<()>().await,
    }
}

/// Drop the sink senders and wait for the tasks, so the last changelog lands.
async fn close_sinks(sinks: SinkPump, last_error: &Mutex<Option<String>>) {
    if let Err(error) = sinks.close().await {
        record_error(last_error, error);
    }
}
