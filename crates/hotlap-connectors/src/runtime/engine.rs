//! The dedicated engine thread: owns `Hotlap`, drives the source and commands.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap::Hotlap;
use hotlap_engine::EngineCore;
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};
use tokio::time::{Instant, Interval, interval_at};

use crate::error::ConnectorError;
use crate::runtime::checkpoint::Checkpointer;
use crate::runtime::command::{self, Command};
use crate::runtime::pipeline::{self, Pipeline, feed_source, record_error};
use crate::runtime::sink::SinkPump;
use crate::source::SourceStream;

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
    ready: oneshot::Sender<Result<(), ConnectorError>>,
) {
    let engine = match prepare(&mut pipeline) {
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
fn prepare(pipeline: &mut Pipeline) -> Result<Engine, ConnectorError> {
    let mut hotlap = Hotlap::open_with(Box::new(EngineCore::new()));
    pipeline::setup(&mut hotlap, pipeline)?;
    let source = pipeline::merged_stream(pipeline.source.as_ref())?;
    let sinks = SinkPump::start(&pipeline.sinks);
    let coordinated = sinks.coordinated();
    let (checkpointer, ticker) = match pipeline.checkpoint.take() {
        Some(config) => {
            let tick = interval_at(Instant::now() + config.interval, config.interval);
            (
                Some(Checkpointer::new(config.backend, config.retain).with_sinks(coordinated)),
                Some(tick),
            )
        }
        None => (None, None),
    };
    Ok(Engine {
        hotlap,
        source,
        sinks,
        checkpointer,
        ticker,
    })
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
