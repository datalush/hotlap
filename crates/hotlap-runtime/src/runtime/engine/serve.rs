//! The serving loop: multiplexes the source stream, ticker and commands.

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use futures::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Interval;

use super::Engine;
use crate::runtime::command::{self, Command};
use crate::runtime::pipeline::{FeedStatus, Pipeline, feed_source, record_error};
use crate::runtime::sink::SinkPump;
use crate::runtime::sources::SourceEvent;
use hotlap_connectors::error::ConnectorError;

/// Serve the source stream and the command channel until shutdown.
pub(super) async fn serve(
    mut engine: Engine,
    pipeline: Pipeline,
    rx: &mut UnboundedReceiver<Command>,
    last_error: &Mutex<Option<String>>,
    checkpoint_error: &Mutex<Option<String>>,
    built: &AtomicBool,
) {
    let mut source_done = false;
    let mut failed = false;
    loop {
        tokio::select! {
            maybe = engine.source.next(), if !source_done => {
                match next_status(&mut engine, &pipeline, maybe, last_error, built).await {
                    FeedStatus::Continue => {}
                    FeedStatus::Exhausted => source_done = true,
                    // Fail-stop: a source fault disables further ingestion,
                    // periodic checkpoints and view builds, but the command
                    // channel stays open so the handle can still shut down.
                    FeedStatus::Failed => {
                        source_done = true;
                        failed = true;
                    }
                }
            }
            _ = tick(&mut engine.ticker), if !failed => {
                run_periodic(&mut engine, &pipeline, checkpoint_error).await;
            }
            cmd = rx.recv() => {
                if handle(cmd, &mut engine, &pipeline, failed).await {
                    close_sinks(engine.sinks, last_error).await;
                    return;
                }
            }
        }
    }
}

/// Feed one source item and report whether ingestion may continue.
async fn next_status(
    engine: &mut Engine,
    pipeline: &Pipeline,
    item: Option<Result<SourceEvent, ConnectorError>>,
    last_error: &Mutex<Option<String>>,
    built: &AtomicBool,
) -> FeedStatus {
    feed_source(
        &mut engine.hotlap,
        &pipeline.sources,
        &engine.sinks,
        item,
        last_error,
        built,
    )
    .await
}

/// Run one periodic checkpoint, recording a failure on its own error slot.
async fn run_periodic(
    engine: &mut Engine,
    pipeline: &Pipeline,
    checkpoint_error: &Mutex<Option<String>>,
) {
    command::run_periodic(
        &mut engine.checkpointer,
        &engine.hotlap,
        &pipeline.sources,
        checkpoint_error,
    )
    .await;
}

/// Handle one command; returns true when the engine must shut down.
async fn handle(
    cmd: Option<Command>,
    engine: &mut Engine,
    pipeline: &Pipeline,
    failed: bool,
) -> bool {
    command::handle(
        cmd,
        &mut engine.hotlap,
        &pipeline.sources,
        &mut engine.checkpointer,
        failed,
    )
    .await
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

#[cfg(test)]
mod tests;
