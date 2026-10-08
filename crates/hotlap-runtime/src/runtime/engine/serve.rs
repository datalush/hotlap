//! The serving loop: multiplexes the source stream, ticker and commands.

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use futures::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Interval;

use super::Engine;
use crate::runtime::command::{self, Command};
use crate::runtime::pipeline::{FeedStatus, Pipeline, feed_source, record_error};

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
                match feed_source(
                    &mut engine.hotlap,
                    &pipeline.sources,
                    &engine.sinks,
                    maybe,
                    last_error,
                    built,
                )
                .await
                {
                    FeedStatus::Continue => {}
                    FeedStatus::Exhausted => source_done = true,
                    FeedStatus::Failed => {
                        // Fail-stop: a source fault disables further ingestion,
                        // periodic checkpoints and view builds, but the command
                        // channel stays open so the handle can still shut down.
                        source_done = true;
                        failed = true;
                    }
                }
            }
            _ = tick(&mut engine.ticker), if !failed => {
                command::run_periodic(
                    &mut engine.checkpointer,
                    &engine.hotlap,
                    &pipeline.sources,
                    checkpoint_error,
                )
                .await;
            }
            cmd = rx.recv() => {
                if command::handle(
                    cmd,
                    &mut engine.hotlap,
                    &pipeline.sources,
                    &mut engine.checkpointer,
                    failed,
                )
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
async fn close_sinks(sinks: crate::runtime::sink::SinkPump, last_error: &Mutex<Option<String>>) {
    if let Err(error) = sinks.close().await {
        record_error(last_error, error);
    }
}
