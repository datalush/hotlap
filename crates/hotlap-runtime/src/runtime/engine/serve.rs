//! The serving loop: multiplexes the source stream, ticker and commands.

use futures::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Interval;

use super::{Engine, EngineShared};
use crate::runtime::command::{self, Command};
use crate::runtime::pipeline::{FeedStatus, Pipeline, feed_source};
use crate::runtime::sources::SourceEvent;
use hotlap_connectors::error::ConnectorError;

/// Serve the source stream and the command channel until shutdown.
pub(super) async fn serve(
    mut engine: Engine,
    pipeline: Pipeline,
    rx: &mut UnboundedReceiver<Command>,
    shared: &EngineShared,
) {
    let mut source_done = false;
    let mut failed = false;
    loop {
        tokio::select! {
            maybe = engine.source.next(), if !source_done && !failed => {
                match next_status(&mut engine, &pipeline, maybe, shared).await {
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
                // A failed checkpoint leaves the engine inconsistent; stop
                // polling the next source before it can advance the offsets.
                if run_periodic(&mut engine, &pipeline, shared).await {
                    failed = true;
                }
            }
            cmd = rx.recv() => {
                if handle(cmd, &mut engine, &pipeline, &mut failed).await {
                    shut_down(engine, shared).await;
                    return;
                }
            }
        }
    }
}

/// Release the checkpointer's retained senders and close every sink.
///
/// The checkpointer holds clones of the changelog senders for its barrier. If
/// they outlive the pump's own senders, the sink task never sees the channel
/// reach EOF, so its final commit never runs and the close would only end on
/// the timeout. Dropping the checkpointer first lets a healthy sink drain and
/// commit; a stalled one is aborted and reported by [`SinkPump::close`].
async fn shut_down(engine: Engine, shared: &EngineShared) {
    let Engine {
        sinks,
        checkpointer,
        ..
    } = engine;
    drop(checkpointer);
    if let Err(error) = sinks.close().await
        && let Ok(mut slot) = shared.close_error.lock()
    {
        slot.get_or_insert(error);
    }
}

/// Feed one source item and report whether ingestion may continue.
async fn next_status(
    engine: &mut Engine,
    pipeline: &Pipeline,
    item: Option<Result<SourceEvent, ConnectorError>>,
    shared: &EngineShared,
) -> FeedStatus {
    feed_source(
        &mut engine.hotlap,
        &pipeline.sources,
        &engine.sinks,
        item,
        &shared.last_error,
        &shared.built,
    )
    .await
}

/// Run one periodic checkpoint; returns whether it left the state inconsistent.
async fn run_periodic(engine: &mut Engine, pipeline: &Pipeline, shared: &EngineShared) -> bool {
    command::run_periodic(
        &mut engine.checkpointer,
        &engine.hotlap,
        &pipeline.sources,
        &shared.checkpoint_error,
    )
    .await
}

/// Handle one command; returns true when the engine must shut down.
async fn handle(
    cmd: Option<Command>,
    engine: &mut Engine,
    pipeline: &Pipeline,
    failed: &mut bool,
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

#[cfg(test)]
mod tests;
