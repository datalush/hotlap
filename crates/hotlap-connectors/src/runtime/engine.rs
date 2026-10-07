//! The dedicated engine thread: owns `Hotlap`, drives the source and commands.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use hotlap::Hotlap;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::error::ConnectorError;
use crate::runtime::handle::Command;
use crate::runtime::pipeline::{self, Pipeline};
use crate::source::SourceBatch;

/// Run the engine loop until shutdown or channel close.
pub(crate) fn run(
    pipeline: Pipeline,
    mut rx: UnboundedReceiver<Command>,
    last_error: Arc<Mutex<Option<String>>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return,
    };
    rt.block_on(async move {
        let mut hotlap = match Hotlap::open() {
            Ok(h) => h,
            Err(_) => return,
        };
        if pipeline::setup(&mut hotlap, &pipeline).is_err() {
            return;
        }
        let mut source = match pipeline::merged_stream(pipeline.source.as_ref()) {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut source_done = false;
        loop {
            tokio::select! {
                maybe = source.next(), if !source_done => {
                    source_done = on_source_item(&mut hotlap, &pipeline, maybe, &last_error);
                }
                cmd = rx.recv() => match cmd {
                    Some(Command::Snapshot { view, reply }) => {
                        let _ = reply.send(hotlap.snapshot(&view).map_err(map_err));
                    }
                    Some(Command::LateDropped { input, reply }) => {
                        let _ = reply.send(hotlap.late_dropped(&input).map_err(map_err));
                    }
                    Some(Command::Shutdown { reply }) => {
                        let _ = reply.send(());
                        return;
                    }
                    None => return,
                },
            }
        }
    });
}

/// Ingest one source item, recording any error and reporting source completion.
fn on_source_item(
    hotlap: &mut Hotlap,
    pipeline: &Pipeline,
    item: Option<Result<SourceBatch, ConnectorError>>,
    last_error: &Mutex<Option<String>>,
) -> bool {
    match item {
        Some(Ok(sb)) => match pipeline::ingest(hotlap, &pipeline.input, &sb) {
            Ok(()) => false,
            Err(error) => {
                record_error(last_error, error);
                true
            }
        },
        Some(Err(error)) => {
            record_error(last_error, error);
            true
        }
        None => true,
    }
}

/// Store the first source error; it is the one that stopped the source branch.
fn record_error(last_error: &Mutex<Option<String>>, error: ConnectorError) {
    if let Ok(mut slot) = last_error.lock() {
        slot.get_or_insert_with(|| error.to_string());
    }
}

fn map_err(e: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(e.0)
}
