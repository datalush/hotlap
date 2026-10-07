//! The dedicated engine thread: owns `Hotlap`, drives the source and commands.

use futures::StreamExt;
use hotlap::Hotlap;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::error::ConnectorError;
use crate::runtime::handle::Command;
use crate::runtime::pipeline::{self, Pipeline};

/// Run the engine loop until shutdown or channel close.
pub(crate) fn run(pipeline: Pipeline, mut rx: UnboundedReceiver<Command>) {
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
                maybe = source.next(), if !source_done => match maybe {
                    Some(Ok(sb)) => {
                        let _ = pipeline::ingest(&mut hotlap, &pipeline.input, &sb);
                    }
                    Some(Err(_)) => {}
                    None => source_done = true,
                },
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

fn map_err(e: hotlap::HotlapError) -> ConnectorError {
    ConnectorError::Infrastructure(e.0)
}
