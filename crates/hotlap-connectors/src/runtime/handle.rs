//! Thread-safe handle to a running engine, and the command protocol.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use hotlap::Row;
use tokio::sync::{mpsc, oneshot};

use crate::error::ConnectorError;
use crate::runtime::engine;
use crate::runtime::pipeline::Pipeline;

/// Command sent from the handle to the engine thread.
pub enum Command {
    Snapshot {
        view: String,
        reply: oneshot::Sender<Result<Vec<Row>, ConnectorError>>,
    },
    LateDropped {
        input: String,
        reply: oneshot::Sender<Result<u64, ConnectorError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Owns the engine thread and speaks to it over a channel.
pub struct EngineHandle {
    tx: mpsc::UnboundedSender<Command>,
    join: Option<JoinHandle<()>>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl EngineHandle {
    /// Spawn the engine thread and start the pipeline.
    pub fn start(pipeline: Pipeline) -> Result<Self, ConnectorError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let last_error = Arc::new(Mutex::new(None));
        let engine_error = Arc::clone(&last_error);
        let join = std::thread::spawn(move || engine::run(pipeline, rx, engine_error));
        Ok(Self {
            tx,
            join: Some(join),
            last_error,
        })
    }

    /// First source/ingestion error that stopped the source branch, if any.
    pub fn last_error(&self) -> Result<Option<String>, ConnectorError> {
        self.last_error
            .lock()
            .map(|slot| slot.clone())
            .map_err(|_| ConnectorError::Infrastructure("engine error state poisoned".into()))
    }

    /// Read the consolidated output of a view.
    pub fn snapshot(&self, view: &str) -> Result<Vec<Row>, ConnectorError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Snapshot {
                view: view.to_string(),
                reply,
            })
            .map_err(|_| stopped())?;
        rx.blocking_recv().map_err(|_| stopped())?
    }

    /// Events dropped as late in `input`.
    pub fn late_dropped(&self, input: &str) -> Result<u64, ConnectorError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LateDropped {
                input: input.to_string(),
                reply,
            })
            .map_err(|_| stopped())?;
        rx.blocking_recv().map_err(|_| stopped())?
    }

    /// Stop the engine and join its thread.
    pub fn shutdown(mut self) -> Result<(), ConnectorError> {
        let (reply, rx) = oneshot::channel();
        let _ = self.tx.send(Command::Shutdown { reply });
        let _ = rx.blocking_recv();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        Ok(())
    }
}

fn stopped() -> ConnectorError {
    ConnectorError::Infrastructure("engine thread stopped".into())
}
