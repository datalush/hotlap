//! Coordinated drain and final delivery for all sink writers.

use super::{CLOSE_TIMEOUT, REAP_TIMEOUT, SinkPump, cancelled, stopped};
use hotlap_connectors::error::ConnectorError;

impl SinkPump {
    /// Drain and join every writer before authorizing any implicit EOF commit.
    pub async fn close(self) -> Result<(), ConnectorError> {
        let interrupted = self.interrupted.load(std::sync::atomic::Ordering::SeqCst);
        if interrupted {
            self.close_clean
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        let mut failure = None;
        let mut pending = Vec::new();
        for entry in self.entries {
            drop(entry.tx);
            pending.push((entry.shared, entry.handle));
        }
        let mut drained = Vec::new();
        for (shared, mut handle) in pending {
            match tokio::time::timeout(CLOSE_TIMEOUT, &mut handle).await {
                Ok(Ok(Ok(()))) => drained.push(shared),
                Ok(Ok(Err(error))) => {
                    self.close_clean
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                    record(&mut failure, error);
                }
                Ok(Err(join)) => {
                    self.close_clean
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                    record(&mut failure, join_error(join));
                }
                Err(_) => {
                    handle.abort();
                    let _ = tokio::time::timeout(REAP_TIMEOUT, &mut handle).await;
                    self.close_clean
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                    record(&mut failure, timed_out());
                }
            }
        }
        if failure.is_none() && self.close_clean.load(std::sync::atomic::Ordering::SeqCst) {
            for shared in drained {
                if let Err(error) = shared.commit().await {
                    self.close_clean
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                    record(&mut failure, error);
                    break;
                }
            }
        }
        if failure.is_none() && interrupted {
            return Err(cancelled());
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn record(slot: &mut Option<ConnectorError>, error: ConnectorError) {
    slot.get_or_insert(error);
}

fn join_error(error: tokio::task::JoinError) -> ConnectorError {
    if error.is_panic() {
        ConnectorError::Infrastructure(format!("sink task panicked: {error}"))
    } else {
        stopped()
    }
}

fn timed_out() -> ConnectorError {
    ConnectorError::Infrastructure("sink task timed out during close".into())
}
