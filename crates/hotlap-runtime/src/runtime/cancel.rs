//! Cooperative cancellation for awaits that can be parked by a slow sink.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;

use hotlap_connectors::error::ConnectorError;

/// A cloneable flag that wakes every waiter when the engine is stopping.
///
/// The engine sets it before asking the serving loop to shut down, so an await
/// parked behind a stalled sink (a changelog send, a barrier flush, a sink
/// `prepare`/`commit`) can be abandoned instead of holding the engine thread
/// until the sink drains. A ready await still wins, so a clean shutdown never
/// aborts an operation that could have completed.
#[derive(Clone)]
pub(crate) struct Cancel {
    tx: watch::Sender<bool>,
    tripped: Arc<AtomicBool>,
}

impl Cancel {
    pub(crate) fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self {
            tx,
            tripped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Request cancellation; every current and future waiter observes it.
    ///
    /// Stored unconditionally, so a cancel issued between awaits (when no waiter
    /// is subscribed) is still seen by the next `cancelled` call.
    pub(crate) fn cancel(&self) {
        self.tx.send_replace(true);
    }

    /// Whether cancellation has been requested.
    pub(crate) fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }

    /// Whether an awaited operation was abandoned by cancellation.
    ///
    /// The engine uses this to refuse a false success: an interrupted delivery
    /// or checkpoint is not the same as a completed one.
    pub(crate) fn tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// Run `future` to completion, or abandon it once cancellation is requested.
    ///
    /// A ready `future` always wins. When cancellation wins, the abandonment is
    /// recorded so a later caller can refuse to report success.
    pub(crate) async fn race<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Result<T, ConnectorError> {
        tokio::select! {
            biased;
            value = future => Ok(value),
            () = self.cancelled() => {
                self.tripped.store(true, Ordering::SeqCst);
                Err(cancelled_error())
            }
        }
    }

    /// Resolve once cancellation is requested, or when the owner is gone.
    pub(crate) async fn cancelled(&self) {
        let mut rx = self.tx.subscribe();
        if *rx.borrow_and_update() {
            return;
        }
        let _ = rx.changed().await;
    }
}

/// Error recorded when cancellation abandoned an in-flight operation.
pub(crate) fn cancelled_error() -> ConnectorError {
    ConnectorError::Infrastructure("shutdown cancelled an in-flight operation".into())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_cancel_before_any_waiter_is_observed_later() {
        let cancel = Cancel::new();
        cancel.cancel();
        let waited = tokio::time::timeout(Duration::from_secs(2), cancel.cancelled()).await;
        assert!(waited.is_ok(), "a pre-existing cancel must resolve at once");
    }

    #[tokio::test]
    async fn a_cross_thread_cancel_wakes_a_parked_select() {
        let cancel = Cancel::new();
        let other = cancel.clone();
        let waiter = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = futures::future::pending::<()>() => {}
                () = other.cancelled() => {}
            }
        });
        tokio::task::yield_now().await;
        std::thread::spawn(move || cancel.cancel());
        let done = tokio::time::timeout(Duration::from_secs(2), waiter).await;
        assert!(done.is_ok(), "cancel never woke the parked select");
    }

    #[tokio::test]
    async fn a_race_marks_the_abandonment() {
        let cancel = Cancel::new();
        cancel.cancel();
        let result = cancel.race(futures::future::pending::<()>()).await;
        assert!(result.is_err(), "a cancelled race must fail");
        assert!(cancel.tripped(), "the abandonment must be recorded");
    }
}
