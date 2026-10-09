//! Cooperative cancellation for a sink pump blocked by backpressure.

use tokio::sync::watch;

/// A cloneable flag that wakes every waiter when the engine is stopping.
///
/// The engine sets it before asking the serving loop to shut down, so a pump
/// parked on a full changelog channel can abandon the send instead of holding
/// the engine thread until the sink drains. A ready send still wins inside the
/// pump, so a clean shutdown never drops a batch on cancellation alone.
#[derive(Clone)]
pub(crate) struct Cancel {
    tx: watch::Sender<bool>,
}

impl Cancel {
    pub(crate) fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx }
    }

    /// Request cancellation; every current and future waiter observes it.
    ///
    /// Stored unconditionally, so a cancel issued between pumps (when no waiter
    /// is subscribed) is still seen by the next `cancelled` call.
    pub(crate) fn cancel(&self) {
        self.tx.send_replace(true);
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
}
