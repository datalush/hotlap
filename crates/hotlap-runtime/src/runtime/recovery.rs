//! Recovery: restore the newest valid checkpoint, reopen sources and replay.
//!
//! A restart loads the last valid checkpoint (engine snapshot plus captured
//! per-source offsets), validates it against the declared sources, restores the
//! engine, reopens each source split at its captured offset and feeds the
//! resulting streams back in. Because the engine is restored to the checkpoint
//! before the offsets are replayed, replaying the log from those offsets yields
//! exactly the state the crashed run had reached.

mod decision;
mod load;
mod pending;
mod restore;
mod resume;
mod sources;

pub use decision::RecoveryDecision;

use std::sync::Mutex;

use hotlap::Hotlap;
use hotlap_engine::MetricsRegistry;

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::sources::{InputStream, Sources};
use hotlap_connectors::error::ConnectorError;
use pending::{discard, pending_commit};
use sources::read_body;

/// The last valid checkpoint, if the store holds one.
pub struct Recovery;

/// What a resolved recovery decision yields.
enum Resolved {
    /// Serve a fresh source stream (clean start, or a discard without fallback).
    Stream,
    /// Resume from this checkpoint.
    Checkpoint(Checkpoint),
}

impl Recovery {
    /// Decide how to recover, detecting a checkpoint that was mid-commit when
    /// the process stopped.
    ///
    /// The newest valid checkpoint and any pending commit body are validated
    /// against `sources` before a decision is returned, so a promotion or a
    /// resume never runs against an incompatible declaration. An undecodable
    /// current-format body keeps the existing discard path; a foreign or
    /// incompatible format is a fatal `Unsupported`.
    pub fn inspect(
        checkpointer: &Checkpointer,
        sources: &Sources,
    ) -> Result<RecoveryDecision, ConnectorError> {
        let fallback = Self::load(checkpointer, sources)?;
        if let Some(pending) = pending_commit(checkpointer, fallback.as_ref().map(|c| c.id))? {
            let reason = match read_body(checkpointer, pending)? {
                Some(checkpoint) => {
                    sources::validate(sources, &checkpoint.sources, &checkpoint.engine)?;
                    if checkpointer.redriable() {
                        return Ok(RecoveryDecision::Promote(checkpoint));
                    }
                    if !checkpointer.replay_safe() {
                        return Ok(RecoveryDecision::Reject {
                            pending,
                            reason: "a transactional sink cannot be re-driven; replay could \
                                     duplicate its committed output",
                        });
                    }
                    "a sink is not re-drivable"
                }
                // The body cannot be decoded or restored, so the commit cannot
                // be promoted either. Replaying over a transactional sink could
                // duplicate an already-committed transaction, so refuse rather
                // than fall back.
                None => {
                    if !checkpointer.replay_safe() {
                        return Ok(RecoveryDecision::Reject {
                            pending,
                            reason: "the pending commit body is corrupt or incomplete and a \
                                     transactional sink cannot be safely replayed",
                        });
                    }
                    "the pending commit body is corrupt or incomplete"
                }
            };
            return Ok(RecoveryDecision::Discard {
                pending,
                fallback,
                reason,
            });
        }
        match fallback {
            Some(checkpoint) => Ok(RecoveryDecision::Resume(checkpoint)),
            None => Ok(RecoveryDecision::Clean),
        }
    }

    /// Resolve any interrupted commit and return the source stream to serve.
    ///
    /// This is the layer that holds the sinks, so it executes [`Self::inspect`]:
    /// a promoted checkpoint re-drives the commit before resuming; a discarded
    /// one replays from the fallback and records an explicit warning signal. A
    /// failed re-drive is only discarded when a replay-safe sink allows it.
    pub async fn start(
        hotlap: &mut Hotlap,
        sources: &Sources,
        checkpointer: &mut Checkpointer,
        signal: &Mutex<Option<String>>,
        metrics: &MetricsRegistry,
    ) -> Result<InputStream, ConnectorError> {
        checkpointer.sweep_stale_commits()?;
        let decision = Self::inspect(checkpointer, sources)?;
        match Self::resolve(decision, sources, checkpointer, signal, metrics).await? {
            Resolved::Stream => sources.stream(),
            Resolved::Checkpoint(checkpoint) => {
                checkpointer.resume_after(checkpoint.id);
                Self::resume(hotlap, sources, &checkpoint)
            }
        }
    }

    /// Turn a recovery decision into a checkpoint to resume or a clean start.
    async fn resolve(
        decision: RecoveryDecision,
        sources: &Sources,
        checkpointer: &mut Checkpointer,
        signal: &Mutex<Option<String>>,
        metrics: &MetricsRegistry,
    ) -> Result<Resolved, ConnectorError> {
        match decision {
            RecoveryDecision::Clean => Ok(Resolved::Stream),
            RecoveryDecision::Resume(checkpoint) => Ok(Resolved::Checkpoint(checkpoint)),
            RecoveryDecision::Promote(checkpoint) => {
                Self::promote(checkpoint, sources, checkpointer, signal, metrics).await
            }
            RecoveryDecision::Discard {
                pending,
                fallback,
                reason,
            } => match discard(checkpointer, metrics, signal, pending, fallback, reason)? {
                Some(checkpoint) => Ok(Resolved::Checkpoint(checkpoint)),
                None => Ok(Resolved::Stream),
            },
            // Refuse rather than replay: a transactional sink may already have
            // committed, so the marker, body and sink state stay untouched.
            RecoveryDecision::Reject { pending, reason } => Err(ConnectorError::Unsupported(
                format!("refusing to replay interrupted checkpoint {pending}: {reason}"),
            )),
        }
    }

    /// Re-drive a promoted commit, discarding and replaying only when safe.
    async fn promote(
        checkpoint: Checkpoint,
        sources: &Sources,
        checkpointer: &mut Checkpointer,
        signal: &Mutex<Option<String>>,
        metrics: &MetricsRegistry,
    ) -> Result<Resolved, ConnectorError> {
        match checkpointer.promote(checkpoint.id).await {
            Ok(()) => Ok(Resolved::Checkpoint(checkpoint)),
            // An operational failure publishing a promoted commit must
            // propagate untouched: no fallback read, no discard, no source
            // rollback or start.
            Err(error @ ConnectorError::Storage(_)) => Err(error),
            // A non-storage re-drive failure leaves the commit unresolved. Only
            // a replay-safe sink may be discarded and replayed; otherwise keep
            // the pending evidence and surface the error.
            Err(error) => {
                if !checkpointer.replay_safe() {
                    return Err(error);
                }
                let fallback = Self::load(checkpointer, sources)?;
                let reason = format!("commit re-drive failed ({error})");
                match discard(
                    checkpointer,
                    metrics,
                    signal,
                    checkpoint.id,
                    fallback,
                    &reason,
                )? {
                    Some(fallback) => Ok(Resolved::Checkpoint(fallback)),
                    None => Ok(Resolved::Stream),
                }
            }
        }
    }
}
