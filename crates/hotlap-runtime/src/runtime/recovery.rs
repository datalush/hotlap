//! Recovery: restore the newest valid checkpoint, reopen sources and replay.
//!
//! A restart loads the last valid checkpoint (engine snapshot plus captured
//! per-source offsets), validates it against the declared sources, restores the
//! engine, reopens each source split at its captured offset and feeds the
//! resulting streams back in. Because the engine is restored to the checkpoint
//! before the offsets are replayed, replaying the log from those offsets yields
//! exactly the state the crashed run had reached.

mod pending;
mod restore;
mod sources;

use std::sync::Mutex;

use hotlap::Hotlap;
use hotlap_engine::MetricsRegistry;

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::sources::{InputStream, Sources};
use hotlap_connectors::error::ConnectorError;
use pending::{discard, pending_commit};
use restore::restore;
use sources::{read_body, read_valid};

/// The last valid checkpoint, if the store holds one.
pub struct Recovery;

/// How startup should recover, including a checkpoint interrupted mid-commit.
///
/// An interrupted commit is one whose durable `commit` marker is present but
/// whose `valid` marker is not: the body is complete and the sinks may or may
/// not have committed before the process stopped.
#[derive(Debug)]
pub enum RecoveryDecision {
    /// No usable checkpoint: start clean.
    Clean,
    /// Resume from this already valid checkpoint.
    Resume(Checkpoint),
    /// Re-drive the interrupted commit for `Checkpoint` (every sink declares its
    /// commit re-drivable), publish it and resume from it without replay.
    Promote(Checkpoint),
    /// The interrupted checkpoint cannot be re-driven: discard `pending` and
    /// replay from `fallback` (the newest valid checkpoint, if any).
    Discard {
        /// Id of the interrupted checkpoint to discard.
        pending: u64,
        /// Newest valid checkpoint to replay from, if one exists.
        fallback: Option<Checkpoint>,
        /// Why the commit cannot be re-driven, for the warning signal.
        reason: &'static str,
    },
}

impl Recovery {
    /// Decide how to recover, detecting a checkpoint that was mid-commit when
    /// the process stopped.
    ///
    /// The newest valid checkpoint and any pending commit body are validated
    /// against `sources` before a decision is returned, so a promotion or a
    /// resume never runs against an incompatible declaration. An undecodable
    /// current-format body keeps the SP8 discard path; a foreign or
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
                    "a sink is not re-drivable"
                }
                None => "the pending commit body is corrupt or incomplete",
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
    /// failed re-drive is itself discarded and replayed rather than aborting
    /// startup, so a hostile marker cannot wedge every restart in a retry loop.
    pub async fn start(
        hotlap: &mut Hotlap,
        sources: &Sources,
        checkpointer: &mut Checkpointer,
        signal: &Mutex<Option<String>>,
        metrics: &MetricsRegistry,
    ) -> Result<InputStream, ConnectorError> {
        checkpointer.sweep_stale_commits()?;
        let checkpoint = match Self::inspect(checkpointer, sources)? {
            RecoveryDecision::Clean => return sources.stream(),
            RecoveryDecision::Resume(checkpoint) => checkpoint,
            RecoveryDecision::Promote(checkpoint) => {
                match checkpointer.promote(checkpoint.id).await {
                    Ok(()) => checkpoint,
                    Err(error) => {
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
                            Some(fallback) => fallback,
                            None => return sources.stream(),
                        }
                    }
                }
            }
            RecoveryDecision::Discard {
                pending,
                fallback,
                reason,
            } => match discard(checkpointer, metrics, signal, pending, fallback, reason)? {
                Some(checkpoint) => checkpoint,
                None => return sources.stream(),
            },
        };
        checkpointer.resume_after(checkpoint.id);
        Self::resume(hotlap, sources, &checkpoint)
    }

    /// Newest valid checkpoint, or `None` for a clean start.
    ///
    /// The `latest` pointer is tried first. If its checkpoint fails to decode
    /// as the current format, older checkpoints are tried newest-first, so a
    /// corrupt tip does not abort startup while a valid predecessor remains. A
    /// checkpoint that validates against the declared sources is returned; an
    /// incompatible one is a fatal `Unsupported` rather than a silent skip.
    pub fn load(
        checkpointer: &Checkpointer,
        sources: &Sources,
    ) -> Result<Option<Checkpoint>, ConnectorError> {
        match checkpointer.latest() {
            Ok(Some(id)) => {
                if let Some(checkpoint) = read_valid(checkpointer, id)? {
                    sources::validate(sources, &checkpoint.sources, &checkpoint.engine)?;
                    return Ok(Some(checkpoint));
                }
            }
            // A damaged `latest` pointer is current-format corruption: scan the
            // store for an older valid checkpoint instead of aborting startup.
            // An operational failure propagates and is never treated as absence.
            Ok(None) | Err(ConnectorError::Corruption(_) | ConnectorError::Missing(_)) => {}
            Err(error) => return Err(error),
        }
        for id in checkpointer.ids_descending()? {
            if let Some(checkpoint) = read_valid(checkpointer, id)? {
                sources::validate(sources, &checkpoint.sources, &checkpoint.engine)?;
                return Ok(Some(checkpoint));
            }
        }
        Ok(None)
    }

    /// Restore `checkpoint` into `hotlap` and reopen `sources` at the captured
    /// offsets, yielding a stream that replays from the checkpoint.
    ///
    /// [`SourceState`](hotlap_connectors::source::SourceState) holds the offset of the
    /// **next** record to read, advanced only after a batch is applied. The
    /// checkpoint therefore already contains every record below that offset and
    /// replay must start exactly there: starting one record earlier duplicates,
    /// one later loses. The runtime commits offsets after ingestion, so no
    /// in-flight batch can break the invariant.
    ///
    /// The checkpoint is validated before the engine is restored or any source
    /// is reopened. Errors when a source can no longer serve a captured offset,
    /// so insufficient retention fails loudly instead of losing records.
    pub fn resume(
        hotlap: &mut Hotlap,
        sources: &Sources,
        checkpoint: &Checkpoint,
    ) -> Result<InputStream, ConnectorError> {
        sources::validate(sources, &checkpoint.sources, &checkpoint.engine)?;
        restore(hotlap, &checkpoint.engine)?;
        let splits = sources::resume(sources, &checkpoint.sources)?;
        sources.stream_with(&splits)
    }
}
