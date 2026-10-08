//! Recovery: restore the newest valid checkpoint, reopen sources and replay.
//!
//! A restart loads the last valid checkpoint (engine snapshot plus captured
//! source offsets), restores the engine, reopens each source split at its
//! captured offset and feeds the resulting stream back in. Because the engine
//! is restored to the checkpoint before the offset is replayed, replaying the
//! log from that offset yields exactly the state the crashed run had reached.

mod pending;
mod restore;

use std::sync::Mutex;

use hotlap::Hotlap;
use hotlap_engine::MetricsRegistry;

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::pipeline;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceStream};
use pending::{discard, pending_commit};
use restore::{restore, seed_applied};

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
    /// Without a commit marker this is exactly [`Self::load`]: the newest valid
    /// checkpoint, or a clean start. With a marker and a complete body, an
    /// interrupted commit is either promoted (every sink declares its commit
    /// re-drivable) or explicitly discarded and replayed from the previous valid
    /// checkpoint.
    pub fn inspect(checkpointer: &Checkpointer) -> Result<RecoveryDecision, ConnectorError> {
        let fallback = Self::load(checkpointer)?;
        if let Some(pending) = pending_commit(checkpointer, fallback.as_ref().map(|c| c.id))? {
            // A pending body that fails to decode is not promotable: fall
            // through to Discard so the tolerant newest-first fallback wins.
            // The reason distinguishes an undecodable body from a body whose
            // sinks simply cannot re-drive the commit.
            let reason = match checkpointer.read_body(pending).ok().flatten() {
                Some(checkpoint) if checkpointer.redriable() => {
                    return Ok(RecoveryDecision::Promote(checkpoint));
                }
                Some(_) => "a sink is not re-drivable",
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
        source: &dyn Source,
        checkpointer: &mut Checkpointer,
        signal: &Mutex<Option<String>>,
        metrics: &MetricsRegistry,
    ) -> Result<SourceStream, ConnectorError> {
        checkpointer.sweep_stale_commits()?;
        let checkpoint = match Self::inspect(checkpointer)? {
            RecoveryDecision::Clean => return pipeline::merged_stream(source),
            RecoveryDecision::Resume(checkpoint) => checkpoint,
            RecoveryDecision::Promote(checkpoint) => {
                match checkpointer.promote(checkpoint.id).await {
                    Ok(()) => checkpoint,
                    Err(error) => {
                        let fallback = Self::load(checkpointer)?;
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
                            None => return pipeline::merged_stream(source),
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
                None => return pipeline::merged_stream(source),
            },
        };
        checkpointer.resume_after(checkpoint.id);
        Self::resume(hotlap, source, &checkpoint)
    }

    /// Newest valid checkpoint, or `None` for a clean start.
    ///
    /// The `latest` pointer is tried first. If its checkpoint fails to decode
    /// or validate, older checkpoints are tried newest-first, so a corrupt tip
    /// does not abort startup while a valid predecessor remains. A checkpoint
    /// is only visible once every part (engine, sources, `valid` marker) is
    /// written, so a readable one is always coherent.
    pub fn load(checkpointer: &Checkpointer) -> Result<Option<Checkpoint>, ConnectorError> {
        if let Some(id) = checkpointer.latest()?
            && let Ok(checkpoint) = checkpointer.read(id)
        {
            return Ok(Some(checkpoint));
        }
        for id in checkpointer.ids_descending()? {
            if let Ok(checkpoint) = checkpointer.read(id) {
                return Ok(Some(checkpoint));
            }
        }
        Ok(None)
    }

    /// Restore `checkpoint` into `hotlap` and reopen `source` at the captured
    /// offsets, yielding a stream that replays from the checkpoint.
    ///
    /// [`SourceState`](hotlap_connectors::source::SourceState) holds the offset of the
    /// **next** record to read, advanced only after a batch is applied. The
    /// checkpoint therefore already contains every record below that offset and
    /// replay must start exactly there: starting one record earlier duplicates,
    /// one later loses. The runtime commits offsets after ingestion, so no
    /// in-flight batch can break the invariant.
    ///
    /// Errors when the source can no longer serve a captured offset, so
    /// insufficient retention fails loudly instead of losing records.
    pub fn resume(
        hotlap: &mut Hotlap,
        source: &dyn Source,
        checkpoint: &Checkpoint,
    ) -> Result<SourceStream, ConnectorError> {
        restore(hotlap, &checkpoint.engine)?;
        let splits = source.resume(&checkpoint.sources)?;
        seed_applied(source, &checkpoint.sources)?;
        pipeline::merged_stream_from(source, &splits)
    }
}
