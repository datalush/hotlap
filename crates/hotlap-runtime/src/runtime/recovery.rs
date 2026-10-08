//! Recovery: restore the newest valid checkpoint, reopen sources and replay.
//!
//! A restart loads the last valid checkpoint (engine snapshot plus captured
//! source offsets), restores the engine, reopens each source split at its
//! captured offset and feeds the resulting stream back in. Because the engine
//! is restored to the checkpoint before the offset is replayed, replaying the
//! log from that offset yields exactly the state the crashed run had reached.

use std::sync::Mutex;

use hotlap::Hotlap;
use hotlap_engine::{EngineSnapshot, MetricsRegistry};

use crate::runtime::checkpoint::{Checkpoint, Checkpointer};
use crate::runtime::checkpoint_body::hotlap_err;
use crate::runtime::pipeline;
use hotlap_connectors::error::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, SourceStream};

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
            if let Some(checkpoint) = checkpointer.read_body(pending)?
                && checkpointer.redriable()
            {
                return Ok(RecoveryDecision::Promote(checkpoint));
            }
            return Ok(RecoveryDecision::Discard { pending, fallback });
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
    /// one replays from the fallback and records an explicit warning signal.
    pub async fn start(
        hotlap: &mut Hotlap,
        source: &dyn Source,
        checkpointer: &mut Checkpointer,
        signal: &Mutex<Option<String>>,
        metrics: &MetricsRegistry,
    ) -> Result<SourceStream, ConnectorError> {
        let checkpoint = match Self::inspect(checkpointer)? {
            RecoveryDecision::Clean => return pipeline::merged_stream(source),
            RecoveryDecision::Resume(checkpoint) => checkpoint,
            RecoveryDecision::Promote(checkpoint) => {
                checkpointer.promote(checkpoint.id).await?;
                checkpoint
            }
            RecoveryDecision::Discard { pending, fallback } => {
                metrics.inc("checkpoints_discarded");
                pipeline::record_error(
                    signal,
                    ConnectorError::Infrastructure(format!(
                        "discarded interrupted checkpoint {pending}: a sink is not \
                         re-drivable, replaying from the previous valid checkpoint"
                    )),
                );
                checkpointer.discard_commit(pending)?;
                match fallback {
                    Some(checkpoint) => checkpoint,
                    None => return pipeline::merged_stream(source),
                }
            }
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

/// Seeds the source's applied position with the captured offsets.
///
/// `Source::resume` reopens the splits at the captured offsets, but a custom
/// source may not record them as applied. Without this seed a checkpoint taken
/// before the first post-recovery commit captures a stale position, and a later
/// crash replays the log over the restored snapshot (double-apply). `commit`
/// only advances the position, so seeding an offset the source already recorded
/// is a no-op.
fn seed_applied(source: &dyn Source, state: &SourceState) -> Result<(), ConnectorError> {
    for (&split, &offset) in &state.offsets {
        source.commit(split, offset)?;
    }
    Ok(())
}

/// Restore the engine snapshot through the public facade.
fn restore(hotlap: &mut Hotlap, snapshot: &EngineSnapshot) -> Result<(), ConnectorError> {
    hotlap.restore(snapshot).map_err(hotlap_err)
}

/// Newest checkpoint above `floor` with a `commit` marker but no `valid` one.
///
/// The floor is the newest valid id: an interrupted commit is always later, so
/// older markers are ignored.
fn pending_commit(
    checkpointer: &Checkpointer,
    floor: Option<u64>,
) -> Result<Option<u64>, ConnectorError> {
    let floor = floor.unwrap_or(0);
    for id in checkpointer.ids_descending()? {
        if id <= floor {
            break;
        }
        let base = format!("checkpoint/{id}");
        if checkpointer.has_key(&format!("{base}/commit"))?
            && !checkpointer.has_key(&format!("{base}/valid"))?
        {
            return Ok(Some(id));
        }
    }
    Ok(None)
}
