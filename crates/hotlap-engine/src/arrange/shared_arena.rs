use super::{KeyedArrangement, PreparedSourceUpdate};
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use arrow::datatypes::SchemaRef;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArrangementDescriptor {
    pub(crate) root_left_source: u64,
    pub(crate) root_right_source: u64,
    pub(crate) root_left_keys: Vec<usize>,
    pub(crate) root_right_keys: Vec<usize>,
    pub(crate) root_left_schema: SchemaRef,
    pub(crate) root_right_schema: SchemaRef,
    pub(crate) source: u64,
    pub(crate) partner: u64,
    pub(crate) key_indices: Vec<usize>,
    pub(crate) partner_key_indices: Vec<usize>,
    pub(crate) schema: SchemaRef,
    pub(crate) cohort: u64,
    pub(crate) consumed_revision_start: u64,
    pub(crate) consumed_revision_end: u64,
    pub(crate) demand_none: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ArrangementHandle {
    pub(crate) slot: usize,
    pub(crate) generation: u64,
}

struct ArenaOwner {
    descriptor: ArrangementDescriptor,
    arrangement: KeyedArrangement,
    converter: KeyConverter,
    leases: usize,
    last_revision: u64,
    #[cfg(test)]
    commits: u64,
}

struct ArenaSlot {
    generation: u64,
    owner: Option<ArenaOwner>,
}

/// Derived store for physical keyed relations. It owns no input history and
/// never duplicates a live arrangement payload.
#[derive(Default)]
pub(crate) struct ArrangementArena {
    slots: Vec<ArenaSlot>,
    free: Vec<usize>,
}

struct StagedArenaUpdate {
    handle: ArrangementHandle,
    revision_start: u64,
    revision_end: u64,
    identity: DeltaIdentity,
    prepared: PreparedSourceUpdate,
}

#[derive(PartialEq, Eq)]
struct DeltaIdentity {
    schema: SchemaRef,
    rows: Vec<Vec<u8>>,
    diffs: Vec<i64>,
}

/// Holds mutable arena access for one evaluation frame. Owner reads are
/// immutable until `commit`; staged updates are private tokens, not published
/// arrangement state.
pub(crate) struct ArrangementFrame<'a> {
    arena: &'a mut ArrangementArena,
    staged: Vec<StagedArenaUpdate>,
    failed: bool,
}

impl ArrangementArena {
    pub(crate) fn lease(
        &mut self,
        descriptor: ArrangementDescriptor,
    ) -> Result<ArrangementHandle, EngineError> {
        if !descriptor.demand_none
            || descriptor.consumed_revision_start > descriptor.consumed_revision_end
        {
            return Err(EngineError::Unsupported(
                "ineligible shared arrangement descriptor".into(),
            ));
        }
        if let Some((slot, entry)) = self.slots.iter_mut().enumerate().find_map(|(slot, entry)| {
            entry
                .owner
                .as_mut()
                .filter(|owner| owner.descriptor == descriptor)
                .map(|owner| (slot, owner))
        }) {
            entry.leases = entry.leases.checked_add(1).ok_or_else(|| {
                EngineError::Infrastructure("shared arrangement lease count overflow".into())
            })?;
            return Ok(ArrangementHandle {
                slot,
                generation: self.slots[slot].generation,
            });
        }
        let converter = KeyConverter::new(descriptor.schema.as_ref(), &descriptor.key_indices)?;
        let arrangement =
            KeyedArrangement::new(descriptor.schema.clone(), &descriptor.key_indices)?;
        let last_revision = descriptor.consumed_revision_end;
        let owner = ArenaOwner {
            descriptor,
            arrangement,
            converter,
            leases: 1,
            last_revision,
            #[cfg(test)]
            commits: 0,
        };
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.slots.push(ArenaSlot {
                    generation: 0,
                    owner: None,
                });
                self.slots.len() - 1
            }
        };
        self.slots[slot].owner = Some(owner);
        Ok(ArrangementHandle {
            slot,
            generation: self.slots[slot].generation,
        })
    }

    pub(crate) fn lease_arrangement(
        &mut self,
        descriptor: ArrangementDescriptor,
        arrangement: KeyedArrangement,
    ) -> Result<ArrangementHandle, EngineError> {
        if !descriptor.demand_none
            || descriptor.consumed_revision_start > descriptor.consumed_revision_end
            || arrangement.schema().as_ref() != descriptor.schema.as_ref()
            || arrangement.key_indices() != descriptor.key_indices
        {
            return Err(EngineError::Unsupported(
                "external arrangement does not match its descriptor".into(),
            ));
        }
        if let Some((slot, owner)) = self.slots.iter_mut().enumerate().find_map(|(slot, entry)| {
            entry
                .owner
                .as_mut()
                .filter(|owner| owner.descriptor == descriptor)
                .map(|owner| (slot, owner))
        }) {
            if !arrangement.is_empty() {
                return Err(EngineError::Unsupported(
                    "cannot merge a non-empty private table into an existing shared owner".into(),
                ));
            }
            owner.leases = owner.leases.checked_add(1).ok_or_else(|| {
                EngineError::Infrastructure("shared arrangement lease count overflow".into())
            })?;
            return Ok(ArrangementHandle {
                slot,
                generation: self.slots[slot].generation,
            });
        }
        let converter = KeyConverter::new(descriptor.schema.as_ref(), &descriptor.key_indices)?;
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.slots.push(ArenaSlot {
                    generation: 0,
                    owner: None,
                });
                self.slots.len() - 1
            }
        };
        let last_revision = descriptor.consumed_revision_end;
        self.slots[slot].owner = Some(ArenaOwner {
            descriptor,
            arrangement,
            converter,
            leases: 1,
            last_revision,
            #[cfg(test)]
            commits: 0,
        });
        Ok(ArrangementHandle {
            slot,
            generation: self.slots[slot].generation,
        })
    }

    pub(crate) fn release(&mut self, handle: ArrangementHandle) -> Result<(), EngineError> {
        let slot = self.slot_mut(handle)?;
        let owner = slot.owner.as_mut().expect("validated live handle");
        owner.leases = owner.leases.checked_sub(1).ok_or_else(|| {
            EngineError::Infrastructure("shared arrangement lease underflow".into())
        })?;
        if owner.leases == 0 {
            slot.owner = None;
            if let Some(next) = slot.generation.checked_add(1) {
                slot.generation = next;
                self.free.push(handle.slot);
            }
        }
        Ok(())
    }

    pub(crate) fn owner(
        &self,
        handle: ArrangementHandle,
    ) -> Result<&KeyedArrangement, EngineError> {
        self.slot(handle)?
            .owner
            .as_ref()
            .map(|owner| &owner.arrangement)
            .ok_or_else(|| EngineError::Unsupported("stale shared arrangement handle".into()))
    }

    pub(crate) fn frame(&mut self) -> ArrangementFrame<'_> {
        ArrangementFrame {
            arena: self,
            staged: Vec::new(),
            failed: false,
        }
    }

    pub(crate) fn memory_usage(
        &self,
        report: &mut crate::core::memory::MemoryReport,
    ) -> Result<(), EngineError> {
        use crate::core::memory::{checked_add, checked_capacity_bytes};
        report.mark_covered("shared_join_arrangements");
        report.add_estimate(checked_capacity_bytes(
            self.slots.capacity(),
            std::mem::size_of::<ArenaSlot>(),
        )?)?;
        report.add_estimate(checked_capacity_bytes(
            self.free.capacity(),
            std::mem::size_of::<usize>(),
        )?)?;
        if !self.slots.is_empty() {
            report.mark_unknown("shared_arrangement_registry_allocator_overhead");
        }
        for owner in self.slots.iter().filter_map(|slot| slot.owner.as_ref()) {
            let (payload, keys, estimate) = owner.arrangement.memory_usage()?;
            report.add_join(payload, keys, estimate)?;
            report.add_estimate(std::mem::size_of::<ArenaOwner>())?;
            report.add_capacity(checked_capacity_bytes(
                checked_add(
                    owner.descriptor.key_indices.capacity(),
                    owner.descriptor.partner_key_indices.capacity(),
                    "shared arrangement keys",
                )?,
                std::mem::size_of::<usize>(),
            )?)?;
            report.mark_unknown("shared_arrangement_converter_and_schema_metadata");
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn owner_commit_count(&self, handle: ArrangementHandle) -> Result<u64, EngineError> {
        Ok(self
            .slot(handle)?
            .owner
            .as_ref()
            .expect("validated live handle")
            .commits)
    }

    #[cfg(test)]
    pub(crate) fn owner_lease_count(
        &self,
        handle: ArrangementHandle,
    ) -> Result<usize, EngineError> {
        Ok(self
            .slot(handle)?
            .owner
            .as_ref()
            .expect("validated live handle")
            .leases)
    }

    #[cfg(test)]
    pub(crate) fn owner_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.owner.is_some())
            .count()
    }

    fn slot(&self, handle: ArrangementHandle) -> Result<&ArenaSlot, EngineError> {
        let slot = self
            .slots
            .get(handle.slot)
            .ok_or_else(|| EngineError::Unsupported("stale shared arrangement handle".into()))?;
        if slot.generation != handle.generation || slot.owner.is_none() {
            return Err(EngineError::Unsupported(
                "stale shared arrangement handle".into(),
            ));
        }
        Ok(slot)
    }

    fn slot_mut(&mut self, handle: ArrangementHandle) -> Result<&mut ArenaSlot, EngineError> {
        let slot = self
            .slots
            .get_mut(handle.slot)
            .ok_or_else(|| EngineError::Unsupported("stale shared arrangement handle".into()))?;
        if slot.generation != handle.generation || slot.owner.is_none() {
            return Err(EngineError::Unsupported(
                "stale shared arrangement handle".into(),
            ));
        }
        Ok(slot)
    }
}

impl ArrangementFrame<'_> {
    pub(crate) fn lease(
        &mut self,
        descriptor: ArrangementDescriptor,
    ) -> Result<ArrangementHandle, EngineError> {
        self.arena.lease(descriptor)
    }

    pub(crate) fn lease_arrangement(
        &mut self,
        descriptor: ArrangementDescriptor,
        arrangement: KeyedArrangement,
    ) -> Result<ArrangementHandle, EngineError> {
        self.arena.lease_arrangement(descriptor, arrangement)
    }

    pub(crate) fn owner(
        &self,
        handle: ArrangementHandle,
    ) -> Result<&KeyedArrangement, EngineError> {
        self.arena.owner(handle)
    }

    #[cfg(test)]
    pub(crate) fn stage(
        &mut self,
        handle: ArrangementHandle,
        revision: u64,
        delta: &ZSetBatch,
    ) -> Result<(), EngineError> {
        self.stage_range(handle, revision, revision, delta)
    }

    pub(crate) fn stage_range(
        &mut self,
        handle: ArrangementHandle,
        revision_start: u64,
        revision_end: u64,
        delta: &ZSetBatch,
    ) -> Result<(), EngineError> {
        if self.failed {
            return Err(EngineError::Unsupported(
                "shared arrangement frame has failed".into(),
            ));
        }
        let result = self.stage_inner(handle, revision_start, revision_end, delta);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn stage_inner(
        &mut self,
        handle: ArrangementHandle,
        revision_start: u64,
        revision_end: u64,
        delta: &ZSetBatch,
    ) -> Result<(), EngineError> {
        self.arena.slot(handle)?;
        if revision_start > revision_end
            || revision_start
                != owner_last_revision(self.arena, handle)?
                    .checked_add(1)
                    .ok_or_else(|| {
                        EngineError::Infrastructure("shared source revision overflow".into())
                    })?
        {
            return Err(EngineError::Unsupported(
                "shared source revision is not the next consumed range".into(),
            ));
        }
        let identity = delta_identity(delta)?;
        if let Some(existing) = self.staged.iter().find(|update| update.handle == handle) {
            if existing.revision_start != revision_start
                || existing.revision_end != revision_end
                || existing.identity != identity
            {
                return Err(EngineError::Unsupported(
                    "shared owner consumed conflicting source revision in one frame".into(),
                ));
            }
            return Ok(());
        }
        let owner = self
            .arena
            .slot(handle)?
            .owner
            .as_ref()
            .expect("validated live handle");
        let prepared = owner
            .arrangement
            .prepare_source_update(delta, &owner.converter)?;
        self.staged.push(StagedArenaUpdate {
            handle,
            revision_start,
            revision_end,
            identity,
            prepared,
        });
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<(), EngineError> {
        if self.failed {
            return Err(EngineError::Unsupported(
                "shared arrangement frame has failed".into(),
            ));
        }
        for update in self.staged {
            let slot = self.arena.slot_mut(update.handle)?;
            let owner = slot.owner.as_mut().expect("validated live handle");
            update.prepared.commit(&mut owner.arrangement);
            owner.last_revision = update.revision_end;
            #[cfg(test)]
            {
                owner.commits = owner.commits.saturating_add(1);
            }
        }
        Ok(())
    }
}

fn owner_last_revision(
    arena: &ArrangementArena,
    handle: ArrangementHandle,
) -> Result<u64, EngineError> {
    Ok(arena
        .slot(handle)?
        .owner
        .as_ref()
        .expect("validated live handle")
        .last_revision)
}

fn delta_identity(batch: &ZSetBatch) -> Result<DeltaIdentity, EngineError> {
    let converter = crate::keys::full_converter(batch.schema().as_ref())?;
    let rows = converter.convert_columns(batch.batch.columns())?;
    let encoded = (0..batch.len())
        .map(|row| rows.row(row).as_ref().to_vec())
        .collect();
    let diffs = crate::zset::int64_diffs(batch.diff())?;
    Ok(DeltaIdentity {
        schema: batch.schema(),
        rows: encoded,
        diffs: (0..batch.len()).map(|row| diffs.value(row)).collect(),
    })
}

#[cfg(test)]
mod tests;
