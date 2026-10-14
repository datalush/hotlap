mod delta;
pub(crate) mod factorized;
mod helpers;
mod snapshot;
mod state;

use arrow::datatypes::SchemaRef;
use arrow::row::RowConverter;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::{KeyConverter, full_converter};
use crate::zset::consolidate_with;
use factorized::demand::Demand;

use helpers::{concat_zsets, touched_union};

/// Stateful delta-incremental inner equi-join of two keyed streams.
///
/// Both sides accumulate in [`KeyedArrangement`]s keyed by their join columns.
/// Each `apply` evaluates only the join keys touched by the two deltas and, per
/// key, emits the *change* to the join using the delta-join identity
/// `Δ(L×R) = ΔL×R_prev + L_prev×ΔR + ΔL×ΔR`, where the `_prev` states are the
/// retained per-key sides from before this push. A push therefore costs
/// `O(sum over touched keys |ΔL[k]|*|R[k]| + |L[k]|*|ΔR[k]|)` rather than the
/// full `|L[k]|*|R[k]|` product, independent of untouched keyspace.
///
/// Output is `left || right` plus the signed `diff` (the product of both diffs).
/// The schema is frozen after the first `apply`, so the side key converters and
/// the output full-row converter are built once and reused across pushes.
pub struct Join {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left: Option<KeyedArrangement>,
    right: Option<KeyedArrangement>,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    out_schema: Option<SchemaRef>,
    left_conv: Option<KeyConverter>,
    right_conv: Option<KeyConverter>,
    out_conv: Option<RowConverter>,
    /// Immutable plan demand fixed once normalized input schemas are known.
    demand: Option<Demand>,
    #[cfg(test)]
    work: u64,
}

pub(crate) struct JoinCutCommit {
    left: crate::arrange::StagedFinalWeights,
    right: crate::arrange::StagedFinalWeights,
}

pub(crate) struct PreparedJoinSources {
    left: crate::arrange::PreparedSourceUpdate,
    right: crate::arrange::PreparedSourceUpdate,
}

impl PreparedJoinSources {
    pub(crate) fn changed_keys(&self) -> std::collections::BTreeSet<Vec<u8>> {
        self.left
            .changed_keys()
            .chain(self.right.changed_keys())
            .map(<[u8]>::to_vec)
            .collect()
    }

    pub(crate) fn proposed_left<'a>(
        &'a self,
        join: &'a Join,
        key: &[u8],
    ) -> Option<&'a crate::arrange::source_stats::SourceStats> {
        self.left.proposed_stats(join.left.as_ref()?, key)
    }

    pub(crate) fn proposed_right<'a>(
        &'a self,
        join: &'a Join,
        key: &[u8],
    ) -> Option<&'a crate::arrange::source_stats::SourceStats> {
        self.right.proposed_stats(join.right.as_ref()?, key)
    }
}

impl Join {
    pub(crate) fn source_schemas_for_cut(&self) -> Option<(SchemaRef, SchemaRef)> {
        Some((self.left_schema.clone()?, self.right_schema.clone()?))
    }

    pub(crate) fn joined_schema_for_cut(&mut self) -> Result<SchemaRef, EngineError> {
        self.output_schema()
    }

    pub(crate) fn left_for_cut(&self) -> Option<&KeyedArrangement> {
        self.left.as_ref()
    }

    pub(crate) fn right_for_cut(&self) -> Option<&KeyedArrangement> {
        self.right.as_ref()
    }

    pub(crate) fn prepare_source_updates(
        &self,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<PreparedJoinSources, EngineError> {
        let left_arrangement = self.left.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join left arrangement uninitialized".into())
        })?;
        let right_arrangement = self.right.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join right arrangement uninitialized".into())
        })?;
        let left_keys = self.left_conv.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join left converter uninitialized".into())
        })?;
        let right_keys = self.right_conv.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join right converter uninitialized".into())
        })?;
        Ok(PreparedJoinSources {
            left: left_arrangement.prepare_source_update(left, left_keys)?,
            right: right_arrangement.prepare_source_update(right, right_keys)?,
        })
    }

    pub(crate) fn commit_source_updates(&mut self, updates: PreparedJoinSources) {
        updates.left.commit(
            self.left
                .as_mut()
                .expect("prepared left arrangement remains initialized"),
        );
        updates.right.commit(
            self.right
                .as_mut()
                .expect("prepared right arrangement remains initialized"),
        );
    }

    pub(crate) fn register_demand(&mut self, demand: Demand) -> Result<(), EngineError> {
        match &self.demand {
            Some(existing) if existing != &demand => Err(EngineError::Unsupported(
                "join aggregate demand changed after registration".into(),
            )),
            Some(_) => Ok(()),
            None => {
                if let Some(arrangement) = &mut self.left {
                    arrangement.register_source_stats(
                        &demand.left_columns,
                        &demand.left_column_types,
                        &demand.left_column_sums,
                    )?;
                }
                if let Some(arrangement) = &mut self.right {
                    arrangement.register_source_stats(
                        &demand.right_columns,
                        &demand.right_column_types,
                        &demand.right_column_sums,
                    )?;
                }
                self.demand = Some(demand);
                Ok(())
            }
        }
    }

    pub(crate) fn cut_schema(
        &mut self,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<arrow::datatypes::SchemaRef, EngineError> {
        if self.left_keys.len() != self.right_keys.len() {
            return Err(EngineError::Unsupported(
                "join sides must share the same key arity".into(),
            ));
        }
        self.ensure_ready(left, right)?;
        self.output_schema()
    }

    #[allow(dead_code)]
    pub(crate) fn stage_cut_commit(
        &self,
        prepared: &factorized::PreparedCut,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<JoinCutCommit, EngineError> {
        let left_arrangement = self.left.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join left arrangement uninitialized".into())
        })?;
        let right_arrangement = self.right.as_ref().ok_or_else(|| {
            EngineError::Infrastructure("join right arrangement uninitialized".into())
        })?;
        let left_rows: Vec<_> = prepared
            .left
            .iter()
            .map(|r| {
                (
                    r.identity.join_key.clone(),
                    r.identity.payload.clone(),
                    r.old,
                    r.new,
                )
            })
            .collect();
        let right_rows: Vec<_> = prepared
            .right
            .iter()
            .map(|r| {
                (
                    r.identity.join_key.clone(),
                    r.identity.payload.clone(),
                    r.old,
                    r.new,
                )
            })
            .collect();
        Ok(JoinCutCommit {
            left: left_arrangement.stage_final_weights(&left.schema(), &left_rows)?,
            right: right_arrangement.stage_final_weights(&right.schema(), &right_rows)?,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn commit_cut(&mut self, staged: JoinCutCommit) {
        self.left
            .as_mut()
            .expect("staged initialized left arrangement")
            .commit_final_weights(staged.left);
        self.right
            .as_mut()
            .expect("staged initialized right arrangement")
            .commit_final_weights(staged.right);
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.left.is_some() || self.right.is_some()
    }

    pub(crate) fn has_arrangement_storage(&self) -> bool {
        [&self.left, &self.right]
            .into_iter()
            .flatten()
            .any(|arrangement| arrangement.has_storage())
    }

    pub(crate) fn has_retained_rows(&self) -> bool {
        [&self.left, &self.right]
            .into_iter()
            .flatten()
            .any(|arrangement| !arrangement.is_empty())
    }

    pub(crate) fn memory_usage(&self) -> Result<(usize, usize, usize), EngineError> {
        let mut totals = (
            0usize,
            self.left_keys
                .capacity()
                .checked_add(self.right_keys.capacity())
                .and_then(|count| count.checked_mul(std::mem::size_of::<usize>()))
                .ok_or_else(|| {
                    EngineError::Infrastructure("join config capacity overflow".into())
                })?,
            0usize,
        );
        if let Some(demand) = &self.demand {
            let mut demand_bytes = vec_capacity_bytes(&demand.left_join_key)?;
            for bytes in [
                vec_capacity_bytes(&demand.right_join_key)?,
                vec_capacity_bytes(&demand.group_key)?,
                vec_capacity_bytes(&demand.left_columns)?,
                vec_capacity_bytes(&demand.right_columns)?,
                vec_capacity_bytes(&demand.left_column_sums)?,
                vec_capacity_bytes(&demand.right_column_sums)?,
                vec_capacity_bytes(&demand.left_column_types)?,
                vec_capacity_bytes(&demand.right_column_types)?,
            ] {
                demand_bytes = demand_bytes.checked_add(bytes).ok_or_else(|| {
                    EngineError::Infrastructure("join demand size overflow".into())
                })?;
            }
            for data_type in demand
                .left_column_types
                .iter()
                .chain(&demand.right_column_types)
            {
                demand_bytes = demand_bytes
                    .checked_add(data_type_allocated_bytes(data_type)?)
                    .ok_or_else(|| {
                        EngineError::Infrastructure("join demand type overflow".into())
                    })?;
            }
            totals.1 = totals.1.checked_add(demand_bytes).ok_or_else(|| {
                EngineError::Infrastructure("join demand capacity overflow".into())
            })?;
        }
        for arrangement in [&self.left, &self.right].into_iter().flatten() {
            let (payload, capacity, estimate) = arrangement.memory_usage()?;
            totals.0 = totals
                .0
                .checked_add(payload)
                .ok_or_else(|| EngineError::Infrastructure("join payload overflow".into()))?;
            totals.1 = totals
                .1
                .checked_add(capacity)
                .ok_or_else(|| EngineError::Infrastructure("join capacity overflow".into()))?;
            totals.2 = totals
                .2
                .checked_add(estimate)
                .ok_or_else(|| EngineError::Infrastructure("join estimate overflow".into()))?;
        }
        Ok(totals)
    }

    /// Creates a join over the join column indices `left_keys` and `right_keys`.
    /// Side schemas are learned from the first `apply`.
    pub fn new(left_keys: &[usize], right_keys: &[usize]) -> Self {
        Self {
            left_keys: left_keys.to_vec(),
            right_keys: right_keys.to_vec(),
            left: None,
            right: None,
            left_schema: None,
            right_schema: None,
            out_schema: None,
            left_conv: None,
            right_conv: None,
            out_conv: None,
            demand: None,
            #[cfg(test)]
            work: 0,
        }
    }

    /// Applies one delta per side and returns the join changelog since the last
    /// call. Only the keys touched by these deltas are re-evaluated.
    pub fn apply(&mut self, left: &ZSetBatch, right: &ZSetBatch) -> Result<ZSetBatch, EngineError> {
        if self.left_keys.len() != self.right_keys.len() {
            return Err(EngineError::Unsupported(
                "join sides must share the same key arity".to_string(),
            ));
        }
        self.ensure_ready(left, right)?;
        let schema = self.output_schema()?;
        if self.out_conv.is_none() {
            self.out_conv = Some(full_converter(schema.as_ref())?);
        }
        let touched = {
            let left_conv = self.left_converter()?;
            let right_conv = self.right_converter()?;
            touched_union(left, left_conv, right, right_conv)?
        };
        // Capture the retained per-key sides *before* folding in this push's
        // deltas: the delta-join formula multiplies the deltas by these states.
        let previous = self.previous_states(&touched)?;
        self.apply_deltas(left, right)?;
        let (parts, work) = self.delta_join(&touched, &previous, left, right, &schema)?;
        #[cfg(test)]
        {
            self.work = work;
        }
        #[cfg(not(test))]
        let _ = work;
        // Consolidate the per-key parts into one canonical, sorted changelog so
        // the join's output ordering does not depend on touched-key order.
        let out_conv = self.out_converter()?;
        consolidate_with(out_conv, &concat_zsets(&schema, &parts)?)
    }

    /// Number of join pairs evaluated by the last `apply` (test only).
    #[cfg(test)]
    pub fn work(&self) -> u64 {
        self.work
    }
}

fn vec_capacity_bytes<T>(values: &Vec<T>) -> Result<usize, EngineError> {
    values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| EngineError::Infrastructure("join demand capacity overflow".into()))
}

pub(crate) fn data_type_allocated_bytes(
    data_type: &arrow::datatypes::DataType,
) -> Result<usize, EngineError> {
    use arrow::datatypes::DataType;
    match data_type {
        DataType::Dictionary(key, value) => {
            let mut bytes = std::mem::size_of::<DataType>()
                .checked_mul(2)
                .ok_or_else(|| {
                    EngineError::Infrastructure("join demand type allocation overflow".into())
                })?;
            let key_bytes = data_type_allocated_bytes(key)?;
            let value_bytes = data_type_allocated_bytes(value)?;
            bytes = bytes
                .checked_add(key_bytes)
                .and_then(|bytes| bytes.checked_add(value_bytes))
                .ok_or_else(|| {
                    EngineError::Infrastructure("join demand type allocation overflow".into())
                })?;
            Ok(bytes)
        }
        _ => Ok(0),
    }
}

#[cfg(test)]
mod tests;
