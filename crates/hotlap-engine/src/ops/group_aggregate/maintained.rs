use std::collections::BTreeSet;

use hotlap_core::plan_normalize::ColumnMapping;

use crate::arrange::KeyedArrangement;
use crate::error::EngineError;
use crate::ops::join::factorized::group_codec::GroupKeyCodec;
use crate::ops::join::factorized::group_memo::{Contribution, GroupMemo, GroupMemoStage};
use crate::ops::join::factorized::group_metrics::{
    JoinMetricDemand, SourceMeasure, compile_metric_demands,
};
use crate::ops::join::factorized::raw_bounds::RawColumnDemand;
use hotlap_core::snapshot::{AggValue, GroupEntry};

/// Derived state owned by the concrete authoritative GroupAggregate node.
/// It is intentionally omitted from logical snapshot serialization.
pub(crate) struct MaintainedGroupCut {
    pub(crate) mapping: ColumnMapping,
    pub(crate) codec: GroupKeyCodec,
    pub(crate) demands: Vec<JoinMetricDemand>,
    /// Memo metric proving physical joined row cardinality, independent of
    /// visible COUNT/nonnull/SUM slots.
    pub(crate) row_cardinality_index: usize,
    pub(crate) left_raw_columns: Vec<RawColumnDemand>,
    pub(crate) right_raw_columns: Vec<RawColumnDemand>,
    pub(crate) memo: GroupMemo,
}

impl MaintainedGroupCut {
    pub(super) fn validate_authoritative_history(
        &self,
        aggregates: &[hotlap_core::plan::AggSpec],
        groups: &std::collections::HashMap<Vec<u8>, GroupEntry>,
    ) -> Result<(), EngineError> {
        let memo_groups = self.memo.groups().collect::<Vec<_>>();
        // Negative physical source rows and bounded/unsupported summaries can
        // be valid Dense history. They are not a proof of corruption.
        if memo_groups.iter().any(|(_, group)| {
            group.negative_source_keys != 0
                || group.metrics.iter().any(|metric| {
                    metric.eligibility
                        != crate::ops::join::factorized::group_memo::MetricLimit::Exact
                })
        }) {
            return Ok(());
        }

        let mut expected_present = std::collections::HashSet::new();
        for (group_key, summary) in &memo_groups {
            let Some(row_metric) = summary.metrics.get(self.row_cardinality_index) else {
                return Err(history_mismatch("derived row-cardinality metric"));
            };
            let Some(expected_rows) = row_metric.subtotal.to_string().parse::<i64>().ok() else {
                // A valid but unprovable wide history remains on Dense.
                return Ok(());
            };
            if row_metric.eligibility
                != crate::ops::join::factorized::group_memo::MetricLimit::Exact
                || expected_rows < 0
            {
                return Ok(());
            }
            let mut metric_index = 0usize;
            let entry = groups.get(*group_key);
            for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
                let Some(metric) = summary.metrics.get(metric_index) else {
                    return Err(history_mismatch("derived metric shape"));
                };
                let value = match aggregate.func {
                    hotlap_core::plan::AggFunc::Count => {
                        metric_index += 1;
                        let expected = metric.subtotal.to_string().parse::<i64>().ok();
                        expected.map(AggValue::Count)
                    }
                    hotlap_core::plan::AggFunc::Sum => {
                        let sum = metric.subtotal.to_string().parse::<i128>().ok();
                        metric_index += 1;
                        let Some(nonnull) = summary.metrics.get(metric_index) else {
                            return Err(history_mismatch("derived SUM non-null metric"));
                        };
                        let count = nonnull.subtotal.to_string().parse::<i64>().ok();
                        metric_index += 1;
                        match (sum, count) {
                            (Some(sum), Some(count)) => Some(AggValue::SumInteger { sum, count }),
                            _ => None,
                        }
                    }
                    _ => return Ok(()),
                };
                let Some(value) = value else {
                    // A mathematically exact memo outside the authoritative
                    // slot's representable domain is ineligible, not proof.
                    return Ok(());
                };
                if let Some(entry) = entry
                    && entry.values.get(aggregate_index) != Some(&value)
                {
                    return Err(history_mismatch("integer aggregate slot"));
                }
            }
            if expected_rows > 0 {
                expected_present.insert(group_key.to_vec());
                let Some(entry) = entry else {
                    return Err(history_mismatch("missing source-derived group"));
                };
                if entry.rows != expected_rows {
                    return Err(history_mismatch("group row cardinality"));
                }
            } else if let Some(entry) = entry {
                if group_key.is_empty() && entry.rows == 0 {
                    // Global aggregation may retain its legitimate empty
                    // group even though no source row contributes.
                    expected_present.insert(group_key.to_vec());
                } else {
                    return Err(history_mismatch("extra zero-row group"));
                }
            }
        }
        if groups.iter().any(|(key, entry)| {
            !expected_present.contains(key) && !(key.is_empty() && entry.rows == 0)
        }) {
            return Err(history_mismatch(
                "extra group absent from canonical sources",
            ));
        }
        Ok(())
    }

    pub(super) fn known_capacity_bytes(&self) -> Result<usize, EngineError> {
        let mapping = &self.mapping;
        let mut bytes = std::mem::size_of::<Self>();
        bytes = bytes
            .checked_add(
                mapping
                    .left_join_key
                    .capacity()
                    .checked_mul(std::mem::size_of::<usize>())
                    .ok_or_else(memory_error)?,
            )
            .and_then(|v| {
                v.checked_add(
                    mapping
                        .right_join_key
                        .capacity()
                        .checked_mul(std::mem::size_of::<usize>())?,
                )
            })
            .and_then(|v| {
                v.checked_add(
                    mapping
                        .group_key
                        .capacity()
                        .checked_mul(std::mem::size_of::<usize>())?,
                )
            })
            .and_then(|v| {
                v.checked_add(
                    mapping
                        .aggregate_inputs
                        .capacity()
                        .checked_mul(std::mem::size_of::<Option<usize>>())?,
                )
            })
            .and_then(|v| {
                v.checked_add(
                    mapping
                        .join_key_types
                        .capacity()
                        .checked_mul(std::mem::size_of::<arrow::datatypes::DataType>())?,
                )
            })
            .and_then(|v| {
                v.checked_add(
                    mapping
                        .aggregate_input_types
                        .capacity()
                        .checked_mul(std::mem::size_of::<Option<arrow::datatypes::DataType>>())?,
                )
            })
            .ok_or_else(memory_error)?;
        for data_type in mapping
            .join_key_types
            .iter()
            .chain(mapping.aggregate_input_types.iter().flatten())
        {
            bytes = bytes
                .checked_add(crate::ops::join::data_type_allocated_bytes(data_type)?)
                .ok_or_else(memory_error)?;
        }
        bytes = bytes
            .checked_add(
                self.demands
                    .capacity()
                    .checked_mul(std::mem::size_of::<JoinMetricDemand>())
                    .ok_or_else(memory_error)?,
            )
            .ok_or_else(memory_error)?;
        // RowConverter owns Arrow internals with no safe public capacity report;
        // this is an explicit lower bound for allocations visible to this crate.
        bytes = bytes
            .checked_add(self.codec.known_capacity_bytes()?)
            .ok_or_else(memory_error)?;
        Ok(bytes)
    }

    pub(super) fn build(
        mapping: &ColumnMapping,
        aggregates: &[hotlap_core::plan::AggSpec],
        left: &KeyedArrangement,
        right: &KeyedArrangement,
    ) -> Result<Option<Self>, EngineError> {
        let left_raw_columns = raw_columns(mapping, aggregates, left.schema().fields().len(), true);
        let right_raw_columns =
            raw_columns(mapping, aggregates, left.schema().fields().len(), false);
        let left_columns = left_raw_columns
            .iter()
            .map(|column| column.index)
            .collect::<Vec<_>>();
        let right_columns = right_raw_columns
            .iter()
            .map(|column| column.index)
            .collect::<Vec<_>>();
        let Some(mut demands) = compile_metric_demands(
            mapping,
            aggregates,
            left.schema().fields().len(),
            &left_columns,
            &right_columns,
        ) else {
            return Ok(None);
        };
        let mut metric_index = 0usize;
        let mut row_cardinality_index = None;
        for aggregate in aggregates {
            if aggregate.func == hotlap_core::plan::AggFunc::Count && aggregate.input.is_none() {
                row_cardinality_index = Some(metric_index);
            }
            metric_index += match aggregate.func {
                hotlap_core::plan::AggFunc::Sum | hotlap_core::plan::AggFunc::Avg => 2,
                _ => 1,
            };
        }
        let row_cardinality_index = row_cardinality_index.unwrap_or_else(|| {
            let index = demands.len();
            demands.push(JoinMetricDemand {
                left: SourceMeasure::Weight,
                right: SourceMeasure::Weight,
                limit: arrow::datatypes::i256::from_i128(i64::MAX as i128),
            });
            index
        });
        let codec = GroupKeyCodec::new(&mapping.join_key_types, &mapping.group_key)?;
        let mut memo = GroupMemo::default();
        let mut keys = BTreeSet::new();
        keys.extend(left.iter_key_bytes().map(<[u8]>::to_vec));
        keys.extend(right.iter_key_bytes().map(<[u8]>::to_vec));
        for key in keys {
            let group = codec.encode(&key)?;
            let contribution = Contribution::from_optional_source_stats(
                group,
                left.source_stats(&key),
                right.source_stats(&key),
                &demands,
            )?;
            memo.replace(key, contribution)?;
        }
        Ok(Some(Self {
            mapping: mapping.clone(),
            codec,
            demands,
            row_cardinality_index,
            left_raw_columns,
            right_raw_columns,
            memo,
        }))
    }

    pub(super) fn stage(
        &self,
        updates: Vec<(Vec<u8>, Option<Contribution>)>,
    ) -> Result<GroupMemoStage, EngineError> {
        self.memo.stage_updates(updates)
    }

    pub(super) fn commit(&mut self, stage: GroupMemoStage) {
        self.memo.commit(stage);
    }
}

fn memory_error() -> EngineError {
    EngineError::Infrastructure("maintained group memo memory size overflow".into())
}

fn history_mismatch(detail: &str) -> EngineError {
    EngineError::Infrastructure(format!(
        "restored GroupAggregate integer history disagrees with canonical sources: {detail}"
    ))
}

fn raw_columns(
    mapping: &ColumnMapping,
    aggregates: &[hotlap_core::plan::AggSpec],
    left_width: usize,
    left_side: bool,
) -> Vec<RawColumnDemand> {
    let mut columns = Vec::<RawColumnDemand>::new();
    for (agg, input) in aggregates.iter().zip(&mapping.aggregate_inputs) {
        let Some(input) = input else { continue };
        if (left_side && *input < left_width || !left_side && *input >= left_width)
            && (agg.func == hotlap_core::plan::AggFunc::Count
                || agg.func == hotlap_core::plan::AggFunc::Sum
                || agg.func == hotlap_core::plan::AggFunc::Avg)
        {
            let index = if left_side {
                *input
            } else {
                *input - left_width
            };
            let sum = agg.func == hotlap_core::plan::AggFunc::Sum
                || agg.func == hotlap_core::plan::AggFunc::Avg;
            if let Some(existing) = columns.iter_mut().find(|column| column.index == index) {
                existing.sum |= sum;
            } else {
                columns.push(RawColumnDemand { index, sum });
            }
        }
    }
    columns.sort_unstable_by_key(|column| column.index);
    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use hotlap_core::ids::InputId;
    use hotlap_core::plan::AggSpec;
    use hotlap_core::plan_normalize::ColumnMapping;
    use std::sync::Arc;

    #[test]
    fn authoritative_group_aggregate_owns_one_initialized_source_memo() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mut left = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
        let mut right = KeyedArrangement::new(schema.clone(), &[0]).unwrap();
        left.register_source_stats(&[], &[], &[]).unwrap();
        right.register_source_stats(&[], &[], &[]).unwrap();
        let keys = crate::keys::KeyConverter::new(schema.as_ref(), &[0]).unwrap();
        let batch = crate::batch::ZSetBatch::new(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![7]))])
                .unwrap(),
            Arc::new(Int64Array::from(vec![2])),
        )
        .unwrap();
        left.apply(&batch, &keys).unwrap();
        right.apply(&batch, &keys).unwrap();
        let mapping = ColumnMapping {
            left_source: InputId(0),
            right_source: InputId(1),
            left_join_key: vec![0],
            right_join_key: vec![0],
            group_key: vec![0],
            aggregate_inputs: vec![None],
            join_key_types: vec![DataType::Int64],
            aggregate_input_types: vec![None],
        };
        let mut aggregate = crate::ops::GroupAggregate::new(&[0], vec![AggSpec::count()]);
        let before_capacity = aggregate.memory_usage().unwrap().capacity;
        assert!(
            aggregate
                .initialize_maintained_cut(&mapping, &left, &right)
                .unwrap()
        );
        let key = keys
            .convert(batch.batch.columns())
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec();
        let group = aggregate
            .maintained_cut
            .as_ref()
            .unwrap()
            .codec
            .encode(&key)
            .unwrap();
        assert_eq!(
            aggregate
                .maintained_memo()
                .unwrap()
                .group(&group)
                .unwrap()
                .metrics[0]
                .subtotal
                .to_string(),
            "4"
        );
        assert!(aggregate.memory_usage().unwrap().capacity > before_capacity);
        let left_stats = left.source_stats(&key).unwrap();
        let right_stats = right.source_stats(&key).unwrap();
        let replacement = Contribution::from_source_stats(
            group.clone(),
            left_stats,
            right_stats,
            &aggregate.maintained_cut.as_ref().unwrap().demands,
        )
        .unwrap();
        let staged = aggregate
            .stage_maintained_memo(vec![(key.clone(), Some(replacement))])
            .unwrap()
            .unwrap();
        assert_eq!(
            aggregate
                .maintained_memo()
                .unwrap()
                .group(&group)
                .unwrap()
                .metrics[0]
                .subtotal
                .to_string(),
            "4"
        );
        aggregate.commit_maintained_memo(staged);
        assert!(
            aggregate
                .initialize_maintained_cut(&mapping, &left, &right)
                .unwrap()
        );
        assert_eq!(aggregate.maintained_memo().unwrap().len(), 1);
    }
}
