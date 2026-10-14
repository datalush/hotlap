//! Compile selected aggregate slots and derive per-key metrics from borrowed
//! canonical source statistics, without materializing joined rows.

use arrow::datatypes::i256;
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::plan_normalize::ColumnMapping;

use crate::arrange::source_stats::SourceStats;
use crate::error::EngineError;

use super::group_memo::MetricContribution;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceMeasure {
    Weight,
    NonNull(usize),
    Sum(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JoinMetricDemand {
    pub(crate) left: SourceMeasure,
    pub(crate) right: SourceMeasure,
    pub(crate) limit: i256,
}

/// Compile metrics in aggregate order. SUM expands to SUM plus its required
/// non-null count, even when COUNT(expr) is not selected independently.
pub(crate) fn compile_metric_demands(
    mapping: &ColumnMapping,
    aggregates: &[AggSpec],
    left_width: usize,
    left_columns: &[usize],
    right_columns: &[usize],
) -> Option<Vec<JoinMetricDemand>> {
    if mapping.aggregate_inputs.len() != aggregates.len() {
        return None;
    }
    let count_limit = i256::from_i128(i64::MAX as i128);
    let sum_limit = i256::from_i128(i128::MAX);
    let mut result = Vec::new();
    for (agg, input) in aggregates.iter().zip(&mapping.aggregate_inputs) {
        match (agg.func, input) {
            (AggFunc::Count, None) => result.push(JoinMetricDemand {
                left: SourceMeasure::Weight,
                right: SourceMeasure::Weight,
                limit: count_limit,
            }),
            (AggFunc::Count, Some(column)) => {
                let column = *column;
                if column < left_width {
                    let slot = left_columns.binary_search(&column).ok()?;
                    result.push(JoinMetricDemand {
                        left: SourceMeasure::NonNull(slot),
                        right: SourceMeasure::Weight,
                        limit: count_limit,
                    });
                } else {
                    let slot = right_columns.binary_search(&(column - left_width)).ok()?;
                    result.push(JoinMetricDemand {
                        left: SourceMeasure::Weight,
                        right: SourceMeasure::NonNull(slot),
                        limit: count_limit,
                    });
                }
            }
            (AggFunc::Sum, Some(column)) => {
                let column = *column;
                if column < left_width {
                    let slot = left_columns.binary_search(&column).ok()?;
                    result.push(JoinMetricDemand {
                        left: SourceMeasure::Sum(slot),
                        right: SourceMeasure::Weight,
                        limit: sum_limit,
                    });
                    result.push(JoinMetricDemand {
                        left: SourceMeasure::NonNull(slot),
                        right: SourceMeasure::Weight,
                        limit: count_limit,
                    });
                } else {
                    let slot = right_columns.binary_search(&(column - left_width)).ok()?;
                    result.push(JoinMetricDemand {
                        left: SourceMeasure::Weight,
                        right: SourceMeasure::Sum(slot),
                        limit: sum_limit,
                    });
                    result.push(JoinMetricDemand {
                        left: SourceMeasure::Weight,
                        right: SourceMeasure::NonNull(slot),
                        limit: count_limit,
                    });
                }
            }
            _ => return None,
        }
    }
    Some(result)
}

/// Construct one contribution vector for a changed canonical full join key.
/// The source stats are borrowed directly; payload rows are not decoded here.
pub(crate) fn contribution_metrics(
    left: Option<&SourceStats>,
    right: Option<&SourceStats>,
    demands: &[JoinMetricDemand],
) -> Result<Vec<MetricContribution>, EngineError> {
    demands
        .iter()
        .map(|demand| {
            let lhs = measure(left, demand.left)?;
            let rhs = measure(right, demand.right)?;
            MetricContribution::bounded_product(lhs, rhs, demand.limit)
        })
        .collect()
}

fn measure(stats: Option<&SourceStats>, measure: SourceMeasure) -> Result<i256, EngineError> {
    let Some(stats) = stats else {
        return Ok(i256::ZERO);
    };
    match measure {
        SourceMeasure::Weight => Ok(i256::from_i128(stats.weight)),
        SourceMeasure::NonNull(index) => Ok(i256::from_i128(
            stats
                .columns
                .get(index)
                .ok_or_else(|| {
                    EngineError::Infrastructure("source nonnull demand index missing".into())
                })?
                .nonnull,
        )),
        SourceMeasure::Sum(index) => stats
            .columns
            .get(index)
            .ok_or_else(|| EngineError::Infrastructure("source sum demand index missing".into()))?
            .sum
            .ok_or_else(|| EngineError::Infrastructure("source SUM stat not registered".into())),
    }
}
