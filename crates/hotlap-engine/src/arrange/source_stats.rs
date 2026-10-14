use arrow::datatypes::i256;

use crate::error::EngineError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceColumnDemand {
    pub(crate) index: usize,
    pub(crate) sum: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourceColumnValue {
    pub(crate) is_null: bool,
    pub(crate) integer: Option<i128>,
    pub(crate) sum: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SourceColumnStats {
    pub(crate) nonnull: i128,
    pub(crate) sum: Option<i256>,
    pub(crate) abs_sum: Option<i256>,
}

/// Exact physical facts for one canonical arrangement key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SourceStats {
    pub(crate) weight: i128,
    pub(crate) abs_weight: u128,
    pub(crate) support: usize,
    pub(crate) negative_support: usize,
    pub(crate) columns: Vec<SourceColumnStats>,
}

impl SourceStats {
    /// Replaces one row's old weight with its new weight using the immutable
    /// values decoded from that canonical row (one value per demanded column).
    pub(crate) fn stage_weight_change(
        &self,
        old: i64,
        new: i64,
        values: &[SourceColumnValue],
    ) -> Result<Self, EngineError> {
        let delta = i128::from(new) - i128::from(old);
        let mut next = SourceStats {
            weight: self.weight,
            abs_weight: self.abs_weight,
            support: self.support,
            negative_support: self.negative_support,
            columns: self.columns.clone(),
        };
        next.weight = next
            .weight
            .checked_add(delta)
            .ok_or(EngineError::NumericOverflow("source row weight stats"))?;
        next.abs_weight = next
            .abs_weight
            .checked_sub(u128::from(old.unsigned_abs()))
            .and_then(|weight| weight.checked_add(u128::from(new.unsigned_abs())))
            .ok_or(EngineError::NumericOverflow("source absolute weight stats"))?;
        update_support(
            &mut next.support,
            old == 0,
            new == 0,
            "source support stats",
        )?;
        update_support(
            &mut next.negative_support,
            old >= 0,
            new >= 0,
            "source negative support stats",
        )?;
        if next.columns.is_empty() {
            next.columns = values
                .iter()
                .map(|value| SourceColumnStats {
                    sum: value.sum.then_some(i256::ZERO),
                    abs_sum: value.sum.then_some(i256::ZERO),
                    ..SourceColumnStats::default()
                })
                .collect();
        }
        if next.columns.len() != values.len() {
            return Err(EngineError::Unsupported(
                "source stats demand changed after registration".into(),
            ));
        }
        for (column, value) in next.columns.iter_mut().zip(values) {
            if column.sum.is_some() != value.sum || column.abs_sum.is_some() != value.sum {
                return Err(EngineError::Unsupported(
                    "source stats metric changed after registration".into(),
                ));
            }
            if value.is_null {
                continue;
            }
            column.nonnull = column
                .nonnull
                .checked_add(delta)
                .ok_or(EngineError::NumericOverflow("source nonnull stats"))?;
            if value.sum {
                let integer_value = value.integer.ok_or_else(|| {
                    EngineError::Unsupported("integer SUM demand has a non-integer value".into())
                })?;
                let integer = i256::from_i128(integer_value);
                let signed_delta = integer
                    .checked_mul(i256::from_i128(delta))
                    .ok_or(EngineError::NumericOverflow("source sum stats"))?;
                column.sum = Some(
                    column
                        .sum
                        .expect("sum-enabled columns initialize a sum")
                        .checked_add(signed_delta)
                        .ok_or(EngineError::NumericOverflow("source sum stats"))?,
                );
                let magnitude = if integer < i256::ZERO {
                    i256::ZERO - integer
                } else {
                    integer
                };
                let new_abs = wide_unsigned(u128::from(new.unsigned_abs()))?;
                let old_abs = wide_unsigned(u128::from(old.unsigned_abs()))?;
                let positive = magnitude
                    .checked_mul(new_abs)
                    .ok_or(EngineError::NumericOverflow("source absolute sum stats"))?;
                let negative = magnitude
                    .checked_mul(old_abs)
                    .ok_or(EngineError::NumericOverflow("source absolute sum stats"))?;
                let abs_delta = positive
                    .checked_sub(negative)
                    .ok_or(EngineError::NumericOverflow("source absolute sum stats"))?;
                column.abs_sum = Some(
                    column
                        .abs_sum
                        .expect("sum-enabled columns initialize an absolute sum")
                        .checked_add(abs_delta)
                        .ok_or(EngineError::NumericOverflow("source absolute sum stats"))?,
                );
            }
        }
        Ok(next)
    }
}

fn update_support(
    value: &mut usize,
    old_predicate: bool,
    new_predicate: bool,
    context: &'static str,
) -> Result<(), EngineError> {
    match (old_predicate, new_predicate) {
        (true, false) => {
            *value = value
                .checked_add(1)
                .ok_or(EngineError::NumericOverflow(context))?;
        }
        (false, true) => {
            *value = value
                .checked_sub(1)
                .ok_or(EngineError::NumericOverflow(context))?;
        }
        _ => {}
    }
    Ok(())
}

fn wide_unsigned(value: u128) -> Result<i256, EngineError> {
    i256::from_string(&value.to_string()).ok_or(EngineError::NumericOverflow(
        "source unsigned wide conversion",
    ))
}
