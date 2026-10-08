//! Multiset backing the retraction-aware `min`/`max` accumulators.
//!
//! Unlike `count`/`sum`/`avg`, `min`/`max` cannot be folded back out of a
//! running value: retracting the current extreme forces a recomputation. The
//! multiset keeps every non-null value with its multiplicity, so the extreme is
//! always the first (`min`) or last (`max`) live value in total order.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A non-null aggregate value ordered with a total order.
///
/// Integers are widened to `i128` and floats compare with [`f64::total_cmp`],
/// so the multiset stays totally ordered and `-0.0` stays distinct from `0.0`.
/// A multiset only ever holds one variant, but the cross-variant ordering is
/// defined to keep `Ord` total.
///
/// `NaN` is deliberately excluded from the multiset: it is not comparable to
/// any value, so `min`/`max` ignore `NaN` inputs and a group whose only
/// non-null values are `NaN` renders null. Use [`ExtremeValue::float`] to build
/// a float variant and enforce that invariant.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ExtremeValue {
    /// An integer value.
    Int(i128),
    /// A floating-point value; never `NaN`.
    Float(f64),
}

impl ExtremeValue {
    /// Builds a float extreme, dropping `NaN`.
    ///
    /// `NaN` carries no ordering, so letting it into the multiset would either
    /// hide the real extreme or emit a `NaN` result. Treating it like a null
    /// input keeps `min`/`max` deterministic and order-independent.
    pub fn float(value: f64) -> Option<Self> {
        if value.is_nan() {
            None
        } else {
            Some(ExtremeValue::Float(value))
        }
    }
}

impl PartialEq for ExtremeValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for ExtremeValue {}

impl PartialOrd for ExtremeValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExtremeValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (ExtremeValue::Int(a), ExtremeValue::Int(b)) => a.cmp(b),
            (ExtremeValue::Float(a), ExtremeValue::Float(b)) => a.total_cmp(b),
            (ExtremeValue::Int(_), ExtremeValue::Float(_)) => std::cmp::Ordering::Less,
            (ExtremeValue::Float(_), ExtremeValue::Int(_)) => std::cmp::Ordering::Greater,
        }
    }
}

/// A multiset of non-null values ordered by [`ExtremeValue`].
///
/// Each value carries a multiplicity so retracting one occurrence keeps the
/// value available until its last occurrence is removed.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OrderedMultiset {
    counts: BTreeMap<ExtremeValue, i64>,
}

impl OrderedMultiset {
    /// Adds `diff` occurrences of `value`, dropping values that reach zero.
    ///
    /// Retracting a value more often than it was inserted saturates at zero
    /// instead of going negative, keeping the multiset bounded by its inputs.
    pub fn add(&mut self, value: ExtremeValue, diff: i64) {
        let updated = self
            .counts
            .get(&value)
            .copied()
            .unwrap_or(0)
            .saturating_add(diff);
        if updated <= 0 {
            self.counts.remove(&value);
        } else {
            self.counts.insert(value, updated);
        }
    }

    /// The smallest retained value, or `None` when the multiset is empty.
    pub fn min(&self) -> Option<ExtremeValue> {
        self.counts.keys().next().copied()
    }

    /// The largest retained value, or `None` when the multiset is empty.
    pub fn max(&self) -> Option<ExtremeValue> {
        self.counts.keys().next_back().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_survive_until_the_last_occurrence() {
        let mut m = OrderedMultiset::default();
        m.add(ExtremeValue::Int(5), 2);
        m.add(ExtremeValue::Int(9), 1);
        assert_eq!(m.min(), Some(ExtremeValue::Int(5)));
        m.add(ExtremeValue::Int(5), -1);
        assert_eq!(m.min(), Some(ExtremeValue::Int(5)));
        m.add(ExtremeValue::Int(5), -1);
        assert_eq!(m.min(), Some(ExtremeValue::Int(9)));
        assert_eq!(m.max(), Some(ExtremeValue::Int(9)));
    }

    #[test]
    fn floats_order_by_total_cmp() {
        let mut m = OrderedMultiset::default();
        m.add(ExtremeValue::Float(1.5), 1);
        m.add(ExtremeValue::Float(-2.0), 1);
        m.add(ExtremeValue::Float(0.0), 1);
        assert_eq!(m.min(), Some(ExtremeValue::Float(-2.0)));
        assert_eq!(m.max(), Some(ExtremeValue::Float(1.5)));
    }

    #[test]
    fn float_constructor_drops_nan() {
        assert_eq!(ExtremeValue::float(f64::NAN), None);
        assert_eq!(ExtremeValue::float(1.5), Some(ExtremeValue::Float(1.5)));
    }

    #[test]
    fn signed_zero_stays_distinct_in_total_order() {
        // total_cmp keeps `-0.0` below `0.0`, unlike IEEE `==`.
        assert_eq!(
            ExtremeValue::Float(-0.0).cmp(&ExtremeValue::Float(0.0)),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn over_retraction_empties_the_multiset() {
        let mut m = OrderedMultiset::default();
        m.add(ExtremeValue::Int(1), 1);
        m.add(ExtremeValue::Int(1), -5);
        assert_eq!(m.min(), None);
    }
}
