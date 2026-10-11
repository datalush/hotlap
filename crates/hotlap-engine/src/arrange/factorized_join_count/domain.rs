#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NotEligible {
    Schema,
    Encoding,
    RowBound,
    NegativeState,
    NumericBound,
}

pub(super) fn count_bound(a: u128, b: u128, u: u128, v: u128) -> Option<u128> {
    let bound = a
        .checked_mul(b)?
        .checked_add(u.checked_mul(b)?)?
        .checked_add(a.checked_mul(v)?)?
        .checked_add(u.checked_mul(v)?)?;
    (bound <= i64::MAX as u128).then_some(bound)
}

pub(super) fn row_after(old: i64, net: i128, absolute: u128) -> Option<i64> {
    if old < 0 || (old as u128).checked_add(absolute)? > i64::MAX as u128 {
        return None;
    }
    let next = (old as i128).checked_add(net)?;
    i64::try_from(next).ok().filter(|value| *value >= 0)
}

pub(super) fn side_after(old: u128, net: i128) -> Option<u128> {
    if net >= 0 {
        old.checked_add(net as u128)
    } else {
        old.checked_sub(net.unsigned_abs())
    }
}

#[test]
fn checked_domain_preserves_large_unmatched_sides() {
    assert_eq!(count_bound(3, 4, 1, 2), Some(24));
    assert_eq!(count_bound(u128::MAX, 0, 0, 0), Some(0));
    assert_eq!(count_bound(i64::MAX as u128, 1, 1, 0), None);
    assert_eq!(count_bound(u128::MAX, 2, 0, 0), None);
}

#[test]
fn row_gate_does_not_hide_cancelled_or_negative_deltas() {
    assert_eq!(row_after(5, -2, 2), Some(3));
    assert_eq!(row_after(5, -6, 6), None);
    assert_eq!(row_after(i64::MAX, 0, 2), None);
    assert_eq!(
        row_after(0, i64::MIN as i128, (i64::MIN as i128).unsigned_abs()),
        None
    );
    assert_eq!(side_after(7, -3), Some(4));
    assert_eq!(side_after(0, -1), None);
}
