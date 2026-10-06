//! Inner equi-join compilation over the shared collections.
//!
//! Differential dataflow's `join_map` matches records by key and multiplies their
//! signed multiplicities, so retracting a row on either side retracts exactly the
//! joined tuples it had produced (repeated keys included). Each side is re-keyed to
//! `(key_row, payload_row)`; the payload is the whole row, so the output is the
//! left row's columns followed by the right row's, as [`Plan::Join`] documents.
//!
//! [`Plan::Join`]: crate::plan::Plan::Join

use differential_dataflow::VecCollection;

use crate::row::Row;

/// Re-key `row` to `(selected key columns, full row)`.
fn keyed(row: Row, key: &[usize]) -> (Row, Row) {
    let fields = Row(key.iter().map(|&col| row.col(col)).collect());
    (fields, row)
}

/// Concatenate the columns of `left` then those of `right`.
fn concat(left: &Row, right: &Row) -> Row {
    let mut out = left.0.clone();
    out.extend(right.0.iter().cloned());
    Row(out)
}

/// Equijoin `left` and `right` on their selected key columns.
pub(super) fn equijoin<'scope>(
    left: VecCollection<'scope, u64, Row, isize>,
    right: VecCollection<'scope, u64, Row, isize>,
    left_key: &[usize],
    right_key: &[usize],
) -> VecCollection<'scope, u64, Row, isize> {
    let left_key = left_key.to_vec();
    let right_key = right_key.to_vec();
    let left = left.map(move |row| keyed(row, &left_key));
    let right = right.map(move |row| keyed(row, &right_key));
    left.join_map(right, |_key, left, right| concat(left, right))
}
