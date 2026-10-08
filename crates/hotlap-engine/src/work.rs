//! Test-only instrumentation for `arrow::row` encoding work.
//!
//! Encoding runs on every push that materializes full rows (the incremental
//! view output and any `consolidate`). Counting encoded rows lets a test prove
//! a one-row push no longer re-encodes the whole resident state.

#[cfg(test)]
thread_local! {
    /// Per-thread count of rows encoded into `arrow::row` bytes.
    static ENCODED_ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Per-thread count of `RowConverter` constructions.
    static CONVERTER_BUILDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Records `rows` encoded rows (test only).
pub(crate) fn record(rows: usize) {
    #[cfg(test)]
    ENCODED_ROWS.with(|cell| cell.set(cell.get() + rows as u64));
    #[cfg(not(test))]
    let _ = rows;
}

/// Records one `RowConverter` construction (test only).
///
/// Converters are derived from the frozen schema, so a well-behaved operator
/// builds O(1) of them; counting proves they are cached, not rebuilt per push.
pub(crate) fn record_converter() {
    #[cfg(test)]
    CONVERTER_BUILDS.with(|cell| cell.set(cell.get() + 1));
}

/// Resets this thread's work counters (test only).
#[cfg(test)]
pub(crate) fn reset() {
    ENCODED_ROWS.with(|cell| cell.set(0));
    CONVERTER_BUILDS.with(|cell| cell.set(0));
}

/// Returns this thread's encoded-row counter (test only).
#[cfg(test)]
pub(crate) fn encoded_rows() -> u64 {
    ENCODED_ROWS.with(|cell| cell.get())
}

/// Returns this thread's `RowConverter` construction counter (test only).
#[cfg(test)]
pub(crate) fn converter_builds() -> u64 {
    CONVERTER_BUILDS.with(|cell| cell.get())
}
