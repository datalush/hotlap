mod avg_certificate;
pub(crate) mod certificate;
pub(crate) mod certified_consume;
mod consume;
pub(crate) mod flat;
pub(crate) mod group_codec;
pub(crate) mod group_memo;
pub(crate) mod group_metrics;
mod groups;
mod history;
mod keyed;
mod prepare;
pub(crate) mod raw_bounds;
mod stage;
pub(super) use prepare::PreparedCut;
pub(crate) mod demand;
mod stats;

#[cfg(test)]
mod demand_tests;
#[cfg(test)]
mod group_memo_tests;

#[cfg(test)]
thread_local! {
    pub(crate) static FLAT_PAIR_PRODUCTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(crate) static OLD_HISTORY_ROWS_SCANNED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static OLD_HISTORY_SUMMARIZE_NS: std::cell::Cell<u128> = const { std::cell::Cell::new(0) };
    pub(crate) static OLD_HISTORY_SUMMARIZE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static PREPARE_STAGE_NS: std::cell::Cell<[u128; 7]> = const { std::cell::Cell::new([0; 7]) };
    pub(crate) static OLD_HISTORY_KEYS_VISITED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static OLD_HISTORY_DIRECT_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static OLD_HISTORY_KEYS_CANDIDATE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static OLD_BUCKET_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests;

#[allow(unused_imports, dead_code)]
pub(crate) use prepare::{
    ConsumerCaps, CutDecision, DeltaPair, DenseReason, GroupChange, OldState, PrepareInput, prepare,
};

pub(crate) use consume::consume;
