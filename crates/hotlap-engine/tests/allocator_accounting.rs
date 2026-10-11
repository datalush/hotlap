#[path = "support/baseline_alloc.rs"]
mod baseline_alloc;

use std::alloc::{Layout, alloc, dealloc, realloc};

use baseline_alloc::Stats;

#[test]
fn benchmark_allocator_accounts_live_bytes_and_peak_reset() {
    let baseline = Stats::read();
    let layout = Layout::from_size_align(4_096, 8).unwrap();
    let pointer = unsafe { alloc(layout) };
    assert!(!pointer.is_null());

    let allocated = Stats::read();
    assert_eq!(allocated.live - baseline.live, layout.size());
    assert!(allocated.peak >= allocated.live);

    baseline_alloc::reset_peak();
    assert_eq!(Stats::read().live, allocated.live);
    assert_eq!(Stats::read().peak, allocated.live);

    let grown = unsafe { realloc(pointer, layout, 8_192) };
    assert!(!grown.is_null());
    let reallocated = Stats::read();
    assert_eq!(reallocated.live - baseline.live, 8_192);
    assert_eq!(reallocated.peak, reallocated.live);

    let shrunk = unsafe { realloc(grown, Layout::from_size_align(8_192, 8).unwrap(), 2_048) };
    assert!(!shrunk.is_null());
    assert_eq!(Stats::read().live - baseline.live, 2_048);
    unsafe { dealloc(shrunk, Layout::from_size_align(2_048, 8).unwrap()) };
    assert_eq!(Stats::read().live, baseline.live);
}
