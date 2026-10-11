//! Process-local allocation accounting for isolated benchmark test binaries.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
pub struct Stats {
    pub live: usize,
    pub peak: usize,
}

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

pub struct TrackingAllocator;

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            add_live(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        remove_live(layout.size());
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, size) };
        if !resized.is_null() {
            if size >= layout.size() {
                add_live(size - layout.size());
            } else {
                remove_live(layout.size() - size);
            }
        }
        resized
    }
}

impl Stats {
    pub fn read() -> Self {
        Self {
            live: LIVE.load(Ordering::Relaxed),
            peak: PEAK.load(Ordering::Relaxed),
        }
    }
}

fn add_live(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

fn remove_live(bytes: usize) {
    LIVE.fetch_sub(bytes, Ordering::Relaxed);
}

#[allow(dead_code)]
pub fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}

pub fn capture_baseline(read_metadata: impl FnOnce() -> String) -> (String, usize) {
    let metadata = read_metadata();
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    (metadata, live)
}
