//! Single-sample child-process checkpoint cost probe; intentionally ignored.

#[path = "typed_key_cost/fixture.rs"]
mod fixture;
use fixture::{KEYS, configured, seed_batches};

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::ipc::reader::StreamReader;
use hotlap_core::snapshot::{EngineSnapshot, OperatorState, TypedKeyPayload};
use hotlap_core::{IncrementalCore, InputId, ZSetBatch};

fn typed_payload_metrics(snapshot: &EngineSnapshot) -> (usize, usize) {
    let mut bytes = 0;
    let mut batches = 0;
    for state in snapshot
        .views
        .iter()
        .flat_map(|view| view.operators.iter().flatten())
    {
        let payload = match state {
            OperatorState::Group(state) => &state.typed_keys,
            OperatorState::Window(state) => &state.typed_keys,
            OperatorState::Join(_) => continue,
        };
        let TypedKeyPayload::Ipc(stream) = payload else {
            continue;
        };
        bytes += stream.len();
        let mut reader = StreamReader::try_new(std::io::Cursor::new(stream), None).unwrap();
        for batch in reader.by_ref() {
            batch.unwrap();
            batches += 1;
        }
    }
    (bytes, batches)
}

#[test]
#[ignore = "bounded release checkpoint cost child; run independently per case/trial"]
fn checkpoint_cost_child() {
    let case = std::env::var("TYPED_KEY_COST_CASE").expect("set TYPED_KEY_COST_CASE");
    let (mut source, batch, windowed) = configured(&case);
    for batch in seed_batches(&case, &batch) {
        let rows = batch.num_rows();
        source
            .push(
                InputId(0),
                &ZSetBatch::new(
                    batch,
                    Arc::new(Int64Array::from(vec![1_i64; rows])) as ArrayRef,
                )
                .unwrap(),
            )
            .unwrap();
    }

    #[cfg(feature = "bench-allocation-observer")]
    let export_region = stats_alloc::Region::new(super::TEST_ALLOCATOR);
    let export_started = Instant::now();
    let snapshot = source.checkpoint().unwrap();
    let framed = crate::core::ipc::encode_framed(&snapshot).unwrap();
    let export_ns = export_started.elapsed().as_nanos();
    #[cfg(feature = "bench-allocation-observer")]
    let export_alloc = export_region.change();

    let (mut restored, _, _) = configured(&case);
    #[cfg(feature = "bench-allocation-observer")]
    let restore_region = stats_alloc::Region::new(super::TEST_ALLOCATOR);
    let restore_started = Instant::now();
    let decoded: EngineSnapshot = crate::core::ipc::decode_framed(&framed).unwrap();
    restored.restore(&decoded).unwrap();
    let restore_ns = restore_started.elapsed().as_nanos();
    #[cfg(feature = "bench-allocation-observer")]
    let restore_alloc = restore_region.change();
    assert_eq!(restored.checkpoint().unwrap(), snapshot);
    let (typed_ipc_bytes, typed_ipc_record_batches) = typed_payload_metrics(&snapshot);

    #[cfg(feature = "bench-allocation-observer")]
    let allocator = serde_json::json!({
        "scope": "process-wide stats_alloc region; allocations/bytes include unrelated activity and are traffic, not retained bytes",
        "export_allocations": export_alloc.allocations,
        "export_bytes_allocated": export_alloc.bytes_allocated,
        "export_deallocations": export_alloc.deallocations,
        "export_bytes_deallocated": export_alloc.bytes_deallocated,
        "export_reallocations": export_alloc.reallocations,
        "export_bytes_reallocated": export_alloc.bytes_reallocated,
        "restore_allocations": restore_alloc.allocations,
        "restore_bytes_allocated": restore_alloc.bytes_allocated,
        "restore_deallocations": restore_alloc.deallocations,
        "restore_bytes_deallocated": restore_alloc.bytes_deallocated,
        "restore_reallocations": restore_alloc.reallocations,
        "restore_bytes_reallocated": restore_alloc.bytes_reallocated,
    });
    #[cfg(not(feature = "bench-allocation-observer"))]
    let allocator = serde_json::Value::Null;

    println!(
        "TYPED_KEY_COST_JSON {}",
        serde_json::json!({
            "case": case,
            "keys": KEYS,
            "distinct_keys": KEYS,
            "windowed": windowed,
            "export_ns": export_ns,
            "restore_ns": restore_ns,
            "serialized_checkpoint_bytes": framed.len(),
            "typed_ipc_bytes": typed_ipc_bytes,
            "typed_ipc_record_batches": typed_ipc_record_batches,
            "allocator_traffic": allocator,
            "timed_units": "Instant wall-clock nanoseconds; export includes checkpoint plus framed serialization; restore includes frame decode plus EngineCore restore",
            "rss_or_heap_claim": false
        })
    );
}
