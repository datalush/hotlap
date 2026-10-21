//! Deterministic multiview qualification for optional input-history byte quotas.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

const BATCHES: usize = 64;
const ROWS_PER_BATCH: usize = 128;
const SMALL_BYTES: usize = 32 * 1024;
const LARGE_BYTES: usize = 16 * 1024 * 1024;

fn input_batch(batch_index: usize) -> ZSetBatch {
    let keys = (0..ROWS_PER_BATCH)
        .map(|row| (row % 8) as i64)
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys))]).unwrap();
    assert!(batch_index < BATCHES);
    ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1; ROWS_PER_BATCH]))).unwrap()
}

fn grouped_count() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

fn assert_count_oracle(core: &mut EngineCore, view: ViewId) {
    let result = core.snapshot(view).unwrap();
    let keys = result
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = result
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = result.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    let actual = (0..result.len())
        .map(|row| (keys.value(row), counts.value(row), diffs.value(row)))
        .collect::<Vec<_>>();
    let expected = (0..8).map(|key| (key, 1024, 1)).collect::<Vec<_>>();
    assert_eq!(actual, expected, "independent COUNT oracle for {view:?}");
}

#[test]
fn byte_retention_multiview_count_qualification() {
    for views in [1usize, 2, 8] {
        for (mode, byte_cap) in [
            ("none", None),
            ("small", Some(SMALL_BYTES)),
            ("large", Some(LARGE_BYTES)),
        ] {
            let mut core = EngineCore::new();
            core.register_input(InputId(0)).unwrap();
            core.set_input_retention_bytes(BATCHES, byte_cap).unwrap();
            let plan = grouped_count();
            for index in 0..views {
                core.build_view(ViewId(index as u32), &plan).unwrap();
            }
            let mut push_wall_ns_total = 0u128;
            for batch_index in 0..BATCHES {
                let batch = input_batch(batch_index);
                let started = Instant::now();
                core.push(InputId(0), &batch).unwrap();
                push_wall_ns_total += started.elapsed().as_nanos();
            }
            for index in 0..views {
                assert_count_oracle(&mut core, ViewId(index as u32));
            }

            let report = core.memory_report().unwrap();
            // MemoryReport is global and deliberately does not expose a
            // retention-only subtotal; use its total as an observation, not
            // as a false claim about history charge.
            let global_arrow_capacity = report.arrow_buffer_capacity_bytes;
            let replay = core.build_view(ViewId(100), &plan);
            if mode == "small" {
                assert!(
                    matches!(replay, Err(hotlap_core::CoreError::Unsupported(message)) if message.contains("insufficient input retention"))
                );
            } else {
                replay.unwrap();
                assert_count_oracle(&mut core, ViewId(100));
            }
            let late_view = if mode == "small" {
                "rejected_insufficient_history"
            } else {
                "replayed"
            };
            println!(
                "{}",
                serde_json::json!({
                    "record":"retention_multiview_qualification",
                    "mode":mode,
                    "active_views":views,
                    "input_batches":BATCHES,
                    "rows":BATCHES * ROWS_PER_BATCH,
                    "push_count":BATCHES,
                    "push_wall_ns_total":push_wall_ns_total,
                    "configured_byte_cap":byte_cap,
                    "global_arrow_buffer_capacity_bytes":global_arrow_capacity,
                    "history_charge":"not separately exposed by public MemoryReport; quota outcome checked by replay",
                    "global_known_backing_capacity_bytes":report.known_backing_capacity_bytes,
                    "global_logical_payload_bytes":report.logical_payload_bytes,
                    "per_view_known_components":"not separately exposed by public MemoryReport",
                    "retained_event_count":"not exposed by public API",
                    "exact_heap_bytes":report.exact_heap_bytes,
                    "late_view":late_view,
                    "count_oracle":"passed"
                })
            );
        }
    }
}
