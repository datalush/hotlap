//! Differential tests for views created after `START` from retained inputs.
//!
//! A view built after N pushes by replaying retained inputs must snapshot
//! exactly like the same view built before the first push, and must keep
//! updating with later pushes. Insufficient retention must be rejected.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, BinaryArray, DictionaryArray, Int64Array, UInt8Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt8Type};
use arrow::record_batch::RecordBatch;

use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};
use hotlap_engine::EngineCore;

fn schema(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

/// A two-column `(k, ts)` Z-set from `(k, ts, diff)` rows.
fn events(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema(&["k", "ts"]), columns).unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

fn foreign_binary_array(backing_len: usize, visible_len: usize) -> BinaryArray {
    let owner = Arc::new(vec![0u8; backing_len]);
    let ptr = std::ptr::NonNull::new(owner.as_ptr() as *mut u8).unwrap();
    // Safety: the Arc owner preserves this initialized allocation and pointer
    // for the full Arrow buffer lifetime; backing_len is its initialized size.
    let values = unsafe { Buffer::from_custom_allocation(ptr, backing_len, owner) }
        .slice_with_length(0, visible_len);
    assert_eq!(values.len(), visible_len);
    assert!(values.capacity() >= backing_len);
    let binary = ArrayData::builder(DataType::Binary)
        .len(1)
        .add_buffer(Buffer::from_vec(vec![0i32, visible_len as i32]))
        .add_buffer(values)
        .build()
        .unwrap();
    BinaryArray::from(binary)
}

fn owned_binary_event(backing_len: usize, visible_len: usize) -> ZSetBatch {
    let values = Buffer::from_vec(vec![b'x'; backing_len]);
    assert_eq!(values.len(), backing_len);
    assert_eq!(values.capacity(), backing_len);
    let values = values.slice_with_length(0, visible_len);
    assert_eq!(values.len(), visible_len);
    assert_eq!(values.capacity(), backing_len);
    let data = ArrayData::builder(DataType::Binary)
        .len(1)
        .add_buffer(Buffer::from_vec(vec![0i32, visible_len as i32]))
        .add_buffer(values)
        .build()
        .unwrap();
    assert_eq!(data.buffers()[1].capacity(), backing_len);
    let array: ArrayRef = Arc::new(BinaryArray::from(data));
    assert_eq!(array.to_data().buffers()[1].capacity(), backing_len);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1]))).unwrap()
}

/// Materializes a Z-set as `(row values, diff)` pairs for comparison.
fn rows(zset: &ZSetBatch) -> Vec<(Vec<i64>, i64)> {
    let mut out = Vec::new();
    for index in 0..zset.len() {
        let values = zset
            .batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index)
            })
            .collect();
        let diff = zset
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(index);
        out.push((values, diff));
    }
    out
}

fn core(retention: Option<usize>) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 1,
            lag: 0,
        },
    )
    .unwrap();
    if let Some(events) = retention {
        core.set_input_retention(events).unwrap();
    }
    core
}

fn group() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

fn window() -> Plan {
    Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        time_col: 1,
        size: 10,
    }
}

fn phase1(core: &mut EngineCore) {
    core.push(InputId(0), &events(&[(1, 0, 1), (2, 1, 1)]))
        .unwrap();
    core.push(InputId(0), &events(&[(1, 0, -1)])).unwrap();
    core.push(InputId(0), &events(&[(1, 2, 1), (3, 5, 1)]))
        .unwrap();
    // Close the windows starting at 0 and 10.
    core.push(InputId(0), &events(&[(9, 20, 1)])).unwrap();
}

fn phase2(core: &mut EngineCore) {
    core.push(InputId(0), &events(&[(2, 1, -1), (3, 3, 1)]))
        .unwrap();
    core.push(InputId(0), &events(&[(4, 25, 1)])).unwrap();
}

fn snapshots(a: &mut EngineCore, b: &mut EngineCore) {
    for view in 0..2 {
        assert_eq!(
            rows(&a.snapshot(ViewId(view)).unwrap()),
            rows(&b.snapshot(ViewId(view)).unwrap()),
            "late view {view} diverged from full recomputation"
        );
    }
}

#[test]
fn late_views_match_full_recomputation_and_keep_updating() {
    // Reference: group and window views built before the first push.
    let mut reference = core(None);
    reference.build_view(ViewId(0), &group()).unwrap();
    reference.build_view(ViewId(1), &window()).unwrap();

    // Dynamic: the same views built after the first phase, from retention.
    let mut dynamic = core(Some(64));
    phase1(&mut reference);
    phase1(&mut dynamic);
    dynamic.build_view(ViewId(0), &group()).unwrap();
    dynamic.build_view(ViewId(1), &window()).unwrap();
    snapshots(&mut reference, &mut dynamic);

    phase2(&mut reference);
    phase2(&mut dynamic);
    snapshots(&mut reference, &mut dynamic);
}

#[test]
fn late_view_without_retention_is_rejected() {
    let mut core = core(None);
    phase1(&mut core);
    assert!(core.build_view(ViewId(0), &group()).is_err());
}

#[test]
fn late_view_with_truncated_retention_is_rejected() {
    // Keep only two deltas, then push four: the oldest history is gone.
    let mut core = core(Some(2));
    phase1(&mut core);
    assert!(core.build_view(ViewId(0), &group()).is_err());
}

#[test]
fn byte_budget_prunes_late_replay_without_interrupting_live_ingest() {
    let mut core = core(None);
    core.set_input_retention_bytes(8, Some(0)).unwrap();
    core.build_view(ViewId(0), &group()).unwrap();
    core.push(InputId(0), &events(&[(1, 0, 1)])).unwrap();

    assert!(core.build_view(ViewId(1), &group()).is_err());
    core.push(InputId(0), &events(&[(1, 1, 1)])).unwrap();
    assert_eq!(
        rows(&core.snapshot(ViewId(0)).unwrap()),
        vec![(vec![1, 2], 1)]
    );
}

#[test]
fn byte_budget_prunes_at_independent_payload_boundary_and_keeps_live_view() {
    const EACH_VALUE_BYTES: usize = 1024;
    const BUDGET: usize = 2048;
    let batch = |fill| {
        let values = vec![fill; EACH_VALUE_BYTES];
        let data = ArrayData::builder(DataType::Binary)
            .len(1)
            .add_buffer(Buffer::from_vec(vec![0i32, EACH_VALUE_BYTES as i32]))
            .add_buffer(Buffer::from(values))
            .build()
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from(data))]).unwrap();
        ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1]))).unwrap()
    };
    let mut bounded = EngineCore::new();
    bounded.register_input(InputId(0)).unwrap();
    bounded.set_input_retention_bytes(8, Some(BUDGET)).unwrap();
    bounded.push(InputId(0), &batch(b'a')).unwrap();
    bounded
        .build_view(ViewId(0), &Plan::Source(InputId(0)))
        .unwrap();
    bounded.push(InputId(0), &batch(b'b')).unwrap();

    assert!(matches!(
        bounded.build_view(ViewId(1), &Plan::Source(InputId(0))),
        Err(hotlap_core::CoreError::Unsupported(message))
            if message.contains("insufficient input retention")
    ));
    assert_eq!(bounded.snapshot(ViewId(0)).unwrap().len(), 2);
}

#[test]
fn byte_budget_charges_owned_backing_after_array_wrapping_and_keeps_live_view() {
    const BACKING_LEN: usize = 8 * 1024 * 1024;
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.set_input_retention_bytes(8, Some(1024)).unwrap();
    core.push(InputId(0), &owned_binary_event(1, 1)).unwrap();
    core.build_view(ViewId(0), &Plan::Source(InputId(0)))
        .unwrap();
    core.push(InputId(0), &owned_binary_event(BACKING_LEN, 1))
        .unwrap();

    assert!(matches!(
        core.build_view(ViewId(1), &Plan::Source(InputId(0))),
        Err(hotlap_core::CoreError::Unsupported(message))
            if message.contains("insufficient input retention")
    ));
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(
        snapshot
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    core.push(InputId(0), &owned_binary_event(1, 1)).unwrap();
    assert_eq!(
        core.snapshot(ViewId(0))
            .unwrap()
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[test]
fn byte_budget_visits_dictionary_child_buffers() {
    let dictionary = DictionaryArray::<UInt8Type>::try_new(
        UInt8Array::from(vec![0]),
        Arc::new(foreign_binary_array(8 * 1024 * 1024, 1)),
    )
    .unwrap();
    let dtype = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Binary));
    let schema = Arc::new(Schema::new(vec![Field::new("payload", dtype, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(dictionary)]).unwrap();
    let event = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1]))).unwrap();
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.set_input_retention_bytes(8, Some(1024)).unwrap();

    core.push(InputId(0), &event).unwrap();

    assert!(matches!(
        core.build_view(ViewId(0), &Plan::Source(InputId(0))),
        Err(hotlap_core::CoreError::Unsupported(message))
            if message.contains("insufficient input retention")
    ));
}
