//! Kernel restore guards on the snapshot's own identity fields.

use hotlap_core::snapshot::{InputSnapshot, ViewSnapshot};
use hotlap_core::{ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot, InputId, Plan, ViewId};
use hotlap_engine::EngineCore;

/// A snapshot that names the same view handle twice must be rejected before any
/// insert, so the kernel cannot validate one plan and restore another.
#[test]
fn restore_rejects_duplicate_view_ids() {
    let view = |plan: Plan| ViewSnapshot {
        id: ViewId(0),
        plan,
        windowed: false,
        tapped: false,
        output: None,
        pending: None,
        operators: vec![None],
    };
    let input = |id: u32| InputSnapshot {
        id: InputId(id),
        schema: None,
        spec: None,
        watermark: 0,
        splits: Vec::new(),
        late: 0,
    };
    let snapshot = EngineSnapshot {
        format_version: ENGINE_SNAPSHOT_FORMAT_VERSION,
        epoch: 0,
        frozen: false,
        inputs: vec![input(0), input(1)],
        views: vec![
            view(Plan::Source(InputId(0))),
            view(Plan::Source(InputId(1))),
        ],
    };
    let mut core = EngineCore::new();
    let result = core.restore(&snapshot);
    assert!(
        result.is_err(),
        "duplicate view handles must be rejected before any mutation: {result:?}"
    );
}
