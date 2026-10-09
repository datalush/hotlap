//! A genuine v4 engine-snapshot fixture for recovery format tests.
//!
//! The old serialized shapes are mirrored only to build real v4 bytes; this is
//! not a compatibility reader and is never used in production.

use serde::Serialize;

use hotlap_engine::{InputId, InputSnapshot, Plan, ViewId, encode_framed};

#[derive(Serialize)]
struct OldSnapshot {
    format_version: u32,
    epoch: u64,
    frozen: bool,
    inputs: Vec<InputSnapshot>,
    views: Vec<OldView>,
}

#[derive(Serialize)]
struct OldView {
    id: ViewId,
    plan: Plan,
    windowed: bool,
    tapped: bool,
    output: Option<()>,
    pending: Option<()>,
    operators: Vec<Option<OldOperator>>,
}

#[derive(Serialize)]
enum OldOperator {
    Group(OldGroup),
    Window(()),
    Join(()),
}

#[derive(Serialize)]
struct OldGroup {
    schema: Option<Vec<u8>>,
    groups: Vec<(Vec<u8>, OldEntry)>,
}

#[derive(Serialize)]
struct OldEntry {
    rows: i64,
    values: Vec<OldValue>,
}

#[derive(Serialize)]
enum OldValue {
    Count(i64),
    SumInteger { sum: i128, count: i64 },
    SumFloat { sum: f64, count: i64 },
    Avg { sum: f64, count: i64 },
    Min(()),
    Max(()),
}

/// Bytes of a genuine non-empty v4 snapshot holding a finite float sum.
pub fn old_v4_snapshot_bytes() -> Vec<u8> {
    // Construct every variant so the mirrored enum layout stays complete; the
    // serialized `snapshot` below only uses `Group` and `SumFloat`.
    let _operators = [OldOperator::Window(()), OldOperator::Join(())];
    let _values = [
        OldValue::Count(0),
        OldValue::SumInteger { sum: 0, count: 0 },
        OldValue::SumFloat { sum: 0.0, count: 0 },
        OldValue::Avg { sum: 0.0, count: 0 },
        OldValue::Min(()),
        OldValue::Max(()),
    ];
    let snapshot = OldSnapshot {
        format_version: 4,
        epoch: 9,
        frozen: true,
        inputs: Vec::new(),
        views: vec![OldView {
            id: ViewId(0),
            plan: Plan::Source(InputId(0)),
            windowed: false,
            tapped: false,
            output: None,
            pending: None,
            operators: vec![Some(OldOperator::Group(OldGroup {
                schema: None,
                groups: vec![(
                    vec![1, 2, 3],
                    OldEntry {
                        rows: 1,
                        values: vec![OldValue::SumFloat { sum: 2.0, count: 1 }],
                    },
                )],
            }))],
        }],
    };
    encode_framed(&snapshot).unwrap()
}
