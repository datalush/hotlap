//! A genuine pre-existing v4 snapshot must be rejected as foreign, not as a
//! decode failure, so recovery treats it as fatal rather than as corruption.
//!
//! The old serialized shapes are mirrored here solely to build a real v4
//! fixture; they are not a production compatibility reader.

use serde::Serialize;

use hotlap_engine::{
    ENGINE_SNAPSHOT_FORMAT_VERSION, EngineError, EngineSnapshot, InputId, InputSnapshot, Plan,
    ViewId, decode_framed, decode_snapshot, encode_framed,
};

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

/// The v4 float accumulators had no separate special-input multiplicities.
#[derive(Serialize)]
enum OldValue {
    Count(i64),
    SumInteger { sum: i128, count: i64 },
    SumFloat { sum: f64, count: i64 },
    Avg { sum: f64, count: i64 },
    Min(()),
    Max(()),
}

/// A non-empty v4 snapshot holding a group with a finite float sum.
fn old_snapshot() -> OldSnapshot {
    // Construct every variant so the mirrored enum layout stays complete; the
    // returned snapshot below only uses `Group` and `SumFloat`.
    let _operators = [OldOperator::Window(()), OldOperator::Join(())];
    let _values = [
        OldValue::Count(0),
        OldValue::SumInteger { sum: 0, count: 0 },
        OldValue::SumFloat { sum: 0.0, count: 0 },
        OldValue::Avg { sum: 0.0, count: 0 },
        OldValue::Min(()),
        OldValue::Max(()),
    ];
    OldSnapshot {
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
    }
}

#[test]
fn a_genuine_v4_snapshot_is_rejected_before_decoding_its_body() {
    let bytes = encode_framed(&old_snapshot()).unwrap();

    // The fixture is a real v4 body: the current float layout cannot read it.
    assert!(
        decode_framed::<EngineSnapshot>(&bytes).is_err(),
        "the v4 body must not decode as the current layout"
    );

    // The version discriminator rejects it as foreign, not as corruption.
    let error = decode_snapshot(&bytes).expect_err("v4 must be fatal");
    assert!(
        matches!(error, EngineError::Unsupported(_)),
        "expected Unsupported, got {error:?}"
    );
}

#[test]
fn a_future_snapshot_version_is_rejected() {
    let snapshot = EngineSnapshot {
        format_version: ENGINE_SNAPSHOT_FORMAT_VERSION + 1,
        epoch: 1,
        frozen: true,
        inputs: vec![],
        views: vec![],
    };
    let bytes = encode_framed(&snapshot).unwrap();
    assert!(matches!(
        decode_snapshot(&bytes),
        Err(EngineError::Unsupported(_))
    ));
}
