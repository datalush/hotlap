//! Unknown snapshot versions are rejected as foreign, not as corruption.

use hotlap_engine::{
    ENGINE_SNAPSHOT_FORMAT_VERSION, EngineError, EngineSnapshot, decode_snapshot, encode_framed,
};

#[test]
fn an_unknown_snapshot_version_is_rejected_before_decoding_its_body() {
    let snapshot = EngineSnapshot {
        format_version: ENGINE_SNAPSHOT_FORMAT_VERSION + 1,
        epoch: 9,
        frozen: true,
        inputs: Vec::new(),
        views: Vec::new(),
    };
    let bytes = encode_framed(&snapshot).unwrap();
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
