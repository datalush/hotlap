//! Binary snapshot frame: round-trip and corruption handling.

use hotlap_engine::{
    ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot, decode_snapshot, encode_framed, encode_snapshot,
};

fn empty_snapshot() -> EngineSnapshot {
    EngineSnapshot {
        format_version: ENGINE_SNAPSHOT_FORMAT_VERSION,
        epoch: 7,
        frozen: true,
        inputs: vec![],
        views: vec![],
    }
}

#[test]
fn snapshot_round_trips_through_a_binary_frame() {
    let snapshot = empty_snapshot();
    let bytes = encode_snapshot(&snapshot).unwrap();
    assert_eq!(&bytes[..4], b"HLSP", "frame must carry the codec magic");
    assert_eq!(decode_snapshot(&bytes).unwrap(), snapshot);
}

#[test]
fn previous_format_version_is_rejected_on_decode() {
    // Version 4 predates the separate finite-sum/special-count float layout.
    let mut snapshot = empty_snapshot();
    snapshot.format_version = 4;
    let bytes = encode_framed(&snapshot).unwrap();
    let err = decode_snapshot(&bytes).expect_err("the old layout must be rejected");
    assert!(
        matches!(err, hotlap_engine::EngineError::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

#[test]
fn unknown_format_version_is_rejected_on_encode() {
    let mut snapshot = empty_snapshot();
    snapshot.format_version = ENGINE_SNAPSHOT_FORMAT_VERSION + 1;
    assert!(encode_snapshot(&snapshot).is_err());
}

#[test]
fn corrupt_frames_are_errors_not_panics() {
    let bytes = encode_snapshot(&empty_snapshot()).unwrap();

    assert!(
        decode_snapshot(&[]).is_err(),
        "empty frame must be rejected"
    );
    assert!(
        decode_snapshot(&bytes[..bytes.len() - 1]).is_err(),
        "truncated frame must be rejected"
    );

    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xff;
    assert!(decode_snapshot(&bad_magic).is_err(), "bad magic must fail");

    let mut bad_version = bytes.clone();
    bad_version[4] = 0xff;
    assert!(
        decode_snapshot(&bad_version).is_err(),
        "unknown frame version must fail"
    );

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(
        decode_snapshot(&trailing).is_err(),
        "length mismatch must fail"
    );
}

#[test]
fn generic_values_use_the_same_frame() {
    let value: Vec<u32> = vec![1, 2, 3];
    let bytes = encode_framed(&value).unwrap();
    let decoded: Vec<u32> = hotlap_engine::decode_framed(&bytes).unwrap();
    assert_eq!(decoded, value);
}
