//! Tests for the multi-source checkpoint container and its capture.

#[path = "cross_source_support/source.rs"]
mod source_support;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hotlap::InputId;
use hotlap_connectors::ConnectorError;
use hotlap_connectors::source::{Source, SourceState, Split};
use hotlap_engine::{decode_schema, encode_framed};
use hotlap_runtime::runtime::source_checkpoint::{
    SourcesCheckpoint, decode_sources, encode_sources,
};
use hotlap_runtime::runtime::sources::{InputSource, Sources};
use source_support::ControlledSource;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

fn split(id: i32) -> Split {
    Split { id, start: 0 }
}

fn input(id: u32, name: &str, source: Arc<ControlledSource>) -> InputSource {
    InputSource {
        id: InputId(id),
        name: name.to_string(),
        source,
        watermark: None,
    }
}

/// Two sources that both declare split 0 but hold different applied offsets.
fn two_split_zero_sources() -> (Sources, Arc<ControlledSource>, Arc<ControlledSource>) {
    let s = schema();
    let (a, _ta) = ControlledSource::new(s.clone(), vec![split(0)]);
    let (b, _tb) = ControlledSource::new(s, vec![split(0)]);
    a.commit(0, 5).unwrap();
    b.commit(0, 9).unwrap();
    assert_eq!(a.applied().offsets.get(&0), Some(&5));
    assert_eq!(b.commits(), vec![(0, 9)]);
    let sources = Sources::new(vec![input(1, "b", b.clone()), input(0, "a", a.clone())]).unwrap();
    (sources, a, b)
}

#[test]
fn capture_does_not_open_the_source_stream() {
    let bad = ControlledSource::failing_read(schema());
    let sources = Sources::new(vec![input(0, "bad", bad)]).unwrap();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    assert_eq!(checkpoint.entries[0].name, "bad");
}

#[test]
fn split_zero_states_roundtrip_independently() {
    let (sources, _a, _b) = two_split_zero_sources();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    assert_eq!(checkpoint.entries[0].id, InputId(0));
    assert_eq!(checkpoint.entries[0].state.offsets.get(&0), Some(&5));
    assert_eq!(checkpoint.entries[1].state.offsets.get(&0), Some(&9));

    let decoded = decode_sources(&encode_sources(&checkpoint).unwrap()).unwrap();
    assert_eq!(decoded.entries[0].state.offsets.get(&0), Some(&5));
    assert_eq!(decoded.entries[1].state.offsets.get(&0), Some(&9));
}

#[test]
fn capture_keeps_the_source_schema_metadata() {
    let (sources, _a, _b) = two_split_zero_sources();
    let checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    let decoded = decode_schema(&checkpoint.entries[0].schema).unwrap();
    assert_eq!(decoded, sources.get(InputId(0)).unwrap().source.schema());
}

#[test]
fn editing_one_entry_leaves_the_other_source_untouched() {
    let (sources, _a, _b) = two_split_zero_sources();
    let mut checkpoint = SourcesCheckpoint::capture(&sources).unwrap();
    checkpoint.entries[0].state.offsets.insert(0, 99);
    let decoded = decode_sources(&encode_sources(&checkpoint).unwrap()).unwrap();
    assert_eq!(decoded.entries[0].state.offsets.get(&0), Some(&99));
    assert_eq!(decoded.entries[1].state.offsets.get(&0), Some(&9));
}

#[test]
fn previous_payload_is_not_supported() {
    let old = encode_framed(&SourceState::default()).unwrap();
    assert!(matches!(
        decode_sources(&old),
        Err(ConnectorError::Unsupported(_))
    ));
}

#[test]
fn unknown_version_is_not_supported() {
    let empty = SourcesCheckpoint { entries: vec![] };
    let mut bytes = encode_sources(&empty).unwrap();
    bytes[4..8].copy_from_slice(&2u32.to_le_bytes());
    assert!(matches!(
        decode_sources(&bytes),
        Err(ConnectorError::Unsupported(_))
    ));
}

#[test]
fn unknown_inner_frame_version_is_not_supported() {
    let mut bytes = encode_sources(&SourcesCheckpoint { entries: vec![] }).unwrap();
    // Keep the valid `HLSR`/version-1 header and overwrite only the inner
    // engine frame's version, so the envelope is well-formed but incompatible.
    bytes[12..16].copy_from_slice(&9u32.to_le_bytes());
    assert!(matches!(
        decode_sources(&bytes),
        Err(ConnectorError::Unsupported(_))
    ));
}

#[test]
fn truncated_header_is_infrastructure() {
    let bytes = encode_sources(&SourcesCheckpoint { entries: vec![] }).unwrap();
    assert!(matches!(
        decode_sources(&bytes[..4]),
        Err(ConnectorError::Infrastructure(_))
    ));
    assert!(matches!(
        decode_sources(&bytes[..2]),
        Err(ConnectorError::Infrastructure(_))
    ));
}

#[test]
fn corrupt_payload_is_infrastructure() {
    let mut bytes = encode_sources(&SourcesCheckpoint { entries: vec![] }).unwrap();
    bytes[8] ^= 0xff;
    assert!(matches!(
        decode_sources(&bytes),
        Err(ConnectorError::Infrastructure(_))
    ));
}

#[test]
fn trailing_bytes_are_infrastructure() {
    let mut bytes = encode_sources(&SourcesCheckpoint { entries: vec![] }).unwrap();
    bytes.push(0);
    assert!(matches!(
        decode_sources(&bytes),
        Err(ConnectorError::Infrastructure(_))
    ));
}
