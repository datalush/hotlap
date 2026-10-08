//! Versioned binary container for [`SourcesCheckpoint`].
//!
//! The header is the exact `b"HLSR"` magic plus a little-endian `u32` layout
//! version. The payload is the engine's own framed encoding, so this module
//! never duplicates the Arrow/bincode codec and never tries an alternative
//! payload layout.

use hotlap_connectors::error::ConnectorError;
use hotlap_engine::{decode_framed, encode_framed};

use super::{SourcesCheckpoint, codec_err};

/// Magic bytes at the start of every sources checkpoint.
const MAGIC: [u8; 4] = *b"HLSR";
/// Layout version of the sources checkpoint container.
const VERSION: u32 = 1;
/// Fixed header length: magic (4) plus version (4).
const HEADER: usize = 8;

/// Encode `value` as `HLSR` + version + the engine's framed payload.
pub fn encode_sources(value: &SourcesCheckpoint) -> Result<Vec<u8>, ConnectorError> {
    let payload = encode_framed(value).map_err(codec_err)?;
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a sources checkpoint, rejecting foreign magic, unknown versions and
/// corruption. A different magic or version is `Unsupported`; a truncated
/// header or a corrupt payload is `Infrastructure`.
pub fn decode_sources(bytes: &[u8]) -> Result<SourcesCheckpoint, ConnectorError> {
    match bytes.get(..MAGIC.len()) {
        None => Err(truncated(bytes.len())),
        Some(prefix) if prefix != MAGIC.as_slice() => Err(ConnectorError::Unsupported(
            "sources checkpoint magic does not match".into(),
        )),
        Some(_) => decode_body(bytes),
    }
}

/// Read the version word and decode the framed payload after a valid magic.
fn decode_body(bytes: &[u8]) -> Result<SourcesCheckpoint, ConnectorError> {
    if bytes.len() < HEADER {
        return Err(truncated(bytes.len()));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap_or_default());
    if version != VERSION {
        return Err(ConnectorError::Unsupported(format!(
            "unknown sources checkpoint version {version}"
        )));
    }
    decode_framed(&bytes[HEADER..]).map_err(codec_err)
}

/// Build an infrastructure error for a truncated container header.
fn truncated(len: usize) -> ConnectorError {
    ConnectorError::Infrastructure(format!("sources checkpoint header is {len} bytes"))
}
