//! Key <-> on-disk path codec for the durable backend.
//!
//! Keys are namespace paths separated by `/`. Each segment is hex-encoded so
//! arbitrary key bytes stay file-name safe, and each segment becomes one
//! directory level. A key's namespace prefix therefore maps to a subdirectory,
//! which keeps `scan`/`list` bounded to the relevant subtree.

use super::StateError;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// A parsed scan prefix: complete leading segments plus a trailing partial one.
pub(super) struct Prefix<'a> {
    /// Fully specified leading segments (directories to descend into).
    pub dirs: Vec<&'a [u8]>,
    /// Trailing segment; any stored segment starting with it matches. Empty
    /// means "everything under `dirs`".
    pub partial: &'a [u8],
}

/// Reject the empty key and keys with an empty segment (`a//b`, `/a`, `a/`).
pub(super) fn validate_key(key: &[u8]) -> Result<(), StateError> {
    if key.is_empty() {
        return Err(StateError::EmptyKey);
    }
    if key.split(|b| *b == b'/').any(<[u8]>::is_empty) {
        return Err(StateError::InvalidKey);
    }
    Ok(())
}

/// Split a scan prefix into complete directory segments plus a partial tail.
///
/// An empty prefix means "the whole store"; an empty directory segment makes
/// the prefix unrepresentable on disk, signalled as `None`.
pub(super) fn split_prefix(prefix: &[u8]) -> Option<Prefix<'_>> {
    if prefix.is_empty() {
        return Some(Prefix {
            dirs: Vec::new(),
            partial: &[],
        });
    }
    let mut parts: Vec<&[u8]> = prefix.split(|b| *b == b'/').collect();
    let partial = parts.pop()?;
    if parts.iter().any(|segment| segment.is_empty()) {
        return None;
    }
    Some(Prefix {
        dirs: parts,
        partial,
    })
}

/// Rebuild a key by joining decoded segments with `/`.
pub(super) fn join_segments(segments: &[Vec<u8>]) -> Vec<u8> {
    let mut key = Vec::new();
    for (i, segment) in segments.iter().enumerate() {
        if i > 0 {
            key.push(b'/');
        }
        key.extend_from_slice(segment);
    }
    key
}

/// Hex-encode one path segment.
pub(super) fn encode_segment(segment: &[u8]) -> String {
    let mut out = String::with_capacity(segment.len() * 2);
    for &b in segment {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Decode a hex file name back into a segment, or `None` when it is not hex
/// (which also filters out `.tmp` leftovers).
pub(super) fn decode_segment(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
