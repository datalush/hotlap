//! Compact little-endian encoding for a staged weighted changelog.

/// Encode `(row, diff)` pairs as a length-prefixed little-endian buffer.
pub fn encode(changes: &[(Vec<i64>, i64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend((changes.len() as u32).to_le_bytes());
    for (row, diff) in changes {
        out.extend((row.len() as u32).to_le_bytes());
        for value in row {
            out.extend(value.to_le_bytes());
        }
        out.extend(diff.to_le_bytes());
    }
    out
}

/// Decode a buffer produced by [`encode`].
pub fn decode(bytes: &[u8]) -> Vec<(Vec<i64>, i64)> {
    let mut cursor = 0;
    let count = take_u32(bytes, &mut cursor);
    let mut out = Vec::new();
    for _ in 0..count {
        let len = take_u32(bytes, &mut cursor) as usize;
        let mut row = Vec::with_capacity(len);
        for _ in 0..len {
            row.push(take_i64(bytes, &mut cursor));
        }
        out.push((row, take_i64(bytes, &mut cursor)));
    }
    out
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> u32 {
    let value = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    value
}

fn take_i64(bytes: &[u8], cursor: &mut usize) -> i64 {
    let value = i64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    value
}
