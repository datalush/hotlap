//! Small string-scanning helpers for the minimal DDL parser.

use crate::error::SqlError;

/// Strip an ASCII-case-insensitive prefix, returning the trimmed remainder.
pub fn strip_prefix_ci<'a>(sql: &'a str, prefix: &str) -> Result<&'a str, SqlError> {
    let head = sql
        .get(..prefix.len())
        .ok_or_else(|| SqlError::Parse(format!("expected `{prefix}`")))?;
    if !head.eq_ignore_ascii_case(prefix) {
        return Err(SqlError::Parse(format!("expected `{prefix}`")));
    }
    Ok(sql[prefix.len()..].trim_start())
}

/// Return the token following `prefix`, matching the prefix case-insensitively.
pub fn token_after<'a>(sql: &'a str, prefix: &str) -> Result<&'a str, SqlError> {
    strip_prefix_ci(sql, prefix)?
        .split_whitespace()
        .next()
        .ok_or_else(|| SqlError::Parse(format!("missing token after `{prefix}`")))
}

/// Find the `)` matching the `(` at `open`, honoring nesting depth.
pub fn matching_close(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (i, c) in text[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + i);
                }
            }
            _ => {}
        }
    }
    None
}
