//! Normalize `tumble(<col>, INTERVAL '<body>')` calls to integer milliseconds.
//!
//! DataFusion has no interval-literal representation for the compact interval
//! bodies accepted by the DDL layer, so the SQL text is rewritten before
//! planning: the `tumble` size becomes the millisecond value the kernel expects.

use crate::ddl_scan::matching_close;
use crate::error::SqlError;
use crate::watermark::parse_interval_ms;

/// Rewrite every `tumble` call whose size argument is an `INTERVAL` literal.
pub fn normalize_tumble_intervals(query: &str) -> Result<String, SqlError> {
    let mut out = String::with_capacity(query.len());
    let mut rest = query;
    while let Some((start, open)) = find_tumble_call(rest) {
        out.push_str(&rest[..start]);
        let close = matching_close(rest, open)
            .ok_or_else(|| SqlError::Parse("unbalanced `tumble(`".into()))?;
        out.push_str("tumble(");
        out.push_str(&normalize_args(&rest[open + 1..close])?);
        out.push(')');
        rest = &rest[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Locate the next `tumble(` call, returning its start and the `(` position.
fn find_tumble_call(text: &str) -> Option<(usize, usize)> {
    let lower = text.to_ascii_lowercase();
    let mut from = 0;
    while let Some(offset) = lower[from..].find("tumble") {
        let start = from + offset;
        let after = start + "tumble".len();
        let tail = &text[after..];
        let spaces = tail.len() - tail.trim_start().len();
        if tail[spaces..].starts_with('(') {
            return Some((start, after + spaces));
        }
        from = after;
    }
    None
}

/// Normalize the inside of a `tumble(...)` call.
fn normalize_args(inner: &str) -> Result<String, SqlError> {
    let (first, second) = inner
        .split_once(',')
        .ok_or_else(|| SqlError::Parse(format!("tumble expects two arguments: `{inner}`")))?;
    if !second.trim().to_ascii_uppercase().starts_with("INTERVAL") {
        return Ok(inner.to_string());
    }
    let ms = parse_interval_ms(interval_body(second.trim())?)?;
    Ok(format!("{}, {}", first.trim(), ms))
}

/// Extract the quoted body of an `INTERVAL '<body>'` literal.
fn interval_body(arg: &str) -> Result<&str, SqlError> {
    let rest = arg
        .get("INTERVAL".len()..)
        .ok_or_else(|| SqlError::Parse(format!("bad INTERVAL: `{arg}`")))?
        .trim();
    let body = rest.trim_matches(['\'', '"']);
    if body.is_empty() || body == rest {
        return Err(SqlError::Parse(format!("bad INTERVAL literal: `{arg}`")));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_interval_sizes() {
        let out = normalize_tumble_intervals(
            "SELECT k FROM src GROUP BY k, tumble(_event_time, INTERVAL '10 s')",
        )
        .unwrap();
        assert!(out.contains("tumble(_event_time, 10000)"), "{out}");
    }

    #[test]
    fn leaves_integer_sizes_untouched() {
        let sql = "SELECT k FROM src GROUP BY k, tumble(ts, 500)";
        assert_eq!(normalize_tumble_intervals(sql).unwrap(), sql);
    }

    #[test]
    fn rejects_malformed_tumble() {
        assert!(normalize_tumble_intervals("SELECT tumble(ts)").is_err());
    }
}
