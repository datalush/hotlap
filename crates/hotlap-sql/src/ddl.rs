//! Minimal DDL parser for CREATE SOURCE / CREATE MATERIALIZED VIEW / START.

use std::collections::BTreeMap;

use crate::ddl_scan::{matching_close, strip_prefix_ci, token_after};
use crate::error::SqlError;
use crate::watermark::parse_interval_ms;

const SOURCE_PREFIX: &str = "CREATE SOURCE ";
const VIEW_PREFIX: &str = "CREATE MATERIALIZED VIEW ";

/// A parsed DDL statement.
#[derive(Debug)]
pub enum Statement {
    CreateSource(CreateSource),
    CreateView(CreateView),
    Start,
}

/// A `CREATE SOURCE` statement, including its watermark lag.
#[derive(Debug)]
pub struct CreateSource {
    pub name: String,
    pub options: BTreeMap<String, String>,
    pub time_col: String,
    pub lag_ms: i64,
}

/// A `CREATE MATERIALIZED VIEW` statement and its query text.
#[derive(Debug)]
pub struct CreateView {
    pub name: String,
    pub query: String,
}

/// Parse a single DDL statement.
pub fn parse(sql: &str) -> Result<Statement, SqlError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper == "START" {
        return Ok(Statement::Start);
    }
    if upper.starts_with(SOURCE_PREFIX) {
        return parse_create_source(trimmed);
    }
    if upper.starts_with(VIEW_PREFIX) {
        return parse_create_view(trimmed);
    }
    Err(SqlError::Parse(format!("unrecognized statement: {trimmed}")))
}

fn parse_create_source(sql: &str) -> Result<Statement, SqlError> {
    let upper = sql.to_ascii_uppercase();
    let name = token_after(sql, SOURCE_PREFIX)?;
    let options = parse_options(sql, &upper)?;
    let (time_col, lag_ms) = parse_watermark(sql, &upper)?;
    Ok(Statement::CreateSource(CreateSource {
        name: name.to_string(),
        options,
        time_col,
        lag_ms,
    }))
}

fn parse_create_view(sql: &str) -> Result<Statement, SqlError> {
    let rest = strip_prefix_ci(sql, VIEW_PREFIX)?;
    let upper = rest.to_ascii_uppercase();
    let as_pos = upper
        .find(" AS ")
        .ok_or_else(|| SqlError::Parse("missing ` AS ` in CREATE VIEW".into()))?;
    let name = rest[..as_pos].trim();
    let query = rest[as_pos + " AS ".len()..].trim();
    if name.is_empty() || query.is_empty() {
        return Err(SqlError::Parse("incomplete CREATE VIEW".into()));
    }
    Ok(Statement::CreateView(CreateView {
        name: name.to_string(),
        query: query.to_string(),
    }))
}

/// Parse the `WITH (k='v', ...)` option block.
fn parse_options(sql: &str, upper: &str) -> Result<BTreeMap<String, String>, SqlError> {
    let with_pos = upper
        .find("WITH")
        .ok_or_else(|| SqlError::Parse("missing `WITH (...)`".into()))?;
    let open = upper[with_pos..]
        .find('(')
        .map(|i| with_pos + i)
        .ok_or_else(|| SqlError::Parse("missing `(` after WITH".into()))?;
    let close = matching_close(upper, open)
        .ok_or_else(|| SqlError::Parse("unbalanced `WITH (...)`".into()))?;
    parse_pairs(&sql[open + 1..close])
}

/// Parse `k='v'` pairs separated by commas.
fn parse_pairs(inner: &str) -> Result<BTreeMap<String, String>, SqlError> {
    let mut options = BTreeMap::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| SqlError::Parse(format!("bad option: `{part}`")))?;
        let key = key.trim();
        if key.is_empty() {
            return Err(SqlError::Parse(format!("bad option: `{part}`")));
        }
        let value = value.trim().trim_matches(['\'', '"']);
        options.insert(key.to_string(), value.to_string());
    }
    Ok(options)
}

/// Parse `WATERMARK FOR <col> AS <col> - INTERVAL '<body>'`.
fn parse_watermark(sql: &str, upper: &str) -> Result<(String, i64), SqlError> {
    let marker = "WATERMARK FOR ";
    let pos = upper
        .find(marker)
        .ok_or_else(|| SqlError::Parse("missing `WATERMARK FOR`".into()))?;
    let after = &sql[pos + marker.len()..];
    let time_col = after
        .split_whitespace()
        .next()
        .ok_or_else(|| SqlError::Parse("missing watermark column".into()))?;
    let upper_after = after.to_ascii_uppercase();
    let as_pos = upper_after
        .find(" AS ")
        .ok_or_else(|| SqlError::Parse("missing ` AS ` in WATERMARK".into()))?;
    let rhs = &after[as_pos + " AS ".len()..];
    let interval = "INTERVAL ";
    let pos = rhs
        .to_ascii_uppercase()
        .find(interval)
        .ok_or_else(|| SqlError::Parse("missing `INTERVAL` in WATERMARK".into()))?;
    let body = rhs[pos + interval.len()..].trim().trim_matches(['\'', '"']);
    let lag_ms = parse_interval_ms(body)?;
    Ok((time_col.to_string(), lag_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_source() {
        let stmt = parse("CREATE SOURCE src WITH (connector='fluss', bootstrap='localhost:9123', table='default.log') WATERMARK FOR ts AS ts - INTERVAL '5 s';").unwrap();
        match stmt {
            Statement::CreateSource(s) => {
                assert_eq!(s.name, "src");
                assert_eq!(s.options.get("connector").unwrap(), "fluss");
                assert_eq!(s.time_col, "ts");
                assert_eq!(s.lag_ms, 5_000);
            }
            _ => panic!("expected CreateSource"),
        }
    }

    #[test]
    fn parses_create_materialized_view() {
        let stmt = parse("CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) FROM src GROUP BY k, tumble(ts, INTERVAL '10 s');").unwrap();
        match stmt {
            Statement::CreateView(v) => {
                assert_eq!(v.name, "mv");
                assert!(v.query.to_lowercase().contains("select"));
            }
            _ => panic!("expected CreateView"),
        }
    }

    #[test]
    fn parses_start() {
        assert!(matches!(parse("START;").unwrap(), Statement::Start));
    }
}
