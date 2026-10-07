//! `CREATE SINK` parsing.

use std::collections::BTreeMap;

use super::parse_options;
use crate::ddl_scan::token_after;
use crate::error::SqlError;

const SINK_PREFIX: &str = "CREATE SINK ";

/// A `CREATE SINK` statement: its target view plus connector options.
#[derive(Debug)]
pub struct CreateSink {
    pub name: String,
    pub options: BTreeMap<String, String>,
    pub view: String,
}

/// Parse `CREATE SINK <name> WITH (...) AS SELECT ... FROM <view>`.
pub(super) fn parse(sql: &str) -> Result<CreateSink, SqlError> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let upper = sql.to_ascii_uppercase();
    let name = token_after(sql, SINK_PREFIX)?;
    let options = parse_options(sql, &upper)?;
    let view = parse_view(sql, &upper)?;
    Ok(CreateSink {
        name: name.to_string(),
        options,
        view,
    })
}

/// Extract the source view name from the `AS SELECT ... FROM <view>` tail.
fn parse_view(sql: &str, upper: &str) -> Result<String, SqlError> {
    let as_pos = upper
        .find(" AS ")
        .ok_or_else(|| SqlError::Parse("missing ` AS ` in CREATE SINK".into()))?;
    let after = &sql[as_pos + " AS ".len()..];
    let upper_after = &upper[as_pos + " AS ".len()..];
    let from_pos = upper_after
        .find(" FROM ")
        .ok_or_else(|| SqlError::Parse("missing ` FROM ` in CREATE SINK".into()))?;
    let view = after[from_pos + " FROM ".len()..]
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(['\'', '"']);
    if view.is_empty() {
        return Err(SqlError::Parse("missing view name in CREATE SINK".into()));
    }
    Ok(view.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_sink() {
        let s =
            parse("CREATE SINK out WITH (connector='fluss', table='db/t') AS SELECT * FROM mv;")
                .unwrap();
        assert_eq!(s.name, "out");
        assert_eq!(s.options.get("connector").unwrap(), "fluss");
        assert_eq!(s.view, "mv");
    }
}
