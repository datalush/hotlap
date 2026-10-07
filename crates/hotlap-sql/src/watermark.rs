//! Watermark interval parsing and event-time column resolution.

use arrow::datatypes::Schema;

use crate::error::SqlError;

/// Parse an interval body like `"5 s"` / `"500 ms"` / `"2 min"` into milliseconds.
pub fn parse_interval_ms(body: &str) -> Result<i64, SqlError> {
    let parts: Vec<&str> = body.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(SqlError::Parse(format!("bad interval: `{body}`")));
    }
    let n: i64 = parts[0]
        .parse()
        .map_err(|_| SqlError::Parse(format!("bad interval number: `{}`", parts[0])))?;
    let unit_ms: i64 = match parts[1] {
        "ms" => 1,
        "s" => 1_000,
        "min" | "m" => 60_000,
        "h" => 3_600_000,
        other => return Err(SqlError::Parse(format!("unknown interval unit: `{other}`"))),
    };
    n.checked_mul(unit_ms)
        .ok_or_else(|| SqlError::Parse("interval overflow".into()))
}

/// Resolve a column name to its index in the source Arrow schema.
pub fn column_index(schema: &Schema, name: &str) -> Result<usize, SqlError> {
    schema
        .fields()
        .iter()
        .position(|f| f.name() == name)
        .ok_or_else(|| SqlError::Catalog(format!("unknown column: {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_units() {
        assert_eq!(parse_interval_ms("5 s").unwrap(), 5_000);
        assert_eq!(parse_interval_ms("500 ms").unwrap(), 500);
        assert_eq!(parse_interval_ms("2 min").unwrap(), 120_000);
        assert_eq!(parse_interval_ms("1 h").unwrap(), 3_600_000);
    }

    #[test]
    fn interval_rejects_bad() {
        assert!(parse_interval_ms("5 fortnights").is_err());
        assert!(parse_interval_ms("s").is_err());
    }
}
