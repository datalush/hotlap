//! Table rendering for query results and engine metrics.

use std::io::{self, Write};
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use hotlap_runtime::session::MetricsSnapshot;

/// Write `batches` as an aligned, bordered table.
pub fn rows<W: Write>(out: &mut W, batches: &[RecordBatch]) -> io::Result<()> {
    let table = pretty_format_batches(batches).map_err(io::Error::other)?;
    writeln!(out, "{table}")
}

/// Write the metric snapshot as a `name`/`value` table.
pub fn metrics<W: Write>(out: &mut W, snapshot: &MetricsSnapshot) -> io::Result<()> {
    rows(out, &[metric_batch(snapshot)?])
}

/// Build the two-column record batch backing the metrics table.
fn metric_batch(snapshot: &MetricsSnapshot) -> io::Result<RecordBatch> {
    let names: Vec<&str> = snapshot.entries.keys().map(String::as_str).collect();
    let values: Vec<u64> = snapshot.entries.values().copied().collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::UInt64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(names)),
        Arc::new(UInt64Array::from(values)),
    ];
    RecordBatch::try_new(schema, columns).map_err(io::Error::other)
}
