//! Float `GROUP BY` aggregates: non-NaN results must match DataFusion.

mod float_support;

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use float_support::{Row, floats, ints, started, wait_for};

/// Reference rows computed by DataFusion over the same inputs.
async fn datafusion_rows(data: &[(i64, f64)]) -> Vec<Row> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            data.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            data.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("src", batch).unwrap();
    let query = "SELECT k, count(*), sum(v), avg(v), min(v), max(v) \
         FROM src GROUP BY k ORDER BY k";
    let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let (k, count, sum) = (ints(batch, 0), ints(batch, 1), floats(batch, 2));
        let (avg, min, max) = (floats(batch, 3), floats(batch, 4), floats(batch, 5));
        for i in 0..batch.num_rows() {
            out.push((
                k[i],
                count[i],
                sum[i].unwrap(),
                avg[i].unwrap(),
                min[i],
                max[i],
            ));
        }
    }
    out
}

#[tokio::test]
async fn float_aggregates_match_datafusion() {
    // Dyadic fractions keep both accumulators bit-identical regardless of the
    // order DataFusion folds its input.
    let data: &[(i64, f64)] = &[
        (1, 10.5),
        (1, 20.25),
        (2, 7.0),
        (1, 30.125),
        (2, 3.5),
        (3, 5.0),
        (1, 40.0),
        (2, -2.5),
    ];
    let mut session = started(data).await;
    let want = datafusion_rows(data).await;
    let got = wait_for(&mut session, |got| got == want.as_slice()).await;
    assert_eq!(got, want);
    session.shutdown().await.unwrap();
}
