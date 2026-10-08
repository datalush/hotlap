//! Differential test: our predicate mask must equal DataFusion's `PhysicalExpr`
//! evaluated on the same batch, null (unknown) cells included.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{ColumnarValue, Filter, LogicalPlan};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use hotlap::{InputId, Plan, Predicate};
use hotlap_sql::translate::to_kernel_plan;

/// Three rows across every scalar type, each with a null somewhere.
fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
        Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)])),
        Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn find_logical_filter(plan: &LogicalPlan) -> Option<&Filter> {
    match plan {
        LogicalPlan::Filter(f) => Some(f),
        LogicalPlan::Projection(p) => find_logical_filter(&p.input),
        _ => None,
    }
}

fn find_kernel_predicate(plan: &Plan) -> Option<&Predicate> {
    match plan {
        Plan::Filter { pred, .. } => Some(pred),
        Plan::Project { input, .. } => find_kernel_predicate(input),
        _ => None,
    }
}

fn options(mask: &BooleanArray) -> Vec<Option<bool>> {
    (0..mask.len())
        .map(|index| (!mask.is_null(index)).then(|| mask.value(index)))
        .collect()
}

/// Evaluates a DataFusion physical expression into a three-valued mask.
fn physical_mask(expr: Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Vec<Option<bool>> {
    let value = expr.evaluate(batch).unwrap();
    let array: ArrayRef = match value {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(ScalarValue::Boolean(v)) => {
            Arc::new(BooleanArray::from(vec![v; batch.num_rows()]))
        }
        ColumnarValue::Scalar(other) => panic!("expected a boolean mask, got {other:?}"),
    };
    options(array.as_any().downcast_ref::<BooleanArray>().unwrap())
}

const CASES: &[&str] = &[
    "i < 3",
    "i <= 2",
    "i > 1",
    "i >= 3",
    "i = 2",
    "i <> 2",
    "f = 2.0",
    "f > 1.5",
    "f <= 1.0",
    "k < 3 AND i > 1",
    "k < 3 OR i > 3",
    "NOT (k = 1)",
    "s = 'a'",
    "s <> 'b'",
    "b = true",
    "k IS NULL",
    "k IS NOT NULL",
    "i > 1 AND s = 'a'",
    "NOT (i IS NULL)",
    "i > 1 OR f < 2.0",
    // Mixed-type comparisons: DataFusion promotes both sides to a common
    // supertype at execution; the kernel must not truncate the literal.
    "k > -1.5",
    "k < 2.5",
    "k = 1.5",
    "i > -1.5",
    "f > 1",
    "f = 1",
];

#[tokio::test]
async fn matches_datafusion_physical_expr() {
    let ctx = SessionContext::new();
    let batch = batch();
    let mem = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("src", Arc::new(mem)).unwrap();

    for where_sql in CASES {
        let df = ctx
            .sql(&format!("SELECT * FROM src WHERE {where_sql}"))
            .await
            .unwrap();
        let logical = df.logical_plan();
        let filter = find_logical_filter(logical).expect("a filter in the plan");
        let physical = ctx
            .create_physical_expr(filter.predicate.clone(), filter.input.schema())
            .unwrap();
        let expected = physical_mask(physical, &batch);

        let plan = to_kernel_plan(logical, InputId(0)).unwrap();
        let pred = find_kernel_predicate(&plan).expect("a kernel filter");
        let got = options(&pred.eval(&batch).unwrap());

        assert_eq!(got, expected, "predicate `{where_sql}` diverged");
    }
}
