use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::prelude::SessionContext;

use hotlap_connectors::datafusion::provider::SourceTableProvider;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};

struct OneBatchSource {
    schema: SchemaRef,
    batch: RecordBatch,
    unbounded: bool,
}

impl Source for OneBatchSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn splits(&self) -> Result<Vec<Split>, hotlap_connectors::ConnectorError> {
        Ok(vec![Split { id: 0, start: 0 }])
    }
    fn read(&self, _split: &Split) -> Result<SourceStream, hotlap_connectors::ConnectorError> {
        let items: Vec<Result<SourceBatch, _>> = vec![Ok(SourceBatch {
            batch: self.batch.clone(),
            base_offset: 0,
            next_offset: 3,
            split: 0,
        })];
        Ok(Box::pin(futures::stream::iter(items)))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
    fn is_unbounded(&self) -> bool {
        self.unbounded
    }
}

fn one_batch_source(unbounded: bool) -> Arc<dyn Source> {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    Arc::new(OneBatchSource {
        schema,
        batch,
        unbounded,
    })
}

fn register(ctx: &SessionContext, source: Arc<dyn Source>) {
    ctx.register_table("src", Arc::new(SourceTableProvider::new(source)))
        .unwrap();
}

#[tokio::test]
async fn select_counts_source_rows() {
    let ctx = SessionContext::new();
    register(&ctx, one_batch_source(false));
    let df = ctx.sql("SELECT count(*) AS n FROM src").await.unwrap();
    let out = df.collect().await.unwrap();
    let n = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 3);
}

#[tokio::test]
async fn unbounded_source_limits_rows() {
    let ctx = SessionContext::new();
    register(&ctx, one_batch_source(true));
    let df = ctx.sql("SELECT k FROM src LIMIT 1").await.unwrap();
    let out = df.collect().await.unwrap();
    let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn scan_boundedness_follows_source() {
    let ctx = SessionContext::new();
    let state = ctx.state();

    let bounded = SourceTableProvider::new(one_batch_source(false));
    let plan = bounded.scan(&state, None, &[], None).await.unwrap();
    assert!(matches!(
        plan.properties().boundedness,
        Boundedness::Bounded
    ));

    let unbounded = SourceTableProvider::new(one_batch_source(true));
    let plan = unbounded.scan(&state, None, &[], None).await.unwrap();
    assert!(matches!(
        plan.properties().boundedness,
        Boundedness::Unbounded { .. }
    ));
}
