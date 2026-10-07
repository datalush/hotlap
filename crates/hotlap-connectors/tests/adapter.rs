use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use hotlap_connectors::datafusion::provider::SourceTableProvider;
use hotlap_connectors::source::{Source, SourceBatch, SourceState, SourceStream, Split};

struct OneBatchSource {
    schema: SchemaRef,
    batch: RecordBatch,
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
        })];
        Ok(Box::pin(futures::stream::iter(items)))
    }
    fn state(&self) -> SourceState {
        SourceState::default()
    }
    fn event_time_column(&self) -> Option<usize> {
        None
    }
}

#[tokio::test]
async fn select_counts_source_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let source: Arc<dyn Source> = Arc::new(OneBatchSource { schema, batch });

    let ctx = SessionContext::new();
    ctx.register_table("src", Arc::new(SourceTableProvider::new(source)))
        .unwrap();
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
