use arrow::array::Int64Array;

pub fn rows(batch: &hotlap::ZSetBatch) -> (Vec<Vec<i64>>, Vec<i64>) {
    let values = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = batch.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let rows = (0..values.len())
        .map(|index| vec![values.value(index)])
        .collect();
    (rows, diffs.values().to_vec())
}
