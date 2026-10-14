use crate::ZSetBatch;
use arrow::array::{Array, Int64Array};

pub(super) fn rows(batch: &ZSetBatch) -> Vec<Vec<Option<i64>>> {
    let diffs = batch.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..batch.len())
        .map(|i| {
            let mut row = (0..batch.batch.num_columns())
                .map(|column| {
                    let values = batch
                        .batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (!values.is_null(i)).then(|| values.value(i))
                })
                .collect::<Vec<_>>();
            row.push(Some(diffs.value(i)));
            row
        })
        .collect()
}

pub(super) fn canonical_rows(batch: &ZSetBatch) -> Vec<Vec<Option<i64>>> {
    let mut result = rows(batch);
    result.sort();
    result
}
