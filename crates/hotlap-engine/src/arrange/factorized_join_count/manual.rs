use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use hotlap_core::ZSetBatch;
use std::sync::Arc;

pub(super) struct Manual {
    left: crate::ops::group_aggregate::GroupAggregate,
    right: crate::ops::group_aggregate::GroupAggregate,
    join: crate::ops::join::Join,
    left_key: usize,
    right_key: usize,
    output_schema: Option<SchemaRef>,
}

impl Manual {
    pub(super) fn new(left_key: usize, right_key: usize) -> Self {
        Self {
            left: crate::ops::group_aggregate::GroupAggregate::new(
                &[left_key],
                vec![hotlap_core::AggSpec::count()],
            ),
            right: crate::ops::group_aggregate::GroupAggregate::new(
                &[right_key],
                vec![hotlap_core::AggSpec::count()],
            ),
            join: crate::ops::join::Join::new(&[0], &[0]),
            left_key,
            right_key,
            output_schema: None,
        }
    }

    pub(super) fn apply(
        &mut self,
        left: &ZSetBatch,
        right: &ZSetBatch,
    ) -> Result<ZSetBatch, crate::EngineError> {
        let schema = self
            .output_schema
            .get_or_insert_with(|| {
                let mut reference = super::support::Reference::new(self.left_key, self.right_key);
                reference
                    .apply(
                        &super::support::empty_batch(left.batch.schema()),
                        &super::support::empty_batch(right.batch.schema()),
                    )
                    .unwrap()
                    .batch
                    .schema()
            })
            .clone();
        let left_summary = self.left.apply(left)?;
        let right_summary = self.right.apply(right)?;
        let joined = self.join.apply(&left_summary, &right_summary)?;
        let key = joined
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left_count = joined
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_count = joined
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let diffs = joined.diff.as_any().downcast_ref::<Int64Array>().unwrap();
        let counts = (0..joined.len())
            .map(|i| {
                left_count.value(i).checked_mul(right_count.value(i)).ok_or(
                    crate::EngineError::Unsupported("manual count overflow".into()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    (0..joined.len())
                        .map(|i| {
                            if key.is_null(i) {
                                None
                            } else {
                                Some(key.value(i))
                            }
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(counts)),
            ],
        )?;
        crate::consolidate(&ZSetBatch::new(
            batch,
            Arc::new(Int64Array::from(diffs.values().to_vec())),
        )?)
    }
}
