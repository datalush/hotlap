mod candidate;
mod domain;
mod support;
#[cfg(test)]
mod candidate_tests {
    use super::{
        candidate::Candidate,
        domain::NotEligible,
        support::{Reference, batch, output, schema},
    };

    #[test]
    fn provisional_candidate_emits_factorized_count() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        let left = batch(&[(Some(1), Some(10), 2)]);
        let right = batch(&[(Some(1), Some(20), 3)]);
        let result = candidate.apply(&left, &right).unwrap();
        assert_eq!(output(&result), [((Some(1), 6), 1)].into());
    }

    #[test]
    fn right_side_retraction_changes_count_and_work_is_row_bounded() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        let left_rows: Vec<_> = (0..10_000).map(|value| (Some(1), Some(value), 1)).collect();
        let right = batch(&[(Some(1), Some(7), 1)]);
        candidate.apply(&batch(&left_rows), &right).unwrap();
        let mut reference = Reference::new(0, 0);
        reference.apply(&batch(&left_rows), &right).unwrap();
        let result = candidate
            .apply(&batch(&[]), &batch(&[(Some(1), Some(7), -1)]))
            .unwrap();
        let expected = reference
            .apply(&batch(&[]), &batch(&[(Some(1), Some(7), -1)]))
            .unwrap();
        assert_eq!(output(&result), [((Some(1), 10_000), -1)].into());
        assert_eq!(result.batch.schema(), expected.batch.schema());
        assert_eq!(output(&result), output(&expected));
        assert_eq!(candidate.work().row_lookups, 1);
        assert_eq!(candidate.work().touched_keys, 1);
        assert_eq!(reference.work(), 10_000);
        assert_eq!(candidate.work().row_lookups, 1);
        assert_eq!(candidate.work().touched_keys, 1);
    }

    #[test]
    fn invalid_second_key_and_output_failure_leave_state_unchanged() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        candidate
            .apply(
                &batch(&[(Some(1), Some(10), 1)]),
                &batch(&[(Some(1), Some(20), 1)]),
            )
            .unwrap();
        let before = candidate.digest();
        let error = candidate
            .apply(
                &batch(&[(Some(1), Some(11), 1), (Some(2), Some(12), -1)]),
                &batch(&[]),
            )
            .err()
            .expect("invalid update rejected");
        assert_eq!(error, NotEligible::NegativeState);
        assert_eq!(candidate.digest(), before);
        let error = candidate
            .apply(
                &batch(&[(Some(1), Some(11), 1)]),
                &batch(&[(Some(1), Some(21), -1)]),
            )
            .err()
            .expect("invalid right side rejected");
        assert_eq!(error, NotEligible::NegativeState);
        assert_eq!(candidate.digest(), before);
        candidate.inject_output_failure(true);
        assert_eq!(
            candidate
                .apply(&batch(&[(Some(1), Some(11), 1)]), &batch(&[]))
                .err()
                .expect("injected output failure"),
            NotEligible::Encoding
        );
        assert_eq!(candidate.digest(), before);
        candidate.inject_output_failure(false);
    }

    #[test]
    fn schema_rejection_after_valid_left_input_preserves_complete_digest() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        candidate
            .apply(
                &batch(&[(Some(1), Some(10), 1)]),
                &batch(&[(Some(1), Some(20), 1)]),
            )
            .unwrap();
        let before = candidate.digest();
        let invalid = batch(&[(Some(1), Some(21), 1)]);
        let error = candidate
            .apply(
                &batch(&[(Some(1), Some(11), 1)]),
                &with_wrong_schema(invalid),
            )
            .err()
            .expect("schema rejected");
        assert_eq!(error, NotEligible::Schema);
        assert_eq!(candidate.digest(), before);
    }

    #[test]
    fn row_and_numeric_bounds_preserve_complete_digest() {
        let mut row_candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        row_candidate
            .apply(&batch(&[(Some(2), Some(10), i64::MAX)]), &batch(&[]))
            .unwrap();
        let before = row_candidate.digest();
        assert_eq!(
            row_candidate
                .apply(&batch(&[(Some(2), Some(10), 1)]), &batch(&[]))
                .err()
                .expect("row bound"),
            NotEligible::RowBound
        );
        assert_eq!(row_candidate.digest(), before);

        let mut numeric = Candidate::new(schema(), schema(), 0, 0).unwrap();
        numeric
            .apply(
                &batch(&[(Some(3), Some(10), i64::MAX)]),
                &batch(&[(Some(3), Some(20), 1)]),
            )
            .unwrap();
        let before = numeric.digest();
        assert_eq!(
            numeric
                .apply(&batch(&[]), &batch(&[(Some(3), Some(21), 1)]))
                .err()
                .expect("numeric bound"),
            NotEligible::NumericBound
        );
        assert_eq!(numeric.digest(), before);
    }

    #[test]
    fn invalid_second_side_leaves_left_and_right_unchanged() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        candidate
            .apply(
                &batch(&[(Some(1), Some(10), 1)]),
                &batch(&[(Some(1), Some(20), 1)]),
            )
            .unwrap();
        let before = candidate.digest();
        let error = candidate
            .apply(
                &batch(&[(Some(1), Some(11), 1)]),
                &batch(&[(Some(1), Some(21), -1)]),
            )
            .err()
            .expect("invalid right update rejected");
        assert_eq!(error, NotEligible::NegativeState);
        assert_eq!(candidate.digest(), before);
    }

    #[test]
    fn zero_net_cancellation_still_enforces_absolute_row_bound() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        let oversized = batch(&[
            (Some(1), Some(10), i64::MAX),
            (Some(1), Some(10), 1),
            (Some(1), Some(10), -i64::MAX),
            (Some(1), Some(10), -1),
        ]);
        let error = candidate
            .apply(&oversized, &batch(&[]))
            .err()
            .expect("absolute weight bound");
        assert_eq!(error, NotEligible::RowBound);
        assert!(candidate.snapshot().is_empty());
    }

    #[test]
    fn candidate_bounds_prefix_and_retains_zero_product_summary() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        candidate
            .apply(
                &batch(&[(Some(4), Some(1), 1)]),
                &batch(&[(Some(4), Some(2), i64::MAX)]),
            )
            .unwrap();
        assert_eq!(
            candidate
                .apply(&batch(&[(Some(4), Some(1), 1)]), &batch(&[]))
                .err()
                .expect("count bound"),
            NotEligible::NumericBound
        );
        let mut unmatched = Candidate::new(schema(), schema(), 0, 0).unwrap();
        unmatched
            .apply(&batch(&[(Some(9), Some(1), 1)]), &batch(&[]))
            .unwrap();
        assert_eq!(unmatched.summary_count(), 1);
        unmatched
            .apply(&batch(&[]), &batch(&[(Some(9), Some(2), 1)]))
            .unwrap();
        assert_eq!(unmatched.snapshot(), [((Some(9), 1), 1)].into());
    }

    #[test]
    fn positive_growth_accepts_absolute_weight_greater_than_old_count() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        candidate
            .apply(
                &batch(&[(Some(1), Some(10), 1)]),
                &batch(&[(Some(1), Some(20), 1)]),
            )
            .unwrap();
        let result = candidate
            .apply(&batch(&[(Some(1), Some(11), 2)]), &batch(&[]))
            .unwrap();
        assert_eq!(
            output(&result),
            [((Some(1), 1), -1), ((Some(1), 3), 1)].into()
        );
    }

    #[test]
    fn key_named_count_preserves_exact_output_columns() {
        let source = named_count_schema();
        let mut candidate = Candidate::new(source.clone(), source, 0, 0).unwrap();
        let result = candidate
            .apply(
                &named_count_batch(&[(Some(7), Some(3), 2)]),
                &named_count_batch(&[(Some(7), Some(4), 3)]),
            )
            .unwrap();
        assert_eq!(
            result
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values(),
            &[7]
        );
        assert_eq!(
            result
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values(),
            &[6]
        );
        assert_eq!(result.batch.schema().field(0).name(), "count");
        assert_eq!(result.batch.schema().field(1).name(), "count");
    }

    #[test]
    fn candidate_output_is_canonical() {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        let left = batch(&[(Some(2), Some(10), 1), (Some(1), Some(11), 1)]);
        let right = batch(&[(Some(2), Some(20), 1), (Some(1), Some(21), 1)]);
        let raw = candidate.apply(&left, &right).unwrap();
        let canonical = crate::consolidate(&raw).unwrap();
        assert_eq!(raw.batch, canonical.batch);
        assert_eq!(
            raw.diff
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values(),
            canonical
                .diff
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
        );
    }

    fn named_count_schema() -> arrow::datatypes::SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, true),
            Field::new("payload", DataType::Int64, true),
        ]))
    }

    fn named_count_batch(rows: &[(Option<i64>, Option<i64>, i64)]) -> hotlap_core::ZSetBatch {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let batch = RecordBatch::try_new(
            named_count_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        hotlap_core::ZSetBatch::new(
            batch,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        )
        .unwrap()
    }

    fn with_wrong_schema(batch: hotlap_core::ZSetBatch) -> hotlap_core::ZSetBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("other", DataType::Int64, true),
        ]));
        let record = RecordBatch::try_new(schema, batch.batch.columns().to_vec()).unwrap();
        hotlap_core::ZSetBatch::new(
            record,
            Arc::new(Int64Array::from(
                batch
                    .diff()
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec(),
            )),
        )
        .unwrap()
    }
}
