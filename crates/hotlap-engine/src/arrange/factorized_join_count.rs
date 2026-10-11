mod candidate;
mod domain;
mod support;
#[cfg(test)]
mod candidate_tests {
    use super::{
        candidate::Candidate,
        domain::NotEligible,
        support::{batch, output, schema},
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
        let result = candidate
            .apply(&batch(&[]), &batch(&[(Some(1), Some(7), -1)]))
            .unwrap();
        assert_eq!(output(&result), [((Some(1), 10_000), -1)].into());
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
        let before = candidate.snapshot();
        let work = candidate.work();
        let error = candidate
            .apply(
                &batch(&[(Some(1), Some(11), 1), (Some(2), Some(12), -1)]),
                &batch(&[]),
            )
            .err()
            .expect("invalid update rejected");
        assert_eq!(error, NotEligible::NegativeState);
        assert_eq!(candidate.snapshot(), before);
        assert_eq!(candidate.work(), work);
        candidate.inject_output_failure(true);
        assert_eq!(
            candidate
                .apply(&batch(&[(Some(1), Some(11), 1)]), &batch(&[]))
                .err()
                .expect("injected output failure"),
            NotEligible::Encoding
        );
        assert_eq!(candidate.snapshot(), before);
        assert_eq!(candidate.work(), work);
        candidate.inject_output_failure(false);
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
        let before = candidate.snapshot();
        let work = candidate.work();
        let error = candidate
            .apply(
                &batch(&[(Some(1), Some(11), 1)]),
                &batch(&[(Some(1), Some(21), -1)]),
            )
            .err()
            .expect("invalid right update rejected");
        assert_eq!(error, NotEligible::NegativeState);
        assert_eq!(candidate.snapshot(), before);
        assert_eq!(candidate.work(), work);
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
}
