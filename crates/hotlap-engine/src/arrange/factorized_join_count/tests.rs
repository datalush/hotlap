use super::{
    candidate::Candidate,
    domain::NotEligible,
    manual::Manual,
    support::{Reference, Row, batch, batch_at, fold_output, oracle, output, schema},
};

#[test]
fn manual_comparator_red_sequence() {
    let steps = vec![
        (vec![(Some(1), Some(10), 2)], vec![(Some(1), Some(20), 3)]),
        (vec![(Some(1), Some(11), 1)], vec![(Some(1), Some(21), 2)]),
        (vec![(Some(1), Some(10), -2)], vec![(Some(1), Some(20), -3)]),
        (vec![(Some(1), Some(11), -1)], vec![]),
        (vec![(Some(1), Some(11), 1)], vec![]),
    ];
    let mut manual = Manual::new(0, 0);
    let mut left = Candidate::new(schema(), schema(), 0, 0).unwrap();
    let mut reference = Reference::new(0, 0);
    let mut manual_snapshot = Default::default();
    let mut candidate_snapshot = Default::default();
    let mut reference_snapshot = Default::default();
    let mut left_history = Vec::new();
    let mut right_history = Vec::new();
    let mut observed = Vec::new();
    for (l, r) in steps {
        let l = batch(&l);
        let r = batch(&r);
        let candidate_result = left.apply(&l, &r).unwrap();
        let reference_result = reference.apply(&l, &r).unwrap();
        let result = manual.apply(&l, &r).unwrap();
        assert_eq!(result.batch.schema(), reference_result.batch.schema());
        assert_eq!(output(&candidate_result), output(&reference_result));
        assert_eq!(output(&result), output(&reference_result));
        fold_output(&mut candidate_snapshot, &candidate_result);
        fold_output(&mut reference_snapshot, &reference_result);
        fold_output(&mut manual_snapshot, &result);
        observed.push(output(&result));
        left_history.extend(
            l.batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
                .iter()
                .zip(
                    l.diff()
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                        .values(),
                )
                .map(|(key, weight)| (Some(*key), None, *weight)),
        );
        right_history.extend(
            r.batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
                .iter()
                .zip(
                    r.diff()
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                        .values(),
                )
                .map(|(key, weight)| (Some(*key), None, *weight)),
        );
        assert_eq!(candidate_snapshot, reference_snapshot);
    }
    assert_eq!(observed.first(), Some(&[((Some(1), 6), 1)].into()));
    assert_eq!(manual_snapshot, reference_snapshot);
    assert_eq!(manual_snapshot, oracle(&left_history, &right_history));
}

fn check_steps(steps: &[(Vec<Row>, Vec<Row>)], left_key: usize, right_key: usize) {
    let mut candidate = Candidate::new(schema(), schema(), left_key, right_key).unwrap();
    let mut reference = Reference::new(left_key, right_key);
    let mut manual = Manual::new(left_key, right_key);
    let mut histories = [Vec::new(), Vec::new()];
    let mut snapshots = [Default::default(), Default::default(), Default::default()];
    for (left_rows, right_rows) in steps {
        let left = batch_at(left_rows, left_key);
        let right = batch_at(right_rows, right_key);
        let outputs = [
            candidate.apply(&left, &right).unwrap(),
            reference.apply(&left, &right).unwrap(),
            manual.apply(&left, &right).unwrap(),
        ];
        for actual in &outputs {
            assert_eq!(actual.batch.schema(), outputs[1].batch.schema());
            assert_eq!(output(actual), output(&outputs[1]));
        }
        for (snapshot, actual) in snapshots.iter_mut().zip(&outputs) {
            fold_output(snapshot, actual);
        }
        histories[0].extend(left_rows.iter().copied());
        histories[1].extend(right_rows.iter().copied());
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[2], snapshots[1]);
        assert_eq!(snapshots[1], oracle(&histories[0], &histories[1]));
    }
}

#[test]
fn nullable_payloads_duplicates_and_cancellations_match() {
    let steps = vec![
        (
            vec![(None, None, 2), (None, Some(3), 1), (None, None, -1)],
            vec![(None, None, 2), (None, Some(8), 1)],
        ),
        (vec![(None, Some(3), -1)], vec![(None, None, -1)]),
        (vec![(Some(4), None, 2)], vec![(Some(5), None, 7)]),
    ];
    check_steps(&steps, 0, 0);
}

#[test]
fn key_at_column_one_keeps_real_key_and_exact_schema() {
    check_steps(
        &[
            (vec![(Some(10), Some(1), 2)], vec![(Some(20), Some(1), 3)]),
            (vec![(Some(11), Some(1), 1)], vec![(Some(21), Some(1), 2)]),
            (vec![(Some(10), Some(1), -2)], vec![(Some(20), Some(1), -3)]),
        ],
        1,
        1,
    );
}

#[test]
fn wide_unmatched_side_does_not_narrow_summary_to_i64() {
    let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
    let mut reference = Reference::new(0, 0);
    let left = batch(&[(Some(3), Some(10), i64::MAX), (Some(3), Some(11), i64::MAX)]);
    let right = batch(&[]);
    let candidate_result = candidate.apply(&left, &right).unwrap();
    let reference_result = reference.apply(&left, &right).unwrap();
    assert_eq!(
        candidate_result.batch.schema(),
        reference_result.batch.schema()
    );
    assert!(output(&candidate_result).is_empty());
    assert!(output(&reference_result).is_empty());
}

#[test]
fn rejected_schema_and_negative_retraction_preserve_complete_digest() {
    let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
    candidate
        .apply(
            &batch(&[(Some(1), Some(10), 1)]),
            &batch(&[(Some(1), Some(20), 1)]),
        )
        .unwrap();
    let before = candidate.digest();
    let rejected = candidate
        .apply(&batch(&[(Some(1), Some(11), 1)]), &batch(&[]))
        .unwrap();
    assert_eq!(candidate.digest(), before);
    assert!(output(&rejected).is_empty());
    let before = candidate.digest();
    assert!(matches!(
        candidate.apply(&batch(&[]), &batch(&[(Some(1), Some(20), -2)])),
        Err(NotEligible::NegativeState)
    ));
    assert_eq!(candidate.digest(), before);

    // The reference is deliberately fresh; its actual behavior is characterized,
    // not assumed to match Candidate's domain-specific rejection.
    let mut reference = Reference::new(0, 0);
    let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        reference.apply(&batch(&[]), &batch(&[(Some(1), Some(20), -2)]))
    }));
    assert!(observed.is_ok());
}

#[test]
fn deterministic_histories_cover_eight_hundred_valid_steps() {
    for seed in [1_u64, 7, 42, 99] {
        let steps = generated_steps(seed);
        assert_eq!(steps.len(), 200);
        check_steps(&steps, 0, 0);
    }
}

fn generated_steps(mut state: u64) -> Vec<(Vec<Row>, Vec<Row>)> {
    fn next(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *state
    }
    let mut retained = [
        std::collections::BTreeMap::<(Option<i64>, Option<i64>), i64>::new(),
        std::collections::BTreeMap::new(),
    ];
    let mut steps = Vec::new();
    for step in 0..200 {
        let mut sides = [Vec::new(), Vec::new()];
        for (side, delta) in sides.iter_mut().enumerate() {
            let rows = &mut retained[side];
            let (row, change) = if step % 4 == 1 && !rows.is_empty() {
                let selected = next(&mut state) as usize % rows.len();
                (*rows.keys().nth(selected).unwrap(), -1)
            } else {
                (
                    (
                        Some((next(&mut state) % 8) as i64),
                        Some((next(&mut state) % 16) as i64),
                    ),
                    1,
                )
            };
            delta.push((row.0, row.1, change));
            if change < 0 {
                let weight = rows.get_mut(&row).unwrap();
                *weight -= 1;
                if *weight == 0 {
                    rows.remove(&row);
                }
            } else {
                *rows.entry(row).or_default() += change;
            }
        }
        if step % 9 == 0 {
            let row = (
                Some((next(&mut state) % 8) as i64),
                Some((next(&mut state) % 16) as i64),
            );
            sides[0].extend([(row.0, row.1, 1), (row.0, row.1, -1)]);
        }
        steps.push((sides[0].clone(), sides[1].clone()));
    }
    steps
}
