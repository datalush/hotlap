use super::{
    candidate::Candidate,
    manual::Manual,
    support::{Output, Reference, Row, batch, fold_output, schema},
};

struct Workload {
    left: Vec<Row>,
    right: Vec<Row>,
    steps: Vec<(Vec<Row>, Vec<Row>)>,
    warmup: usize,
    measured: usize,
    expected: i64,
}

fn workload(case: &str) -> Workload {
    let (left, right, steps, warmup, measured, expected) = match case {
        "distinct" => (
            (0..10_000).map(|p| (Some(0), Some(p), 1)).collect(),
            vec![(Some(0), Some(0), 1)],
            (0..440)
                .map(|n| {
                    (
                        vec![],
                        vec![(Some(0), Some(0), if n % 2 == 0 { -1 } else { 1 })],
                    )
                })
                .collect(),
            40,
            400,
            10_000,
        ),
        "duplicates" => (
            vec![(Some(0), Some(0), 10_000)],
            vec![(Some(0), Some(0), 1)],
            (0..440)
                .map(|n| {
                    (
                        vec![],
                        vec![(Some(0), Some(0), if n % 2 == 0 { -1 } else { 1 })],
                    )
                })
                .collect(),
            40,
            400,
            10_000,
        ),
        "low" => (
            (0..1_000).map(|k| (Some(k), Some(0), 1)).collect(),
            (0..1_000).map(|k| (Some(k), Some(0), 1)).collect(),
            (0..440)
                .map(|n| {
                    let key = n;
                    (
                        vec![(Some(key), Some(0), -1), (Some(key), Some(1), 1)],
                        vec![],
                    )
                })
                .collect(),
            40,
            400,
            1_000,
        ),
        "mixed" => (
            (0..1_000).map(|p| (Some(0), Some(p), 1)).collect(),
            (0..8).map(|p| (Some(0), Some(p), 1)).collect(),
            (0..440)
                .map(|n| {
                    let old_l = if n == 0 { 0 } else { 999 + n };
                    let old_r = if n == 0 { 0 } else { 7 + n };
                    (
                        vec![(Some(0), Some(old_l), -1), (Some(0), Some(1_000 + n), 1)],
                        vec![(Some(0), Some(old_r), -1), (Some(0), Some(8 + n), 1)],
                    )
                })
                .collect(),
            40,
            400,
            8_000,
        ),
        _ => panic!("unsupported benchmark case: {case}"),
    };
    Workload {
        left,
        right,
        steps,
        warmup,
        measured,
        expected,
    }
}

#[test]
fn workload_shapes_and_boundaries_are_exact() {
    let distinct = workload("distinct");
    assert_eq!(distinct.left.len(), 10_000);
    assert_eq!(distinct.left.iter().map(|r| r.2).sum::<i64>(), 10_000);
    let duplicates = workload("duplicates");
    assert_eq!(duplicates.left, [(Some(0), Some(0), 10_000)]);
    assert_eq!(distinct.expected, 10_000);
    assert_eq!(distinct.right, [(Some(0), Some(0), 1)]);
    let low = workload("low");
    assert_eq!(low.left.len(), 1_000);
    assert_eq!(low.right.len(), 1_000);
    assert_eq!(low.steps[39].0[0], (Some(39), Some(0), -1));
    assert_eq!(low.steps[40].0[0], (Some(40), Some(0), -1));
    let mixed = workload("mixed");
    assert_eq!(
        mixed.steps[0].0,
        [(Some(0), Some(0), -1), (Some(0), Some(1_000), 1)]
    );
    assert_eq!(
        mixed.steps[0].1,
        [(Some(0), Some(0), -1), (Some(0), Some(8), 1)]
    );
    for case in ["distinct", "duplicates", "low", "mixed"] {
        let load = workload(case);
        assert_eq!(load.steps.len(), 440);
        assert_eq!(load.warmup, 40);
        assert_eq!(load.measured, 400);
        assert!(load.expected > 0);
    }
}

#[test]
#[ignore = "wall-clock and process-memory measurements"]
fn measure() {
    let strategy = std::env::var("HOTLAP_FACTOR_STRATEGY").expect("strategy required");
    let case = std::env::var("HOTLAP_FACTOR_CASE").expect("case required");
    let rep: u8 = std::env::var("HOTLAP_FACTOR_REP")
        .expect("rep required")
        .parse()
        .unwrap();
    assert!(rep < 5, "rep must be 0..4");
    let data = workload(&case);
    let setup_start = std::time::Instant::now();
    let (mut candidate, mut reference, mut manual) = match strategy.as_str() {
        "candidate" => (
            Some(Candidate::new(schema(), schema(), 0, 0).unwrap()),
            None,
            None,
        ),
        "reference" => (None, Some(Reference::new(0, 0)), None),
        "manual" => (None, None, Some(Manual::new(0, 0))),
        _ => panic!("unsupported strategy: {strategy}"),
    };
    let seed_left = batch(&data.left);
    let seed_right = batch(&data.right);
    let setup_ns = setup_start.elapsed().as_nanos();
    let (seed_output, seed_ns) = match strategy.as_str() {
        "candidate" => {
            let start = std::time::Instant::now();
            let out = candidate
                .as_mut()
                .unwrap()
                .apply(&seed_left, &seed_right)
                .unwrap();
            (out, start.elapsed().as_nanos())
        }
        "reference" => {
            let start = std::time::Instant::now();
            let out = reference
                .as_mut()
                .unwrap()
                .apply(&seed_left, &seed_right)
                .unwrap();
            (out, start.elapsed().as_nanos())
        }
        "manual" => {
            let start = std::time::Instant::now();
            let out = manual
                .as_mut()
                .unwrap()
                .apply(&seed_left, &seed_right)
                .unwrap();
            (out, start.elapsed().as_nanos())
        }
        _ => panic!("unsupported strategy: {strategy}"),
    };
    let mut folded = Output::new();
    fold_output(&mut folded, &seed_output);
    let mut warmup_left = data.left.clone();
    let mut warmup_right = data.right.clone();
    for (left, right) in data.steps.iter().take(data.warmup) {
        warmup_left.extend(left.iter().copied());
        warmup_right.extend(right.iter().copied());
    }
    assert_eq!(folded, super::support::oracle(&warmup_left, &warmup_right));
    let expected_count = match case.as_str() {
        "distinct" | "duplicates" => 10_000,
        "low" => 1,
        "mixed" => 8_000,
        _ => unreachable!(),
    };
    assert!(
        folded
            .iter()
            .all(|((_, count), weight)| *count == expected_count && *weight == 1)
    );
    let mut oracle_left = warmup_left;
    let mut oracle_right = warmup_right;
    let mut samples = Vec::with_capacity(data.measured);
    let mut input_rows = 0usize;
    for (index, (left_rows, right_rows)) in data.steps.iter().enumerate() {
        let left = batch(left_rows);
        let right = batch(right_rows);
        if index >= data.warmup {
            input_rows += left.len() + right.len();
        }
        let start = std::time::Instant::now();
        let result = match strategy.as_str() {
            "reference" => reference.as_mut().unwrap().apply(&left, &right).unwrap(),
            "candidate" => candidate.as_mut().unwrap().apply(&left, &right).unwrap(),
            "manual" => manual.as_mut().unwrap().apply(&left, &right).unwrap(),
            _ => panic!("unsupported strategy: {strategy}"),
        };
        let elapsed = start.elapsed().as_nanos();
        fold_output(&mut folded, &result);
        if index >= data.warmup {
            oracle_left.extend(left_rows.iter().copied());
            oracle_right.extend(right_rows.iter().copied());
            assert_eq!(folded, super::support::oracle(&oracle_left, &oracle_right));
        }
        assert!(
            folded
                .iter()
                .all(|((_, count), weight)| *count == expected_count && *weight == 1)
        );
        if index >= data.warmup {
            samples.push(elapsed);
        }
    }
    assert_eq!(samples.len(), data.measured);
    assert!(samples.iter().all(|sample| *sample > 0));
    samples.sort_unstable();
    let nearest_rank = |pct: usize| (pct * samples.len()).div_ceil(100).saturating_sub(1);
    let p50 = samples[nearest_rank(50)];
    let p99 = samples[nearest_rank(99)];
    let total_ns: u128 = samples.iter().sum();
    let memory = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmHWM:"))
                .and_then(|line| line.split_whitespace().nth(1)?.parse::<u64>().ok())
        });
    println!(
        "strategy,case,rep,input_rows,distinct_left,distinct_right,keys,steps,setup_ns,seed_ns,total_ns,p50_ns,p99_ns,vmhwm_kib"
    );
    println!(
        "{strategy},{case},{rep},{input_rows},{},{},{},{},{},{seed_ns},{total_ns},{p50},{p99},{}",
        data.left.len(),
        data.right.len(),
        if case == "low" { 1_000 } else { 1 },
        data.measured,
        setup_ns,
        memory.map_or_else(|| "NA".into(), |value| value.to_string())
    );
}
