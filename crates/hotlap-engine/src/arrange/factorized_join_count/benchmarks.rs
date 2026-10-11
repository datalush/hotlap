use super::{
    candidate::Candidate,
    manual::Manual,
    support::{Output, Reference, Row, batch, fold_output, output, schema},
};
use arrow::datatypes::{DataType, Field, Schema};

#[derive(Clone, Copy)]
struct WorkloadConfig {
    left_count: usize,
    right_count: usize,
    keys: usize,
    warmup: usize,
    measured: usize,
}

struct Workload {
    case: &'static str,
    left: Vec<Row>,
    right: Vec<Row>,
    steps: Vec<(Vec<Row>, Vec<Row>)>,
    warmup: usize,
    measured: usize,
    expected: i64,
    keys: usize,
}

struct PreparedWorkload {
    seed_left: hotlap_core::ZSetBatch,
    seed_right: hotlap_core::ZSetBatch,
    steps: Vec<(hotlap_core::ZSetBatch, hotlap_core::ZSetBatch)>,
}

fn prepare(data: &Workload) -> PreparedWorkload {
    PreparedWorkload {
        seed_left: batch(&data.left),
        seed_right: batch(&data.right),
        steps: data
            .steps
            .iter()
            .map(|(left, right)| (batch(left), batch(right)))
            .collect(),
    }
}

fn expected_output(data: &Workload, count: i64) -> Output {
    match data.case {
        "distinct" | "duplicates" if count == 0 => Output::new(),
        "distinct" | "duplicates" => [((Some(0), count), 1)].into(),
        "low" => (0..data.keys)
            .map(|key| ((Some(key as i64), 1), 1))
            .collect(),
        "mixed" => [((Some(0), data.expected), 1)].into(),
        _ => panic!("unsupported benchmark case: {}", data.case),
    }
}

fn workload(case: &str) -> Workload {
    let config = match case {
        "distinct" | "duplicates" => WorkloadConfig {
            left_count: 10_000,
            right_count: 1,
            keys: 1,
            warmup: 40,
            measured: 400,
        },
        "low" => WorkloadConfig {
            left_count: 1_000,
            right_count: 1_000,
            keys: 1_000,
            warmup: 40,
            measured: 400,
        },
        "mixed" => WorkloadConfig {
            left_count: 1_000,
            right_count: 8,
            keys: 1,
            warmup: 40,
            measured: 400,
        },
        _ => panic!("unsupported benchmark case: {case}"),
    };
    workload_with(case, config)
}

fn workload_with(case: &str, config: WorkloadConfig) -> Workload {
    assert!(config.left_count > 0 && config.right_count > 0 && config.keys > 0);
    assert!(config.warmup + config.measured > 0);
    let total_steps = config.warmup + config.measured;
    let (left, right, steps, expected) = match case {
        "distinct" => {
            assert_eq!(config.right_count, 1);
            assert_eq!(config.keys, 1);
            (
                (0..config.left_count)
                    .map(|p| (Some(0), Some(p as i64), 1))
                    .collect(),
                vec![(Some(0), Some(0), 1)],
                (0..total_steps)
                    .map(|n| {
                        let diff = if n % 2 == 0 { -1 } else { 1 };
                        (vec![], vec![(Some(0), Some(0), diff)])
                    })
                    .collect(),
                config.left_count as i64,
            )
        }
        "duplicates" => {
            assert_eq!(config.right_count, 1);
            assert_eq!(config.keys, 1);
            (
                vec![(Some(0), Some(0), config.left_count as i64)],
                vec![(Some(0), Some(0), 1)],
                (0..total_steps)
                    .map(|n| {
                        let diff = if n % 2 == 0 { -1 } else { 1 };
                        (vec![], vec![(Some(0), Some(0), diff)])
                    })
                    .collect(),
                config.left_count as i64,
            )
        }
        "low" => {
            assert_eq!(config.left_count, config.keys);
            assert_eq!(config.right_count, config.keys);
            assert!(total_steps <= config.keys, "low workload repeats a key");
            (
                (0..config.keys)
                    .map(|key| (Some(key as i64), Some(0), 1))
                    .collect(),
                (0..config.keys)
                    .map(|key| (Some(key as i64), Some(0), 1))
                    .collect(),
                (0..total_steps)
                    .map(|n| {
                        let key = n as i64;
                        (
                            vec![(Some(key), Some(0), -1), (Some(key), Some(1), 1)],
                            vec![],
                        )
                    })
                    .collect(),
                config.keys as i64,
            )
        }
        "mixed" => {
            assert_eq!(config.keys, 1);
            (
                (0..config.left_count)
                    .map(|p| (Some(0), Some(p as i64), 1))
                    .collect(),
                (0..config.right_count)
                    .map(|p| (Some(0), Some(p as i64), 1))
                    .collect(),
                (0..total_steps)
                    .map(|n| {
                        let old_l = if n == 0 {
                            0
                        } else {
                            config.left_count as i64 - 1 + n as i64
                        };
                        let old_r = if n == 0 {
                            0
                        } else {
                            config.right_count as i64 - 1 + n as i64
                        };
                        (
                            vec![
                                (Some(0), Some(old_l), -1),
                                (Some(0), Some(config.left_count as i64 + n as i64), 1),
                            ],
                            vec![
                                (Some(0), Some(old_r), -1),
                                (Some(0), Some(config.right_count as i64 + n as i64), 1),
                            ],
                        )
                    })
                    .collect(),
                (config.left_count * config.right_count) as i64,
            )
        }
        _ => panic!("unsupported benchmark case: {case}"),
    };
    Workload {
        case: match case {
            "distinct" => "distinct",
            "duplicates" => "duplicates",
            "low" => "low",
            "mixed" => "mixed",
            _ => unreachable!(),
        },
        left,
        right,
        steps,
        warmup: config.warmup,
        measured: config.measured,
        expected,
        keys: config.keys,
    }
}

#[test]
fn workload_shapes_and_boundaries_are_exact() {
    let distinct = workload("distinct");
    assert_eq!(
        distinct.left,
        (0..10_000)
            .map(|payload| (Some(0), Some(payload), 1))
            .collect::<Vec<_>>()
    );
    let duplicates = workload("duplicates");
    assert_eq!(duplicates.left, [(Some(0), Some(0), 10_000)]);
    assert_eq!(distinct.right, [(Some(0), Some(0), 1)]);
    assert_eq!(duplicates.right, [(Some(0), Some(0), 1)]);
    assert_eq!(distinct.expected, 10_000);
    assert_eq!(distinct.keys, 1);
    assert_eq!(distinct.steps.len(), 440);
    assert_eq!(distinct.steps[0].1, [(Some(0), Some(0), -1)]);
    assert_eq!(distinct.steps[1].1, [(Some(0), Some(0), 1)]);
    assert_eq!(distinct.steps, duplicates.steps);
    let low = workload("low");
    assert_eq!(low.left.len(), 1_000);
    assert_eq!(low.right.len(), 1_000);
    assert_eq!(low.expected, 1_000);
    assert_eq!(low.steps.len(), 440);
    assert_eq!(low.steps[39].0[0], (Some(39), Some(0), -1));
    assert_eq!(low.steps[40].0[0], (Some(40), Some(0), -1));
    for (n, (left, right)) in low.steps.iter().enumerate() {
        let key = Some(n as i64);
        assert_eq!(*left, [(key, Some(0), -1), (key, Some(1), 1)]);
        assert!(right.is_empty());
    }
    let mixed = workload("mixed");
    assert_eq!(
        mixed.steps[0].0,
        [(Some(0), Some(0), -1), (Some(0), Some(1_000), 1)]
    );
    assert_eq!(
        mixed.steps[0].1,
        [(Some(0), Some(0), -1), (Some(0), Some(8), 1)]
    );
    assert_eq!(mixed.expected, 8_000);
    assert_eq!(mixed.steps.len(), 440);
    for (n, (left, right)) in mixed.steps.iter().enumerate() {
        let old_left = if n == 0 { 0 } else { 999 + n as i64 };
        let old_right = if n == 0 { 0 } else { 7 + n as i64 };
        assert_eq!(
            *left,
            [
                (Some(0), Some(old_left), -1),
                (Some(0), Some(1_000 + n as i64), 1),
            ]
        );
        assert_eq!(
            *right,
            [
                (Some(0), Some(old_right), -1),
                (Some(0), Some(8 + n as i64), 1),
            ]
        );
    }
    for case in ["distinct", "duplicates", "low", "mixed"] {
        let load = workload(case);
        assert_eq!(load.steps.len(), 440);
        assert_eq!(load.warmup, 40);
        assert_eq!(load.measured, 400);
        assert!(load.expected > 0);
    }
}

fn assert_frame_schemas_and_deltas(
    candidate: &hotlap_core::ZSetBatch,
    reference: &hotlap_core::ZSetBatch,
    manual: &hotlap_core::ZSetBatch,
) {
    let expected_schema = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("count", DataType::Int64, true),
    ]);
    for frame in [candidate, reference, manual] {
        assert_eq!(frame.batch.schema().as_ref(), &expected_schema);
    }
    assert_eq!(output(candidate), output(reference));
    assert_eq!(output(manual), output(reference));
}

struct WorkloadGate {
    candidate: Candidate,
    reference: Reference,
    manual: Manual,
    candidate_folded: Output,
    reference_folded: Output,
    manual_folded: Output,
    history_left: Vec<Row>,
    history_right: Vec<Row>,
}

impl WorkloadGate {
    fn new(data: &Workload) -> Self {
        let mut candidate = Candidate::new(schema(), schema(), 0, 0).unwrap();
        let mut reference = Reference::new(0, 0);
        let mut manual = Manual::new(0, 0);
        let seed_left = batch(&data.left);
        let seed_right = batch(&data.right);
        let candidate_seed = candidate.apply(&seed_left, &seed_right).unwrap();
        let reference_seed = reference.apply(&seed_left, &seed_right).unwrap();
        let manual_seed = manual.apply(&seed_left, &seed_right).unwrap();
        assert_frame_schemas_and_deltas(&candidate_seed, &reference_seed, &manual_seed);

        let mut gate = Self {
            candidate,
            reference,
            manual,
            candidate_folded: Output::new(),
            reference_folded: Output::new(),
            manual_folded: Output::new(),
            history_left: data.left.clone(),
            history_right: data.right.clone(),
        };
        fold_output(&mut gate.candidate_folded, &candidate_seed);
        fold_output(&mut gate.reference_folded, &reference_seed);
        fold_output(&mut gate.manual_folded, &manual_seed);
        gate.assert_state(data, data.expected);
        gate
    }

    fn apply_step(&mut self, data: &Workload, index: usize) {
        let (left_rows, right_rows) = &data.steps[index];
        let left = batch(left_rows);
        let right = batch(right_rows);
        let candidate_delta = self.candidate.apply(&left, &right).unwrap();
        let reference_delta = self.reference.apply(&left, &right).unwrap();
        let manual_delta = self.manual.apply(&left, &right).unwrap();
        assert_frame_schemas_and_deltas(&candidate_delta, &reference_delta, &manual_delta);

        fold_output(&mut self.candidate_folded, &candidate_delta);
        fold_output(&mut self.reference_folded, &reference_delta);
        fold_output(&mut self.manual_folded, &manual_delta);
        self.history_left.extend(left_rows.iter().copied());
        self.history_right.extend(right_rows.iter().copied());
        let expected_count = match data.case {
            "distinct" | "duplicates" if index.is_multiple_of(2) => 0,
            _ => data.expected,
        };
        self.assert_state(data, expected_count);
    }

    fn assert_state(&self, data: &Workload, expected_count: i64) {
        let expected = expected_output(data, expected_count);
        let oracle = super::support::oracle(&self.history_left, &self.history_right);
        assert_eq!(oracle, expected);
        assert_eq!(self.candidate_folded, expected);
        assert_eq!(self.reference_folded, expected);
        assert_eq!(self.manual_folded, expected);
    }
}

#[test]
fn scaled_workloads_match_oracle_across_warmup_and_measurement_for_every_strategy() {
    let cases = [
        (
            "distinct",
            WorkloadConfig {
                left_count: 32,
                right_count: 1,
                keys: 1,
                warmup: 2,
                measured: 4,
            },
        ),
        (
            "duplicates",
            WorkloadConfig {
                left_count: 32,
                right_count: 1,
                keys: 1,
                warmup: 2,
                measured: 4,
            },
        ),
        (
            "low",
            WorkloadConfig {
                left_count: 8,
                right_count: 8,
                keys: 8,
                warmup: 2,
                measured: 4,
            },
        ),
        (
            "mixed",
            WorkloadConfig {
                left_count: 16,
                right_count: 4,
                keys: 1,
                warmup: 2,
                measured: 4,
            },
        ),
    ];
    for (case, config) in cases {
        let data = workload_with(case, config);
        let mut gate = WorkloadGate::new(&data);
        for index in 0..data.warmup {
            gate.apply_step(&data, index);
        }
        for index in data.warmup..data.steps.len() {
            gate.apply_step(&data, index);
        }
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
    let prepared = prepare(&data);
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
    let setup_ns = setup_start.elapsed().as_nanos();
    let (seed_output, seed_ns) = match strategy.as_str() {
        "candidate" => {
            let start = std::time::Instant::now();
            let out = candidate
                .as_mut()
                .unwrap()
                .apply(&prepared.seed_left, &prepared.seed_right)
                .unwrap();
            (out, start.elapsed().as_nanos())
        }
        "reference" => {
            let start = std::time::Instant::now();
            let out = reference
                .as_mut()
                .unwrap()
                .apply(&prepared.seed_left, &prepared.seed_right)
                .unwrap();
            (out, start.elapsed().as_nanos())
        }
        "manual" => {
            let start = std::time::Instant::now();
            let out = manual
                .as_mut()
                .unwrap()
                .apply(&prepared.seed_left, &prepared.seed_right)
                .unwrap();
            (out, start.elapsed().as_nanos())
        }
        _ => panic!("unsupported strategy: {strategy}"),
    };
    let mut warmup_output = Output::new();
    fold_output(&mut warmup_output, &seed_output);
    for (index, (left, right)) in prepared.steps.iter().take(data.warmup).enumerate() {
        let warm_result = match strategy.as_str() {
            "reference" => reference.as_mut().unwrap().apply(left, right).unwrap(),
            "candidate" => candidate.as_mut().unwrap().apply(left, right).unwrap(),
            "manual" => manual.as_mut().unwrap().apply(left, right).unwrap(),
            _ => unreachable!(),
        };
        fold_output(&mut warmup_output, &warm_result);
        let expected_count = match case.as_str() {
            "distinct" | "duplicates" => {
                if index % 2 == 0 {
                    0
                } else {
                    data.expected
                }
            }
            _ => data.expected,
        };
        assert_eq!(warmup_output, expected_output(&data, expected_count));
    }
    let mut folded = warmup_output;
    let mut samples = Vec::with_capacity(data.measured);
    let mut input_rows = 0usize;
    for (index, (left, right)) in prepared.steps.iter().skip(data.warmup).enumerate() {
        input_rows += left.len() + right.len();
        let start = std::time::Instant::now();
        let result = match strategy.as_str() {
            "reference" => reference.as_mut().unwrap().apply(left, right).unwrap(),
            "candidate" => candidate.as_mut().unwrap().apply(left, right).unwrap(),
            "manual" => manual.as_mut().unwrap().apply(left, right).unwrap(),
            _ => panic!("unsupported strategy: {strategy}"),
        };
        fold_output(&mut folded, &result);
        let elapsed = start.elapsed().as_nanos();
        let expected_count = match case.as_str() {
            "distinct" | "duplicates" => {
                if index % 2 == 0 {
                    0
                } else {
                    data.expected
                }
            }
            _ => data.expected,
        };
        assert_eq!(folded, expected_output(&data, expected_count));
        samples.push(elapsed);
    }
    assert_eq!(samples.len(), data.measured);
    assert!(samples.iter().all(|sample| *sample > 0));
    samples.sort_unstable();
    let nearest_rank = |pct: usize| (pct * samples.len()).div_ceil(100).saturating_sub(1);
    let p50 = samples[nearest_rank(50)];
    let p99 = samples[nearest_rank(99)];
    let total_ns: u128 = samples.iter().sum();
    let distinct_left = data
        .left
        .iter()
        .map(|(key, payload, _)| (key, payload))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let distinct_right = data
        .right
        .iter()
        .map(|(key, payload, _)| (key, payload))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
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
        distinct_left,
        distinct_right,
        if case == "low" { 1_000 } else { 1 },
        data.measured,
        setup_ns,
        memory.map_or_else(|| "NA".into(), |value| value.to_string())
    );
}
