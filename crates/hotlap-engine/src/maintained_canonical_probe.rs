use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::i256;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::RowConverter;
use hotlap_core::plan::{AggFunc, AggSpec};
use hotlap_core::snapshot::GroupEntry;

use self::output::{canonical_rows, rows};
use self::state::{
    CertificateSources, Contribution, RawJoinDeltaBounds, SourceStats, add_group, batch_sign,
    certify, contribution, stage_stats,
};
use crate::arrange::KeyedArrangement;

#[path = "maintained_canonical_probe_output.rs"]
mod output;

#[path = "maintained_canonical_probe_state.rs"]
mod state;
use crate::keys::KeyConverter;
use crate::ops::group_aggregate::columns::render;
use crate::ops::group_aggregate::materialize::{Change, materialize};
use crate::ops::{GroupAggregate, Join};
use crate::{EngineError, ZSetBatch};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("tag", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn batch(rows: &[(Option<i64>, i64, Option<i64>, i64)]) -> ZSetBatch {
    let schema = schema();
    let keys = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let tags = rows.iter().map(|row| row.1).collect::<Vec<_>>();
    let values = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    let diffs = rows.iter().map(|row| row.3).collect::<Vec<_>>();
    let record = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(tags)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();
    ZSetBatch::new(record, Arc::new(Int64Array::from(diffs))).unwrap()
}

#[test]
#[ignore = "paired operator-cut timing experiment; run explicitly with --ignored --nocapture"]
fn canonical_probe_paired_operator_experiment() {
    #[derive(Clone, Copy)]
    struct Case {
        fanout: usize,
        changed_keys: usize,
        mode: &'static str,
    }
    let cases = [1, 16, 64, 256]
        .into_iter()
        .flat_map(|fanout| {
            [1, 16].into_iter().flat_map(move |changed_keys| {
                ["append", "delete"].into_iter().map(move |mode| Case {
                    fanout,
                    changed_keys,
                    mode,
                })
            })
        })
        .collect::<Vec<_>>();
    let mut report = Vec::new();
    for case in cases {
        let steps = 64usize;
        let left_pool = 68usize;
        let mut left_seed_rows = Vec::new();
        let mut right_seed_rows = Vec::new();
        for key in 0..128i64 {
            for row in 0..(left_pool + 4) {
                left_seed_rows.push((
                    Some(key),
                    row as i64,
                    (row % 5 != 0).then_some((row as i64 % 13) + 1),
                    1,
                ));
            }
            for row in 0..case.fanout {
                right_seed_rows.push((Some(key), row as i64, Some((row as i64 % 7) + 1), 1));
            }
        }
        let left_seed = batch(&left_seed_rows);
        let right_seed = batch(&right_seed_rows);
        let mut deltas = Vec::with_capacity(steps);
        let mut tags = vec![left_pool as i64 + 4; 128];
        for step in 0..steps {
            let mut changes = Vec::with_capacity(case.changed_keys);
            for i in 0..case.changed_keys {
                let serial = step * case.changed_keys + i;
                let key = (serial % 128) as i64;
                let (tag, diff) = if case.mode == "append" {
                    let tag = tags[key as usize];
                    tags[key as usize] += 1;
                    (tag, 1)
                } else {
                    let payload_index = serial / 128;
                    (payload_index as i64, -1)
                };
                let value = if case.mode == "delete" {
                    (tag % 5 != 0).then_some((tag % 13) + 1)
                } else {
                    Some((step % 19) as i64 + 2)
                };
                changes.push((Some(key), tag, value, diff));
            }
            deltas.push(batch(&changes));
        }
        let mut measured = Vec::new();
        for trial in 0..4 {
            let mut candidate = CanonicalJoinProbe::new();
            candidate.apply(&left_seed, &right_seed).unwrap();
            let (mut dense_join, mut dense_group) = dense();
            let joined_seed = dense_join.apply(&left_seed, &right_seed).unwrap();
            dense_group.apply(&joined_seed).unwrap();
            drop(joined_seed);
            let empty = batch(&[]);
            let mut candidate_out = Vec::with_capacity(steps);
            let mut dense_out = Vec::with_capacity(steps);
            let (candidate_ns, dense_ns) = if trial % 2 == 0 {
                let start = Instant::now();
                for delta in &deltas {
                    candidate_out.push(candidate.apply(delta, &empty).unwrap());
                }
                let candidate_ns = start.elapsed().as_nanos();
                let start = Instant::now();
                for delta in &deltas {
                    let joined = dense_join.apply(delta, &empty).unwrap();
                    dense_out.push(dense_group.apply(&joined).unwrap());
                }
                (candidate_ns, start.elapsed().as_nanos())
            } else {
                let start = Instant::now();
                for delta in &deltas {
                    let joined = dense_join.apply(delta, &empty).unwrap();
                    dense_out.push(dense_group.apply(&joined).unwrap());
                }
                let dense_ns = start.elapsed().as_nanos();
                let start = Instant::now();
                for delta in &deltas {
                    candidate_out.push(candidate.apply(delta, &empty).unwrap());
                }
                (start.elapsed().as_nanos(), dense_ns)
            };
            for (actual, expected) in candidate_out.iter().zip(&dense_out) {
                assert_eq!(actual.schema(), expected.schema());
                assert_eq!(canonical_rows(actual), canonical_rows(expected));
            }
            if trial > 0 {
                measured.push((candidate_ns, dense_ns));
            }
            drop(candidate_out);
            drop(dense_out);
            drop(candidate);
            drop(dense_join);
            drop(dense_group);
        }
        let mut candidate_times = measured.iter().map(|v| v.0).collect::<Vec<_>>();
        let mut dense_times = measured.iter().map(|v| v.1).collect::<Vec<_>>();
        candidate_times.sort_unstable();
        dense_times.sort_unstable();
        report.push(serde_json::json!({"fanout":case.fanout,"changed_keys":case.changed_keys,"mode":case.mode,"steps":steps,"timed_trials":3,"candidate_total_ns_median":candidate_times[1],"dense_total_ns_median":dense_times[1],"candidate_median_ns_per_push":candidate_times[1] / steps as u128,"dense_median_ns_per_push":dense_times[1] / steps as u128,"all_trials_certified":true,"output_parity":"all 64 full typed Arrow changelogs matched each trial"}));
        drop(left_seed);
        drop(right_seed);
    }
    println!(
        "CANONICAL_PAIRED {}",
        serde_json::to_string_pretty(&report).unwrap()
    );
}

#[test]
#[ignore = "fresh-process RSS probe; set CKYG_MEMORY_MODE=candidate|dense"]
fn canonical_probe_process_rss_experiment() {
    use std::hint::black_box;
    use std::{env, fs};

    let mode = env::var("CKYG_MEMORY_MODE").expect("CKYG_MEMORY_MODE is required");
    let mut parts = mode.split(':');
    let operator = parts.next().unwrap();
    let update_kind = parts.next().unwrap_or("append");
    assert!(operator == "candidate" || operator == "dense");
    assert!(update_kind == "append" || update_kind == "delete");
    let fanout = 256usize;
    let changed_keys = 16usize;
    let steps = 64usize;
    let left_pool = 68usize;
    let mut left_rows = Vec::new();
    let mut right_rows = Vec::new();
    for key in 0..128i64 {
        for row in 0..(left_pool + 4) {
            left_rows.push((
                Some(key),
                row as i64,
                (row % 5 != 0).then_some((row % 13 + 1) as i64),
                1,
            ));
        }
        for row in 0..fanout {
            right_rows.push((Some(key), row as i64, Some((row % 7 + 1) as i64), 1));
        }
    }
    let left_seed = batch(&left_rows);
    let right_seed = batch(&right_rows);
    let mut deltas = Vec::new();
    let mut next = vec![left_pool as i64 + 4; 128];
    for step in 0..steps {
        let changes = (0..changed_keys)
            .map(|i| {
                let key = ((step * changed_keys + i) % 128) as i64;
                if update_kind == "delete" {
                    let tag = ((step * changed_keys + i) / 128) as i64;
                    return (Some(key), tag, (tag % 5 != 0).then_some(tag % 13 + 1), -1);
                }
                let tag = next[key as usize];
                next[key as usize] += 1;
                (Some(key), tag, Some((step % 19 + 2) as i64), 1)
            })
            .collect::<Vec<_>>();
        deltas.push(batch(&changes));
    }
    if operator == "candidate" {
        let mut state = CanonicalJoinProbe::new();
        state.apply(&left_seed, &right_seed).unwrap();
        let seed_rows = left_seed.len() + right_seed.len();
        drop(left_seed);
        drop(right_seed);
        let setup = rss_kib(&fs::read_to_string("/proc/self/status").unwrap());
        let empty = batch(&[]);
        for delta in &deltas {
            state.apply(delta, &empty).unwrap();
        }
        let after = rss_kib(&fs::read_to_string("/proc/self/status").unwrap());
        black_box((&state.left.rows, &state.right.rows, &state.output_state));
        println!(
            "CANONICAL_RSS {{\"mode\":\"candidate\",\"setup_vm_rss_kib\":{setup},\"after_updates_vm_rss_kib\":{after},\"seed_arrow_rows\":{seed_rows},\"keys\":128,\"fanout\":256,\"changed_keys\":16,\"steps\":64,\"update_kind\":\"{update_kind}\",\"scope\":\"whole lib-test process RSS from procfs; not allocator live bytes\"}}"
        );
    } else {
        let mut join = Join::new(&[0], &[0]);
        let mut group = dense().1;
        let seeded = join.apply(&left_seed, &right_seed).unwrap();
        group.apply(&seeded).unwrap();
        let seed_rows = left_seed.len() + right_seed.len();
        drop(seeded);
        drop(left_seed);
        drop(right_seed);
        let setup = rss_kib(&fs::read_to_string("/proc/self/status").unwrap());
        let empty = batch(&[]);
        for delta in &deltas {
            let joined = join.apply(delta, &empty).unwrap();
            group.apply(&joined).unwrap();
        }
        let after = rss_kib(&fs::read_to_string("/proc/self/status").unwrap());
        black_box((&join, &group));
        println!(
            "CANONICAL_RSS {{\"mode\":\"dense\",\"setup_vm_rss_kib\":{setup},\"after_updates_vm_rss_kib\":{after},\"seed_arrow_rows\":{seed_rows},\"keys\":128,\"fanout\":256,\"changed_keys\":16,\"steps\":64,\"update_kind\":\"{update_kind}\",\"scope\":\"whole lib-test process RSS from procfs; not allocator live bytes\"}}"
        );
    }
}

fn rss_kib(status: &str) -> usize {
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .expect("VmRSS in /proc/self/status")
}

struct CanonicalSource {
    rows: KeyedArrangement,
    key_converter: KeyConverter,
}

impl CanonicalSource {
    fn new() -> Self {
        let schema = schema();
        Self {
            rows: KeyedArrangement::new(schema.clone(), &[0]).unwrap(),
            key_converter: KeyConverter::new(schema.as_ref(), &[0]).unwrap(),
        }
    }

    fn apply(&mut self, changes: &ZSetBatch) -> Result<(), EngineError> {
        let (staged, _) = self
            .rows
            .stage_batch_for_test(changes, &self.key_converter)?;
        self.rows.commit_batch_for_test(staged);
        Ok(())
    }

    fn logical_rows(&self) -> Result<ZSetBatch, EngineError> {
        self.rows.to_zset()
    }
}

struct CanonicalJoinProbe {
    left: CanonicalSource,
    right: CanonicalSource,
    left_stats: std::collections::HashMap<Vec<u8>, SourceStats>,
    right_stats: std::collections::HashMap<Vec<u8>, SourceStats>,
    contribution: std::collections::HashMap<Vec<u8>, Contribution>,
    output_state: std::collections::HashMap<Vec<u8>, GroupEntry>,
    output_converter: RowConverter,
    output_schema: SchemaRef,
}

impl CanonicalJoinProbe {
    fn new() -> Self {
        Self {
            left: CanonicalSource::new(),
            right: CanonicalSource::new(),
            left_stats: Default::default(),
            right_stats: Default::default(),
            contribution: Default::default(),
            output_state: Default::default(),
            output_converter: crate::keys::converter_for(schema().as_ref(), &[0]).unwrap(),
            output_schema: schema(),
        }
    }

    fn apply(
        &mut self,
        left_delta: &ZSetBatch,
        right_delta: &ZSetBatch,
    ) -> Result<ZSetBatch, EngineError> {
        let (staged_left_rows, left_decoded) = self
            .left
            .rows
            .stage_batch_for_test(left_delta, &self.left.key_converter)?;
        let (staged_right_rows, right_decoded) = self
            .right
            .rows
            .stage_batch_for_test(right_delta, &self.right.key_converter)?;
        let left_stats = stage_stats(left_delta, &self.left_stats, &left_decoded)?;
        let right_stats = stage_stats(right_delta, &self.right_stats, &right_decoded)?;
        let left_sign = batch_sign(left_delta);
        let right_sign = batch_sign(right_delta);
        let mut touched = left_stats.touched.clone();
        touched.extend(right_stats.touched.iter().cloned());
        let mut contribution_changes = Vec::new();
        let mut output_changes = Vec::new();
        let mut changes = Vec::new();
        for key in touched {
            let old = self.contribution.get(&key).copied().unwrap_or_default();
            let mut staged_group = std::collections::HashMap::new();
            if let Some(entry) = self.output_state.get(&key) {
                staged_group.insert(key.clone(), entry.clone());
            }
            if old.count != i256::ZERO {
                add_group(&mut staged_group, &key, old, -1, &self.output_converter)?;
            }
            let l = left_stats
                .by_key
                .get(&key)
                .or_else(|| self.left_stats.get(&key))
                .copied()
                .unwrap_or_default();
            let r = right_stats
                .by_key
                .get(&key)
                .or_else(|| self.right_stats.get(&key))
                .copied()
                .unwrap_or_default();
            let old_l = self.left_stats.get(&key).copied().unwrap_or_default();
            let old_r = self.right_stats.get(&key).copied().unwrap_or_default();
            certify(
                CertificateSources {
                    left: old_l,
                    right: old_r,
                },
                RawJoinDeltaBounds {
                    left_abs: left_stats
                        .raw_abs_weight
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    left_nonnull_abs: left_stats
                        .raw_abs_nonnull
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    right_abs: right_stats
                        .raw_abs_weight
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    left_sum_abs: left_stats
                        .raw_abs_sum
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    left_sign,
                    right_sign,
                },
                CertificateSources { left: l, right: r },
            )?;
            let new = contribution(l, r)?;
            let next_contribution = if new.count == i256::ZERO {
                None
            } else {
                add_group(&mut staged_group, &key, new, 1, &self.output_converter)?;
                Some(new)
            };
            let old_entry = self.output_state.get(&key).cloned();
            let new_entry = staged_group.remove(&key);
            if old_entry != new_entry {
                changes.push(Change {
                    key: key.clone(),
                    previous: old_entry.map(|entry| render(&entry.values)).transpose()?,
                    current: new_entry
                        .clone()
                        .map(|entry| render(&entry.values))
                        .transpose()?,
                });
            }
            contribution_changes.push((key.clone(), next_contribution));
            output_changes.push((key, new_entry));
        }
        let output = materialize(
            &[0],
            &[
                AggSpec::count(),
                AggSpec {
                    func: AggFunc::Count,
                    input: Some(2),
                },
                AggSpec::sum(2),
            ],
            Some(&self.output_schema),
            Some(&self.output_converter),
            None,
            &changes,
        )?;
        self.left.rows.commit_batch_for_test(staged_left_rows);
        self.right.rows.commit_batch_for_test(staged_right_rows);
        for (key, value) in left_stats.by_key {
            if value.support == 0 {
                self.left_stats.remove(&key);
            } else {
                self.left_stats.insert(key, value);
            }
        }
        for (key, value) in right_stats.by_key {
            if value.support == 0 {
                self.right_stats.remove(&key);
            } else {
                self.right_stats.insert(key, value);
            }
        }
        for (key, value) in contribution_changes {
            if let Some(value) = value {
                self.contribution.insert(key, value);
            } else {
                self.contribution.remove(&key);
            }
        }
        for (key, value) in output_changes {
            if let Some(value) = value {
                self.output_state.insert(key, value);
            } else {
                self.output_state.remove(&key);
            }
        }
        Ok(output)
    }
}

fn dense() -> (Join, GroupAggregate) {
    (
        Join::new(&[0], &[0]),
        GroupAggregate::new(
            &[0],
            vec![
                AggSpec::count(),
                AggSpec {
                    func: AggFunc::Count,
                    input: Some(2),
                },
                AggSpec::sum(2),
            ],
        ),
    )
}

#[test]
fn canonical_arrangement_source_replays_arrow_deltas_against_incremental_dense() {
    let mut source = CanonicalJoinProbe::new();
    let (mut join, mut aggregate) = dense();
    let left_seed = batch(&[(Some(7), 10, None, 2), (None, 20, Some(0), 1)]);
    let right_seed = batch(&[(Some(7), 30, Some(4), 1), (None, 40, None, 1)]);
    let source_seed = source.apply(&left_seed, &right_seed).unwrap();
    let oracle_seed_join = join.apply(&left_seed, &right_seed).unwrap();
    let oracle_seed = aggregate.apply(&oracle_seed_join).unwrap();
    assert_eq!(source_seed.schema(), oracle_seed.schema());
    assert_eq!(canonical_rows(&source_seed), canonical_rows(&oracle_seed));
    let mut saw_null_sum = rows(&source_seed)
        .iter()
        .any(|row| row[0] == Some(7) && row[3].is_none());
    let mut saw_zero_sum = false;

    for (left_delta, right_delta) in [
        (batch(&[(Some(7), 11, Some(5), 1)]), batch(&[])),
        (
            batch(&[(Some(7), 12, Some(3), 1)]),
            batch(&[(Some(7), 31, Some(6), 1)]),
        ),
        (batch(&[(Some(7), 11, Some(5), -1)]), batch(&[])),
        (batch(&[(Some(7), 12, Some(3), -1)]), batch(&[])),
        (batch(&[]), batch(&[(Some(7), 31, Some(6), -1)])),
        (batch(&[(Some(7), 10, None, -1)]), batch(&[])),
        (batch(&[(Some(7), 10, Some(0), 1)]), batch(&[])),
        (batch(&[(Some(7), 10, Some(0), -1)]), batch(&[])),
        (batch(&[(Some(7), 10, None, 1)]), batch(&[])),
        (batch(&[]), batch(&[(None, 40, None, -1)])),
    ] {
        let actual = source.apply(&left_delta, &right_delta).unwrap();
        let joined = join.apply(&left_delta, &right_delta).unwrap();
        let expected = aggregate.apply(&joined).unwrap();
        assert_eq!(actual.schema(), expected.schema());
        assert_eq!(canonical_rows(&actual), canonical_rows(&expected));
        saw_null_sum |= rows(&actual)
            .iter()
            .any(|row| row[0] == Some(7) && row[3].is_none());
        saw_zero_sum |= rows(&actual)
            .iter()
            .any(|row| row[0] == Some(7) && row[3] == Some(0));
    }
    assert!(saw_null_sum && saw_zero_sum);
    assert_eq!(
        source.output_state.len(),
        1,
        "last right row retraction removes its group"
    );
}

#[test]
fn canonical_probe_late_right_overflow_does_not_publish_left_or_summary() {
    let mut source = CanonicalJoinProbe::new();
    source
        .apply(&batch(&[]), &batch(&[(Some(9), 90, Some(4), i64::MAX)]))
        .unwrap();
    let left_before = rows(&source.left.logical_rows().unwrap());
    let right_before = rows(&source.right.logical_rows().unwrap());
    let left_stats_before = source.left_stats.clone();
    let right_stats_before = source.right_stats.clone();
    let contribution_before = source.contribution.clone();
    let output_before = source.output_state.clone();
    let result = source.apply(
        &batch(&[(Some(9), 91, Some(5), 1)]),
        &batch(&[(Some(9), 90, Some(4), 1)]),
    );
    assert!(matches!(result, Err(EngineError::NumericOverflow(_))));
    assert_eq!(rows(&source.left.logical_rows().unwrap()), left_before);
    assert_eq!(rows(&source.right.logical_rows().unwrap()), right_before);
    assert_eq!(source.left_stats, left_stats_before);
    assert_eq!(source.right_stats, right_stats_before);
    assert_eq!(source.contribution, contribution_before);
    assert_eq!(source.output_state, output_before);

    let mixed = batch(&[(Some(3), 31, Some(1), 1), (Some(3), 32, Some(2), -1)]);
    assert!(matches!(
        source.apply(&mixed, &batch(&[])),
        Err(EngineError::Unsupported(_))
    ));
    assert_eq!(rows(&source.left.logical_rows().unwrap()), left_before);
    assert_eq!(rows(&source.right.logical_rows().unwrap()), right_before);
}

#[test]
fn canonical_count_prefix_certificate_rejects_dense_overflow_before_publish() {
    let n = (i64::MAX - 1) / 2;
    let mut candidate = CanonicalJoinProbe::new();
    let left_seed = batch(&[(Some(1), 10, None, n)]);
    let right_seed = batch(&[(Some(1), 20, None, 2)]);
    candidate.apply(&left_seed, &right_seed).unwrap();

    let mut dense_join = Join::new(&[0], &[0]);
    let mut dense_group = GroupAggregate::new(&[0], vec![AggSpec::count()]);
    let seed_joined = dense_join.apply(&left_seed, &right_seed).unwrap();
    dense_group.apply(&seed_joined).unwrap();

    let left_delta = batch(&[(Some(1), 0, None, 2)]);
    let right_delta = batch(&[(Some(1), 20, None, -1)]);
    let dense_delta = dense_join.apply(&left_delta, &right_delta).unwrap();
    let dense_result = dense_group.apply(&dense_delta);
    assert!(
        matches!(&dense_result, Err(EngineError::Infrastructure(message)) if message.contains("group row count overflowed")),
        "dense delta={:?}, result={:?}",
        rows(&dense_delta),
        dense_result.as_ref().err()
    );

    let left_before = rows(&candidate.left.logical_rows().unwrap());
    let right_before = rows(&candidate.right.logical_rows().unwrap());
    let left_stats_before = candidate.left_stats.clone();
    let right_stats_before = candidate.right_stats.clone();
    let memo_before = candidate.contribution.clone();
    let output_before = candidate.output_state.clone();
    assert!(matches!(
        candidate.apply(&left_delta, &right_delta),
        Err(EngineError::Unsupported(_))
    ));
    assert_eq!(rows(&candidate.left.logical_rows().unwrap()), left_before);
    assert_eq!(rows(&candidate.right.logical_rows().unwrap()), right_before);
    assert_eq!(candidate.left_stats, left_stats_before);
    assert_eq!(candidate.right_stats, right_stats_before);
    assert_eq!(candidate.contribution, memo_before);
    assert_eq!(candidate.output_state, output_before);
}

#[test]
fn canonical_probe_reclaims_zero_support_source_and_derived_entries_across_cycles() {
    let mut candidate = CanonicalJoinProbe::new();
    for cycle in 0..2i64 {
        let left_rows = (0..64i64)
            .map(|tag| (Some(tag % 8), cycle * 1_000 + tag, None, 1))
            .collect::<Vec<_>>();
        let right_rows = (0..64i64)
            .map(|tag| (Some(tag % 8), cycle * 1_000 + tag + 100, None, 1))
            .collect::<Vec<_>>();
        candidate
            .apply(&batch(&left_rows), &batch(&right_rows))
            .unwrap();
        assert_eq!(candidate.left_stats.len(), 8);
        assert_eq!(candidate.right_stats.len(), 8);

        let left_deletes = left_rows
            .iter()
            .map(|(key, tag, value, _)| (*key, *tag, *value, -1))
            .collect::<Vec<_>>();
        candidate.apply(&batch(&left_deletes), &batch(&[])).unwrap();
        let right_deletes = right_rows
            .iter()
            .map(|(key, tag, value, _)| (*key, *tag, *value, -1))
            .collect::<Vec<_>>();
        candidate
            .apply(&batch(&[]), &batch(&right_deletes))
            .unwrap();

        assert!(candidate.left.rows.is_empty());
        assert!(candidate.right.rows.is_empty());
        assert!(candidate.left_stats.is_empty());
        assert!(candidate.right_stats.is_empty());
        assert!(candidate.contribution.is_empty());
        assert!(candidate.output_state.is_empty());
    }
}

#[test]
fn canonical_source_failed_overflow_keeps_real_arrangement_state_unchanged() {
    let mut source = CanonicalSource::new();
    let initial = batch(&[(Some(1), 1, Some(2), i64::MAX)]);
    source.apply(&initial).unwrap();
    let before = source.logical_rows().unwrap();
    let invalid = batch(&[(Some(1), 1, Some(2), 1)]);
    assert!(matches!(
        source.apply(&invalid),
        Err(EngineError::NumericOverflow(_))
    ));
    assert_eq!(rows(&source.logical_rows().unwrap()), rows(&before));
}
