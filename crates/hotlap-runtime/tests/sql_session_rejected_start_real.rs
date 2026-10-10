//! SQL retries retain a real, non-empty checkpoint after sink preflight rejects.

#[path = "sql_session_rejected_start/real_support.rs"]
mod support;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use hotlap::state::StateBackend;
use hotlap_runtime::runtime::source_checkpoint::{decode_sources, encode_sources};
use support::{SINK_TARGET, SessionFixture, Stats, declared_sql};

#[test]
fn rejected_sink_preflights_retry_same_session_and_restore_real_checkpoint() {
    let fixture = SessionFixture::new();
    let seed_stats = std::sync::Arc::new(Stats::default());
    let mut seed = fixture.open(
        &seed_stats,
        Some(SINK_TARGET.to_owned()),
        SINK_TARGET.to_owned(),
    );
    declared_sql(&mut seed);
    seed.sql("START;").unwrap();

    for expected_offset in 1..=7 {
        fixture.send_source_value(expected_offset);
        fixture.source_ack(expected_offset);
    }
    let seed_rows: BTreeMap<_, _> = (1..=7).map(|key| (key, 1)).collect();
    fixture.wait_for_remote_bag(&seed_rows);
    assert_eq!(seed.checkpoint().unwrap(), 1);
    seed.shutdown().unwrap();

    let saved_seed = fixture.read_checkpoint(1);
    assert_eq!(saved_seed.sources.entries[0].state.offsets[&0], 7);
    assert_eq!(
        saved_seed.sources.entries[0].physical_identity,
        support::SOURCE_IDENTITY
    );
    assert_eq!(
        saved_seed
            .sources
            .views
            .iter()
            .map(|view| view.name.as_str())
            .collect::<Vec<_>>(),
        vec!["mv"]
    );

    let retry_stats = std::sync::Arc::new(Stats::default());
    let described = std::sync::Arc::new(std::sync::Mutex::new(None));
    let actual = std::sync::Arc::new(std::sync::Mutex::new(SINK_TARGET.to_owned()));
    let mut retry = fixture.open_with_identity(&retry_stats, described.clone(), actual.clone());
    declared_sql(&mut retry);

    let no_description = rejected_start(retry.sql("START;"));
    assert!(
        no_description
            .to_string()
            .contains("cannot resolve durable target metadata"),
        "{no_description}"
    );
    assert_rejected_without_effects(&retry_stats, 0);

    *described.lock().unwrap() = Some("test/sql-real-retry/other-described-target".into());
    let described_mismatch = rejected_start(retry.sql("START;"));
    assert!(
        described_mismatch
            .to_string()
            .contains("sink descriptions do not match"),
        "{described_mismatch}"
    );
    assert_rejected_without_effects(&retry_stats, 0);

    *described.lock().unwrap() = Some(SINK_TARGET.to_owned());
    *actual.lock().unwrap() = "test/sql-real-retry/other-created-target".into();
    let created_mismatch = rejected_start(retry.sql("START;"));
    assert!(
        created_mismatch
            .to_string()
            .contains("differs from its preflight target metadata"),
        "{created_mismatch}"
    );
    assert_rejected_without_effects(&retry_stats, 1);

    *actual.lock().unwrap() = SINK_TARGET.to_owned();
    retry
        .sql("START;")
        .expect("same-session retry restores retained durable config");
    assert_eq!(*retry_stats.resumed.lock().unwrap(), vec![7]);
    assert_eq!(retry_stats.source_reads.load(Ordering::SeqCst), 1);
    assert_eq!(retry_stats.sink_creates.load(Ordering::SeqCst), 2);

    let restored_immediately = support::session_bag(&retry);
    assert_eq!(restored_immediately, seed_rows);
    assert_eq!(*fixture.remote_bag.lock().unwrap(), seed_rows);

    fixture.send_source_value(8);
    fixture.source_ack(8);
    let after_future_row: BTreeMap<_, _> = (1..=8).map(|key| (key, 1)).collect();
    fixture.wait_for_remote_bag(&after_future_row);

    assert_eq!(retry.checkpoint().unwrap(), 2);
    let saved_retry = fixture.read_checkpoint(2);
    let encoded_sources = fixture
        .backend
        .get(b"checkpoint/2/sources")
        .unwrap()
        .unwrap();
    let decoded_sources = decode_sources(&encoded_sources).unwrap();
    assert_eq!(saved_retry.sources.entries[0].state.offsets[&0], 8);
    assert_eq!(decoded_sources.entries[0].state.offsets[&0], 8);
    assert_eq!(*fixture.remote_bag.lock().unwrap(), after_future_row);
    retry.shutdown().unwrap();
}

#[test]
fn replay_safe_prepare_body_identity_mismatch_rejects_before_effects_and_retries() {
    let fixture = SessionFixture::new();
    let seed_stats = std::sync::Arc::new(Stats::default());
    let mut seed = fixture.open(
        &seed_stats,
        Some(SINK_TARGET.to_owned()),
        SINK_TARGET.to_owned(),
    );
    declared_sql(&mut seed);
    seed.sql("START;").unwrap();

    for expected_offset in 1..=7 {
        fixture.send_source_value(expected_offset);
        fixture.source_ack(expected_offset);
    }
    let seed_rows: BTreeMap<_, _> = (1..=7).map(|key| (key, 1)).collect();
    fixture.wait_for_remote_bag(&seed_rows);
    assert_eq!(seed.checkpoint().unwrap(), 1);
    seed.shutdown().unwrap();

    let original_sources = fixture
        .backend
        .get(b"checkpoint/1/sources")
        .unwrap()
        .unwrap();
    let engine = fixture
        .backend
        .get(b"checkpoint/1/engine")
        .unwrap()
        .unwrap();
    let participants = fixture
        .backend
        .get(b"checkpoint/1/participants")
        .unwrap()
        .unwrap();
    let mut changed = decode_sources(&original_sources).unwrap();
    changed.entries[0].physical_identity.push_str("-changed");
    let mut writer = fixture.backend.clone();
    writer.put(b"checkpoint/2/engine", engine).unwrap();
    writer
        .put(b"checkpoint/2/sources", encode_sources(&changed).unwrap())
        .unwrap();
    writer
        .put(b"checkpoint/2/participants", participants)
        .unwrap();
    writer
        .put(b"checkpoint/2/prepare", b"prepare-v1".to_vec())
        .unwrap();
    writer
        .put(b"checkpoint/reserved", 2_u64.to_be_bytes().to_vec())
        .unwrap();
    let pending_sources = fixture.backend.get(b"checkpoint/2/sources").unwrap();
    let evidence_before = fixture.backend.scan(b"").unwrap();

    let retry_stats = std::sync::Arc::new(Stats::default());
    let mut retry = fixture.open(
        &retry_stats,
        Some(SINK_TARGET.to_owned()),
        SINK_TARGET.to_owned(),
    );
    declared_sql(&mut retry);

    let error = rejected_start(retry.sql("START;"));
    assert!(
        error
            .to_string()
            .contains("source checkpoint and participant manifest disagree"),
        "{error}"
    );
    assert_rejected_without_effects(&retry_stats, 0);
    assert_eq!(fixture.backend.scan(b"").unwrap(), evidence_before);
    assert_eq!(
        fixture.backend.get(b"checkpoint/2/prepare").unwrap(),
        Some(b"prepare-v1".to_vec())
    );
    assert_eq!(
        fixture.backend.get(b"checkpoint/2/sources").unwrap(),
        pending_sources
    );

    let mut writer = fixture.backend.clone();
    writer
        .put(b"checkpoint/2/sources", original_sources)
        .unwrap();
    retry
        .sql("START;")
        .expect("same-session retry must retain config and use valid fallback");
    assert_eq!(*retry_stats.resumed.lock().unwrap(), vec![7]);
    assert_eq!(retry_stats.source_reads.load(Ordering::SeqCst), 1);
    assert_eq!(retry_stats.sink_creates.load(Ordering::SeqCst), 1);
    assert_eq!(support::session_bag(&retry), seed_rows);
    assert_eq!(*fixture.remote_bag.lock().unwrap(), seed_rows);

    fixture.send_source_value(8);
    fixture.source_ack(8);
    let after_future_row: BTreeMap<_, _> = (1..=8).map(|key| (key, 1)).collect();
    fixture.wait_for_remote_bag(&after_future_row);
    let checkpoint = retry.checkpoint().unwrap();
    assert!(checkpoint > 2, "recovery must not reuse pending id 2");
    assert_eq!(
        fixture.read_checkpoint(checkpoint).sources.entries[0]
            .state
            .offsets[&0],
        8
    );
    assert_eq!(*fixture.remote_bag.lock().unwrap(), after_future_row);
    retry.shutdown().unwrap();
}

fn assert_rejected_without_effects(stats: &Stats, creates: u32) {
    assert_eq!(stats.sink_creates.load(Ordering::SeqCst), creates);
    assert_eq!(stats.sink_writes.load(Ordering::SeqCst), 0);
    assert_eq!(stats.sink_commits.load(Ordering::SeqCst), 0);
    assert_eq!(stats.source_reads.load(Ordering::SeqCst), 0);
    assert!(stats.resumed.lock().unwrap().is_empty());
}

fn rejected_start<E>(result: Result<hotlap_runtime::QueryResult, E>) -> E {
    match result {
        Err(error) => error,
        Ok(_) => panic!("incompatible sink must reject START"),
    }
}
