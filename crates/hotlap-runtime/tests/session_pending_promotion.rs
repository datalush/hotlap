//! Public SQL recovery promotes the selected pending view registry.

#[path = "session_pending_promotion/fixture.rs"]
mod fixture;

use hotlap::state::StateBackend;
use hotlap_runtime::Session;

use fixture::{Progress, SharedBackend, config, declare, pending_body, rows, seed};

#[test]
fn session_promotes_pending_registry_and_restores_output_and_offset() {
    let backend = SharedBackend::default();
    seed(&backend, &[("a", 1)], 3);

    let pending_backend = SharedBackend::default();
    seed(&pending_backend, &[("a", 1), ("b", 2)], 4);
    pending_body(&backend, &pending_backend);

    let progress = Progress::default();
    let mut session = Session::open(config(&backend, progress.clone())).unwrap();
    declare(&mut session, &[("a", 1), ("b", 2)]);
    session.sql("START;").unwrap();

    assert_eq!(
        rows(&session.snapshot("a").unwrap()),
        (vec![vec![1]], vec![2])
    );
    assert_eq!(
        rows(&session.snapshot("b").unwrap()),
        (vec![vec![2]], vec![2])
    );
    assert_eq!(progress.resumed(), Some(4));
    assert!(backend.get(b"checkpoint/2/valid").unwrap().is_some());
    assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
    session.shutdown().unwrap();
}

#[test]
fn unknown_prepare_marker_rejects_without_mutating_pending_evidence() {
    for marker in [
        Vec::new(),
        b"prepare".to_vec(),
        b"unrecognized-phase".to_vec(),
    ] {
        let mut backend = SharedBackend::default();
        seed(&backend, &[("a", 1)], 3);
        let pending_backend = SharedBackend::default();
        seed(&pending_backend, &[("a", 1), ("b", 2)], 4);
        pending_body(&backend, &pending_backend);
        backend.delete(b"checkpoint/2/commit").unwrap();
        backend
            .put(b"checkpoint/2/prepare", marker.clone())
            .unwrap();
        let before_engine = backend.get(b"checkpoint/2/engine").unwrap();
        let before_sources = backend.get(b"checkpoint/2/sources").unwrap();
        let progress = Progress::default();
        let mut session = Session::open(config(&backend, progress.clone())).unwrap();
        declare(&mut session, &[("a", 1), ("b", 2)]);

        let error = match session.sql("START;") {
            Ok(_) => panic!("invalid phase {marker:?} must not promote"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("interrupted checkpoint"));
        assert_eq!(
            progress.resumed(),
            None,
            "preflight must precede source effects"
        );
        assert_eq!(backend.get(b"checkpoint/2/valid").unwrap(), None);
        assert_eq!(backend.get(b"checkpoint/2/commit").unwrap(), None);
        assert_eq!(backend.get(b"checkpoint/2/prepare").unwrap(), Some(marker));
        assert_eq!(backend.get(b"checkpoint/2/engine").unwrap(), before_engine);
        assert_eq!(
            backend.get(b"checkpoint/2/sources").unwrap(),
            before_sources
        );
        session.shutdown().unwrap();
    }
}

#[test]
fn noncanonical_commit_marker_rejects_without_mutating_pending_evidence() {
    let mut backend = SharedBackend::default();
    seed(&backend, &[("a", 1)], 3);
    let pending_backend = SharedBackend::default();
    seed(&pending_backend, &[("a", 1), ("b", 2)], 4);
    pending_body(&backend, &pending_backend);
    backend.put(b"checkpoint/2/commit", b"0".to_vec()).unwrap();
    let before_engine = backend.get(b"checkpoint/2/engine").unwrap();
    let before_sources = backend.get(b"checkpoint/2/sources").unwrap();
    let progress = Progress::default();
    let mut session = Session::open(config(&backend, progress.clone())).unwrap();
    declare(&mut session, &[("a", 1), ("b", 2)]);

    let error = match session.sql("START;") {
        Ok(_) => panic!("noncanonical commit marker must not promote"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("interrupted checkpoint"));
    assert_eq!(progress.resumed(), None);
    assert_eq!(backend.get(b"checkpoint/2/valid").unwrap(), None);
    assert_eq!(
        backend.get(b"checkpoint/2/commit").unwrap(),
        Some(b"0".to_vec())
    );
    assert_eq!(backend.get(b"checkpoint/2/engine").unwrap(), before_engine);
    assert_eq!(
        backend.get(b"checkpoint/2/sources").unwrap(),
        before_sources
    );
    session.shutdown().unwrap();
}
