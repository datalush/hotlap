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
