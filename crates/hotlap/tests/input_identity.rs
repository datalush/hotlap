//! Input identity: explicit ids must not alias or silently renumber.

use hotlap::{Hotlap, InputId};
use hotlap_engine::EngineCore;

fn open() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

#[test]
fn explicit_input_identity_rejects_aliasing() {
    let mut core = open();
    core.register_input_with_id("a", InputId(4)).unwrap();
    assert!(core.register_input_with_id("b", InputId(4)).is_err());
    assert!(core.register_input_with_id("a", InputId(5)).is_err());
    core.register_input("next").unwrap();
    let snapshot = core.checkpoint().unwrap();
    assert!(snapshot.inputs.iter().any(|input| input.id == InputId(5)));
}

#[test]
fn automatic_ids_follow_the_highest_explicit_id() {
    let mut core = open();
    core.register_input_with_id("a", InputId(2)).unwrap();
    core.register_input_with_id("b", InputId(9)).unwrap();
    core.register_input("auto").unwrap();
    let snapshot = core.checkpoint().unwrap();
    // Auto assignment starts past the highest explicit id, not past the count.
    assert!(snapshot.inputs.iter().any(|input| input.id == InputId(10)));
    assert_eq!(snapshot.inputs.len(), 3);
}

#[test]
fn failed_explicit_registration_leaves_no_state() {
    let mut core = open();
    core.register_input_with_id("a", InputId(1)).unwrap();
    assert!(core.register_input_with_id("b", InputId(1)).is_err());
    assert!(core.register_input_with_id("a", InputId(7)).is_err());
    // Only "a" is registered: no partial insert burned an id or a name.
    let snapshot = core.checkpoint().unwrap();
    assert_eq!(snapshot.inputs.len(), 1);
}
