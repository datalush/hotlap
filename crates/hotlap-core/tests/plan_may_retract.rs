//! Tests for plan changelog classification.

use hotlap_core::InputId;
use hotlap_core::plan::{AggSpec, Plan};

#[test]
fn final_windows_are_append_only_but_group_aggregates_can_retract() {
    let source = Plan::Source(InputId(0));
    let window = Plan::TumbleCount {
        input: Box::new(source.clone()),
        key: vec![0],
        time_col: 1,
        size: 10,
    };
    let grouped = Plan::GroupAggregate {
        input: Box::new(source),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };

    assert!(!hotlap_core::plan::may_retract(&window));
    assert!(hotlap_core::plan::may_retract(&grouped));
    assert!(!hotlap_core::plan::may_retract(&Plan::Project {
        input: Box::new(window.clone()),
        cols: vec![0],
    }));
    assert!(!hotlap_core::plan::may_retract(&Plan::TumbleCount {
        input: Box::new(window.clone()),
        key: vec![0],
        time_col: 1,
        size: 20,
    }));
    assert!(hotlap_core::plan::may_retract(&Plan::Filter {
        input: Box::new(grouped.clone()),
        pred: hotlap_core::plan::Predicate::IsNull(0),
    }));
    assert!(hotlap_core::plan::may_retract(&Plan::Join {
        left: Box::new(window),
        right: Box::new(grouped),
        left_key: vec![0],
        right_key: vec![0],
    }));
}
