//! Input identity: explicit ids must not alias, renumber or mutate on failure.

use hotlap::{CoreError, Hotlap, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};
use hotlap_engine::{EngineCore, EngineSnapshot};

fn open() -> Hotlap {
    Hotlap::open_with(Box::new(EngineCore::new()))
}

/// A core that rejects one chosen input id and delegates everything else.
struct RejectingCore {
    inner: EngineCore,
    reject_id: InputId,
}

impl RejectingCore {
    fn rejecting(reject_id: InputId) -> Self {
        Self {
            inner: EngineCore::new(),
            reject_id,
        }
    }
}

impl IncrementalCore for RejectingCore {
    fn register_input(&mut self, input: InputId) -> Result<(), CoreError> {
        if input == self.reject_id {
            return Err(CoreError::Unsupported("registration rejected".into()));
        }
        self.inner.register_input(input)
    }

    fn build_view(&mut self, view: ViewId, plan: &Plan) -> Result<(), CoreError> {
        self.inner.build_view(view, plan)
    }

    fn set_input_retention(&mut self, events: usize) -> Result<(), CoreError> {
        self.inner.set_input_retention(events)
    }

    fn declare_watermark(&mut self, input: InputId, spec: WatermarkSpec) -> Result<(), CoreError> {
        self.inner.declare_watermark(input, spec)
    }

    fn push(&mut self, input: InputId, batch: &ZSetBatch) -> Result<(), CoreError> {
        self.inner.push(input, batch)
    }

    fn snapshot(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError> {
        self.inner.snapshot(view)
    }

    fn late_dropped(&self, input: InputId) -> Result<u64, CoreError> {
        self.inner.late_dropped(input)
    }

    fn take_changes(&mut self, view: ViewId) -> Result<ZSetBatch, CoreError> {
        self.inner.take_changes(view)
    }

    fn tap_view(&mut self, view: ViewId) -> Result<(), CoreError> {
        self.inner.tap_view(view)
    }

    fn checkpoint(&self) -> Result<EngineSnapshot, CoreError> {
        IncrementalCore::checkpoint(&self.inner)
    }

    fn restore(&mut self, snapshot: &EngineSnapshot) -> Result<(), CoreError> {
        IncrementalCore::restore(&mut self.inner, snapshot)
    }
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
fn core_registration_failure_leaves_facade_unchanged() {
    let mut core = Hotlap::open_with(Box::new(RejectingCore::rejecting(InputId(4))));
    assert!(core.register_input_with_id("a", InputId(4)).is_err());
    // The rejected name is free again and the id counter did not advance:
    // reusing "a" gets the automatic id 0, not 5.
    core.register_input("a").unwrap();
    let snapshot = core.checkpoint().unwrap();
    assert_eq!(snapshot.inputs.len(), 1);
    assert!(snapshot.inputs.iter().any(|input| input.id == InputId(0)));
}

#[test]
fn overflowing_explicit_id_is_rejected_without_mutation() {
    let mut core = open();
    assert!(
        core.register_input_with_id("max", InputId(u32::MAX))
            .is_err()
    );
    assert!(core.checkpoint().unwrap().inputs.is_empty());
    // No id was burned: the automatic path still starts at 0.
    core.register_input("auto").unwrap();
    let snapshot = core.checkpoint().unwrap();
    assert_eq!(snapshot.inputs.len(), 1);
    assert!(snapshot.inputs.iter().any(|input| input.id == InputId(0)));
}
