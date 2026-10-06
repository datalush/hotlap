//! Public [`Hotlap`] facade over the stateful [`IncrementalCore`].
//!
//! `Hotlap` owns a boxed [`IncrementalCore`] plus registries mapping caller names
//! to core handles: input names -> [`InputId`], view names -> [`ViewId`]. Ids are
//! assigned monotonically and only advance once the core accepts the declaration.

use std::collections::HashMap;

use crate::core::differential_dataflow::DifferentialCore;
use crate::core::{IncrementalCore, InputId, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Error returned by the public [`Hotlap`] API.
#[derive(Debug)]
pub struct HotlapError(pub String);

/// Stateful engine facade: one live core, many named inputs and views.
pub struct Hotlap {
    core: Box<dyn IncrementalCore>,
    next_input: u32,
    next_view: u32,
    inputs: HashMap<String, InputId>,
    views: HashMap<String, ViewId>,
}

impl Hotlap {
    /// Open an engine and start its backing worker.
    pub fn open() -> Result<Self, HotlapError> {
        Ok(Self {
            core: Box::new(DifferentialCore::new().map_err(|e| HotlapError(format!("{e:?}")))?),
            next_input: 0,
            next_view: 0,
            inputs: Default::default(),
            views: Default::default(),
        })
    }

    /// Declare a source under `name`; plans reference it as `Plan::Source(id)`.
    pub fn register_input(&mut self, name: &str) -> Result<(), HotlapError> {
        if self.inputs.contains_key(name) {
            return Err(HotlapError(format!("input already exists: {name}")));
        }
        let id = InputId(self.next_input);
        self.core
            .register_input(id)
            .map_err(|e| HotlapError(format!("{e:?}")))?;
        self.next_input += 1;
        self.inputs.insert(name.to_string(), id);
        Ok(())
    }

    /// Compile `plan` into a new view registered under `name`.
    pub fn create_view(&mut self, name: &str, plan: Plan) -> Result<(), HotlapError> {
        // Reject duplicate names up front: overwriting the mapping would orphan
        // the previously-built view (still alive in the core, unreachable here).
        if self.views.contains_key(name) {
            return Err(HotlapError(format!("view already exists: {name}")));
        }
        let id = ViewId(self.next_view);
        // Only claim the id once the core accepts the plan; a failed build must
        // not burn an id or shadow an existing name with a dangling mapping.
        self.core
            .build_view(id, &plan)
            .map_err(|e| HotlapError(format!("{e:?}")))?;
        self.next_view += 1;
        self.views.insert(name.to_string(), id);
        Ok(())
    }

    /// Feed `batch` into the input registered as `name`.
    pub fn push(&mut self, input: &str, batch: &ChangeBatch) -> Result<(), HotlapError> {
        let id = *self
            .inputs
            .get(input)
            .ok_or_else(|| HotlapError("no such input".into()))?;
        self.core
            .push(id, batch)
            .map_err(|e| HotlapError(format!("{e:?}")))
    }

    /// Read the consolidated output of the view registered as `name`.
    pub fn snapshot(&mut self, name: &str) -> Result<Vec<Row>, HotlapError> {
        let id = *self
            .views
            .get(name)
            .ok_or_else(|| HotlapError("no such view".into()))?;
        self.core
            .snapshot(id)
            .map_err(|e| HotlapError(format!("{e:?}")))
    }

    /// Shut the engine down. Dropping `Hotlap` also stops the worker.
    pub fn shutdown(self) -> Result<(), HotlapError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{fixture, recompute_group_count};
    use crate::row::Scalar;

    fn count_by(key: usize) -> Plan {
        Plan::GroupCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![key],
        }
    }

    #[test]
    fn api_end_to_end_group_count() {
        let mut h = Hotlap::open().unwrap();
        h.register_input("events").unwrap();
        h.create_view("counts", count_by(0)).unwrap();
        let mut b = ChangeBatch::default();
        for f in fixture() {
            b.push(Row(vec![Scalar::I64(f.key), Scalar::I64(f.value)]), f.diff);
        }
        h.push("events", &b).unwrap();
        let mut got: Vec<(i64, i64)> = h
            .snapshot("counts")
            .unwrap()
            .into_iter()
            .map(|r| match (&r.0[0], &r.0[1]) {
                (Scalar::I64(k), Scalar::I64(c)) => (*k, *c),
                _ => panic!("shape"),
            })
            .collect();
        got.sort();
        assert_eq!(got, recompute_group_count(&fixture()));
        h.shutdown().unwrap();
    }

    #[test]
    fn unknown_input_and_view_names_error() {
        let mut h = Hotlap::open().unwrap();
        let batch = ChangeBatch::default();
        assert!(matches!(h.push("nope", &batch), Err(HotlapError(_))));
        assert!(matches!(h.snapshot("nope"), Err(HotlapError(_))));
    }

    #[test]
    fn schema_freezes_after_first_push() {
        let mut h = Hotlap::open().unwrap();
        h.register_input("in").unwrap();
        h.create_view("v", count_by(0)).unwrap();
        h.push("in", &ChangeBatch::default()).unwrap();

        assert!(h.register_input("late").is_err());
        assert!(h.create_view("late", count_by(0)).is_err());
    }

    #[test]
    fn duplicate_view_name_rejected_and_original_survives() {
        let mut h = Hotlap::open().unwrap();
        h.register_input("in").unwrap();
        h.create_view("v", count_by(0)).unwrap();
        assert!(matches!(
            h.create_view("v", count_by(0)),
            Err(HotlapError(_))
        ));

        // The first view is still operable.
        let mut b = ChangeBatch::default();
        b.push(Row(vec![Scalar::I64(7), Scalar::I64(1)]), 1);
        h.push("in", &b).unwrap();
        assert_eq!(
            h.snapshot("v").unwrap(),
            vec![Row(vec![Scalar::I64(7), Scalar::I64(1)])]
        );
    }
}
