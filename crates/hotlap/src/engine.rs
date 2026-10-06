//! Public [`Hotlap`] facade over the stateful [`IncrementalCore`].
//!
//! `Hotlap` owns a [`DdCore`] plus a name -> [`ViewId`] registry, so callers speak
//! in view names and engine types instead of core handles. View ids are assigned
//! monotonically from a counter; the counter only advances after a build succeeds.

use crate::core::dd::DdCore;
use crate::core::{IncrementalCore, ViewId};
use crate::plan::Plan;
use crate::row::{ChangeBatch, Row};

/// Error returned by the public [`Hotlap`] API.
#[derive(Debug)]
pub struct HotlapError(pub String);

/// Stateful engine facade: one live core, many named views.
pub struct Hotlap {
    core: DdCore,
    next: u32,
    views: std::collections::HashMap<String, ViewId>,
}

impl Hotlap {
    /// Open an engine and start its backing worker.
    pub fn open() -> Result<Self, HotlapError> {
        Ok(Self {
            core: DdCore::new().map_err(|e| HotlapError(format!("{e:?}")))?,
            next: 0,
            views: Default::default(),
        })
    }

    /// Compile `plan` into a new view registered under `name`.
    pub fn create_view(&mut self, name: &str, plan: Plan) -> Result<(), HotlapError> {
        let id = ViewId(self.next);
        // Only claim the id once the core accepts the plan; a failed build must
        // not burn an id or shadow an existing name with a dangling mapping.
        self.core
            .build_view(id, &plan)
            .map_err(|e| HotlapError(format!("{e:?}")))?;
        self.next += 1;
        self.views.insert(name.to_string(), id);
        Ok(())
    }

    /// Feed `batch` into the input of the view registered as `name`.
    pub fn push(&mut self, name: &str, batch: &ChangeBatch) -> Result<(), HotlapError> {
        let id = *self
            .views
            .get(name)
            .ok_or_else(|| HotlapError("no such view".into()))?;
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

    #[test]
    fn api_end_to_end_group_count() {
        let mut h = Hotlap::open().unwrap();
        h.create_view(
            "counts",
            Plan::GroupCount {
                input: Box::new(Plan::Scan),
                key: vec![0],
            },
        )
        .unwrap();
        let mut b = ChangeBatch::default();
        for f in fixture() {
            b.push(Row(vec![Scalar::I64(f.key), Scalar::I64(f.value)]), f.diff);
        }
        h.push("counts", &b).unwrap();
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
    fn unknown_view_name_errors() {
        let mut h = Hotlap::open().unwrap();
        let batch = ChangeBatch::default();
        assert!(matches!(h.push("nope", &batch), Err(HotlapError(_))));
        assert!(matches!(h.snapshot("nope"), Err(HotlapError(_))));
    }
}
