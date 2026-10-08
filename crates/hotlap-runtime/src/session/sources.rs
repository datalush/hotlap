//! Session source registry, bindings and runtime `Sources` construction.
//!
//! Declared sources are keyed by their canonical relation name, so the name
//! DataFusion reports in a plan is the one stored here. Input ids are assigned
//! at `START` in that canonical (sorted) order, which makes the assignment
//! independent of the declaration order and stable across a restart.
//!
//! `START` freezes the bindings: a view declared later compiles against the
//! frozen map, never a freshly recomputed one.

use std::sync::Arc;

use hotlap::{InputId, Plan};
use hotlap_connectors::source::Source;
use hotlap_sql::bindings::{InputSchemas, SourceBindings};
use hotlap_sql::error::SqlError;
use hotlap_sql::translate::to_kernel_plan;

use super::{SqlSession, to_engine};
use crate::runtime::pipeline::Watermark;
use crate::runtime::sources::{InputSource, Sources};

/// A registered source and its optional watermark policy.
pub struct SessionSource {
    pub source: Arc<dyn Source>,
    pub watermark: Option<Watermark>,
}

impl SqlSession {
    /// Canonical relation name to input id, assigned in sorted name order.
    pub(super) fn source_bindings(&self) -> Result<SourceBindings, SqlError> {
        let mut bindings = SourceBindings::new();
        for (index, name) in self.sources.keys().enumerate() {
            let id = u32::try_from(index).map_err(|_| too_many_sources())?;
            bindings.insert(name.clone(), InputId(id));
        }
        Ok(bindings)
    }

    /// The bindings a new view compiles against: the frozen map after `START`,
    /// or the current assignment before it.
    pub(super) fn active_bindings(&self) -> Result<SourceBindings, SqlError> {
        match &self.frozen {
            Some(bindings) => Ok(bindings.clone()),
            None => self.source_bindings(),
        }
    }

    /// The Arrow schema behind every bound input, resolved by name.
    pub(super) fn input_schemas(
        &self,
        bindings: &SourceBindings,
    ) -> Result<InputSchemas, SqlError> {
        let mut schemas = InputSchemas::new();
        for (name, id) in bindings {
            schemas.insert(*id, self.session_source(name)?.source.schema());
        }
        Ok(schemas)
    }

    /// Recompile every pre-`START` view logical plan against `bindings`.
    pub(super) fn compile_views(
        &self,
        bindings: &SourceBindings,
    ) -> Result<Vec<(String, Plan)>, SqlError> {
        self.view_plans
            .iter()
            .map(|view| {
                let plan = to_kernel_plan(&view.logical, bindings)?;
                Ok((view.name.clone(), plan))
            })
            .collect()
    }

    /// Build the runtime `Sources` set, one entry per bound input.
    pub(super) fn build_sources(&self, bindings: &SourceBindings) -> Result<Sources, SqlError> {
        let mut entries = Vec::with_capacity(bindings.len());
        for (name, id) in bindings {
            let entry = self.session_source(name)?;
            entries.push(InputSource {
                id: *id,
                name: name.clone(),
                source: Arc::clone(&entry.source),
                watermark: entry.watermark,
            });
        }
        Sources::new(entries).map_err(to_engine)
    }

    fn session_source(&self, name: &str) -> Result<&SessionSource, SqlError> {
        self.sources
            .get(name)
            .ok_or_else(|| SqlError::Catalog(format!("source not registered: {name}")))
    }
}

fn too_many_sources() -> SqlError {
    SqlError::Unsupported("too many sources for the input id space".into())
}
