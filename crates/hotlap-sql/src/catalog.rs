//! Minimal catalog of sources and materialized views.

use std::collections::HashMap;

use crate::error::SqlError;

/// A declared source's connection descriptor (options captured from DDL).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceDef {
    pub connector: String,
}

/// A materialized view definition.
#[derive(Clone, Debug)]
pub struct MvDef {
    pub query: String,
}

/// Names of declared sources and materialized views.
#[derive(Default)]
pub struct Catalog {
    sources: HashMap<String, SourceDef>,
    views: HashMap<String, MvDef>,
}

impl Catalog {
    pub fn add_source(&mut self, name: &str, def: SourceDef) -> Result<(), SqlError> {
        if self.sources.contains_key(name) {
            return Err(SqlError::Catalog(format!("source already exists: {name}")));
        }
        self.sources.insert(name.to_string(), def);
        Ok(())
    }

    /// Remove a registered source, undoing a failed `CREATE SOURCE`.
    pub fn remove_source(&mut self, name: &str) -> Option<SourceDef> {
        self.sources.remove(name)
    }

    pub fn add_view(&mut self, name: &str, def: MvDef) -> Result<(), SqlError> {
        if self.views.contains_key(name) {
            return Err(SqlError::Catalog(format!("view already exists: {name}")));
        }
        self.views.insert(name.to_string(), def);
        Ok(())
    }

    pub fn source(&self, name: &str) -> Option<&SourceDef> {
        self.sources.get(name)
    }

    pub fn view(&self, name: &str) -> Option<&MvDef> {
        self.views.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn catalog_registers_and_rejects_duplicates() {
        let mut c = Catalog::default();
        c.add_source(
            "s",
            SourceDef {
                connector: "fluss".into(),
            },
        )
        .unwrap();
        assert!(
            c.add_source(
                "s",
                SourceDef {
                    connector: "fluss".into()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn removing_a_source_frees_its_name() {
        let mut c = Catalog::default();
        c.add_source(
            "s",
            SourceDef {
                connector: "fluss".into(),
            },
        )
        .unwrap();
        assert!(c.remove_source("s").is_some());
        assert!(c.source("s").is_none());
        // The name is free again after the failed registration is undone.
        c.add_source(
            "s",
            SourceDef {
                connector: "inmem".into(),
            },
        )
        .unwrap();
    }
}
