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
        c.add_source("s", SourceDef { connector: "fluss".into() }).unwrap();
        assert!(c.add_source("s", SourceDef { connector: "fluss".into() }).is_err());
    }
}
