//! Explicit source bindings shared by plan translation and schema derivation.
//!
//! A relation name is resolved to the [`InputId`] it reads from, and that id to
//! the Arrow schema of its rows. Keeping both maps explicit — instead of a
//! single implicit input — is what stops two relations in one query from
//! collapsing silently into the same source.

use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;
use hotlap::InputId;

/// Canonical relation name (see [`canonical_relation`]) to the source input it
/// reads.
pub type SourceBindings = BTreeMap<String, InputId>;

/// Source input to the Arrow schema of that source's rows.
pub type InputSchemas = BTreeMap<InputId, SchemaRef>;

/// Canonicalize a relation name the way DataFusion resolves it in SQL: an
/// unquoted identifier is case-folded to lowercase, while a double-quoted
/// identifier keeps its case and drops its quotes.
///
/// Callers use this when building [`SourceBindings`] so the name written in
/// `CREATE SOURCE` matches the relation name DataFusion reports in the plan.
pub fn canonical_relation(name: &str) -> String {
    match name
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
    {
        Some(inner) => inner.to_string(),
        None => name.to_ascii_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::canonical_relation;

    #[test]
    fn unquoted_names_are_case_folded() {
        assert_eq!(canonical_relation("Src"), "src");
        assert_eq!(canonical_relation("src"), "src");
    }

    #[test]
    fn quoted_names_keep_their_case() {
        assert_eq!(canonical_relation("\"Src\""), "Src");
        assert_eq!(canonical_relation("\"my Table\""), "my Table");
    }
}
