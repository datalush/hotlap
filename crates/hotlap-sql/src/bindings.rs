//! Explicit source bindings shared by plan translation and schema derivation.
//!
//! A relation name is resolved to the [`InputId`] it reads from, and that id to
//! the Arrow schema of its rows. Keeping both maps explicit — instead of a
//! single implicit input — is what stops two relations in one query from
//! collapsing silently into the same source.

use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;
use hotlap::InputId;

/// Relation name (as written in the query) to the source input it reads.
pub type SourceBindings = BTreeMap<String, InputId>;

/// Source input to the Arrow schema of that source's rows.
pub type InputSchemas = BTreeMap<InputId, SchemaRef>;
