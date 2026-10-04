// SPDX-License-Identifier: Apache-2.0
//! Immutable provider support, not an authorization or transaction guarantee.

use crate::LogReadMode;

/// Read semantics selected on this provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussReadCapability {
    Log(LogReadMode),
    KvSnapshot,
}

/// Meaning of ordinary SQL INSERT, not INSERT OVERWRITE/REPLACE.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlussInsertCapability {
    Append,
    FullRowUpsert,
}

/// Connector support at provider opening/selection time.
///
/// Permissions and table identity/schema/policy are validated during execution.
/// MERGE requires a finite source, complete INSERT values and immutable primary/
/// partition keys; its delete clauses also require `delete` to be true.
/// No global snapshot, statement rollback or exactly-once guarantee is implied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlussCapabilities {
    pub read: FlussReadCapability,
    pub insert: FlussInsertCapability,
    /// Whether the observed table policy permits key deletion.
    pub delete: bool,
    /// Whether the provider supports the restricted KV MERGE contract.
    pub merge: bool,
}
