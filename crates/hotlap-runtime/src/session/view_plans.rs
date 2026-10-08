//! Pre-`START` materialized views kept as DataFusion logical plans.
//!
//! Input ids are only final once every source is declared, so a view declared
//! before `START` is validated against the current bindings but its logical
//! plan is stored. `START` recompiles it against the final assignment, which
//! keeps an early view reading the relation it named even when a later source
//! declaration reorders the ids.

use datafusion::logical_expr::LogicalPlan;

/// A materialized view declared before `START`.
pub struct ViewPlan {
    /// Catalog name of the view.
    pub name: String,
    /// DataFusion plan, recompiled against the final bindings at `START`.
    pub logical: LogicalPlan,
}
