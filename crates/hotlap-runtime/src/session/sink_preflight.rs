//! Preflight checks for declared sinks, before any writer is opened.

use hotlap::Plan;
use hotlap_sql::error::SqlError;

use super::{SinkDef, SinkFactory};

/// Reject duplicate views and plans a factory cannot handle before `create`.
///
/// The factory's declared retraction support is checked here, so a retracting
/// plan or a repeated view never opens a writer; the created sink is re-checked
/// by `Pipeline::validate`, so a factory cannot understate what it builds.
pub(super) fn preflight_sinks(
    sinks: &[SinkDef],
    factory: &dyn SinkFactory,
    views: &[(String, Plan)],
) -> Result<(), SqlError> {
    let mut seen: Vec<&str> = Vec::with_capacity(sinks.len());
    for def in sinks {
        if seen.contains(&def.view.as_str()) {
            return Err(SqlError::Unsupported(format!(
                "view `{}` already has a sink; fan-out is not supported",
                def.view
            )));
        }
        seen.push(def.view.as_str());
    }
    for def in sinks {
        let plan = views
            .iter()
            .find(|(name, _)| name == &def.view)
            .map(|(_, plan)| plan);
        if let Some(plan) = plan
            && hotlap::plan::may_retract(plan)
            && !factory.accepts_retractions(&def.options)
        {
            return Err(SqlError::Unsupported(format!(
                "sink `{}` cannot apply retractions; view `{}` may retract",
                def.name, def.view
            )));
        }
    }
    Ok(())
}
