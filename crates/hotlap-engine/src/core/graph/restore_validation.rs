//! Restore-only checks tying persisted root aggregate history to view output.

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use hotlap_core::InputId;
use hotlap_core::plan_normalize::{Recognition, recognize};

use super::{Node, ViewGraph};
use crate::core::output::ViewOutput;
use crate::error::EngineError;

impl ViewGraph {
    pub(in crate::core) fn validate_persisted_root_output(
        &self,
        schemas: &HashMap<InputId, SchemaRef>,
        output: &ViewOutput,
    ) -> Result<(), EngineError> {
        let (reducer, cut_plan, projection) = match &self.root {
            Node::Group {
                reducer,
                cut_plan: Some(plan),
                ..
            } => (reducer.as_ref(), plan.as_ref(), None),
            Node::Project { input, cols } => match input.as_ref() {
                Node::Group {
                    reducer,
                    cut_plan: Some(plan),
                    ..
                } => (reducer.as_ref(), plan.as_ref(), Some(cols.as_slice())),
                _ => return Ok(()),
            },
            _ => return Ok(()),
        };
        if !matches!(recognize(cut_plan, schemas), Recognition::Eligible(_)) {
            return Ok(());
        }
        let expected = match reducer.persisted_rows()? {
            Some(rows) => Some(match projection {
                Some(columns) => crate::ops::filter_project::project(&rows, columns)?,
                None => rows,
            }),
            None => None,
        };
        output.validate_rows(expected.as_ref())
    }
}
