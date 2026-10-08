//! Export and import of the persistent operator state held by a view graph.

use hotlap_core::snapshot::{GroupState, JoinState, OperatorState, WindowState};

use super::{Node, ViewGraph};
use crate::core::ipc::{decode_zset, encode_zset};
use crate::error::EngineError;

impl ViewGraph {
    /// Exports one entry per node in plan pre-order (`None` when stateless).
    pub(crate) fn export_states(&self) -> Result<Vec<Option<OperatorState>>, EngineError> {
        let mut out = Vec::new();
        self.root.export(&mut out)?;
        Ok(out)
    }

    /// Restores operator state from a pre-order export, consuming every entry.
    pub(crate) fn import_states(
        &mut self,
        states: &[Option<OperatorState>],
    ) -> Result<(), EngineError> {
        let mut cursor = Cursor { states, pos: 0 };
        self.root.import(&mut cursor)?;
        if cursor.pos != states.len() {
            return Err(EngineError::Infrastructure(format!(
                "snapshot has {} operator states, graph consumed {}",
                states.len(),
                cursor.pos
            )));
        }
        Ok(())
    }
}

impl Node {
    /// Appends this subtree's state in pre-order.
    fn export(&self, out: &mut Vec<Option<OperatorState>>) -> Result<(), EngineError> {
        match self {
            Node::Source(_) => out.push(None),
            Node::Filter { input, .. } | Node::Project { input, .. } => {
                out.push(None);
                input.export(out)?;
            }
            Node::Group { input, reducer } => {
                out.push(Some(OperatorState::Group(reducer.export_state()?)));
                input.export(out)?;
            }
            Node::Window { input, reducer } => {
                out.push(Some(OperatorState::Window(reducer.export_state()?)));
                input.export(out)?;
            }
            Node::Joined {
                left,
                right,
                join,
                left_pending,
                right_pending,
            } => {
                let mut state = join.export_state()?;
                state.left_pending = left_pending.as_ref().map(encode_zset).transpose()?;
                state.right_pending = right_pending.as_ref().map(encode_zset).transpose()?;
                out.push(Some(OperatorState::Join(state)));
                left.export(out)?;
                right.export(out)?;
            }
        }
        Ok(())
    }

    /// Restores this subtree's state from the cursor, in pre-order.
    fn import(&mut self, cursor: &mut Cursor<'_>) -> Result<(), EngineError> {
        match self {
            Node::Source(_) => cursor.expect_stateless(),
            Node::Filter { input, .. } | Node::Project { input, .. } => {
                cursor.expect_stateless()?;
                input.import(cursor)
            }
            Node::Group { input, reducer } => {
                reducer.import_state(cursor.take_group()?)?;
                input.import(cursor)
            }
            Node::Window { input, reducer } => {
                reducer.import_state(cursor.take_window()?)?;
                input.import(cursor)
            }
            Node::Joined {
                left,
                right,
                join,
                left_pending,
                right_pending,
            } => {
                let state = cursor.take_join()?;
                *left_pending = state.left_pending.as_ref().map(decode_zset).transpose()?;
                *right_pending = state.right_pending.as_ref().map(decode_zset).transpose()?;
                join.import_state(state)?;
                left.import(cursor)?;
                right.import(cursor)
            }
        }
    }
}

/// Position over a pre-order operator-state export.
struct Cursor<'a> {
    states: &'a [Option<OperatorState>],
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// Returns the next entry, or an error when the export is short.
    fn next(&mut self) -> Result<&'a Option<OperatorState>, EngineError> {
        let entry = self.states.get(self.pos).ok_or_else(|| {
            EngineError::Infrastructure("engine snapshot has too few operator states".to_string())
        })?;
        self.pos += 1;
        Ok(entry)
    }

    /// Consumes a stateless-node entry.
    fn expect_stateless(&mut self) -> Result<(), EngineError> {
        match self.next()? {
            None => Ok(()),
            Some(_) => Err(EngineError::Infrastructure(
                "expected a stateless operator state".to_string(),
            )),
        }
    }

    /// Consumes a group node entry.
    fn take_group(&mut self) -> Result<&'a GroupState, EngineError> {
        match self.next()? {
            Some(OperatorState::Group(state)) => Ok(state),
            _ => Err(EngineError::Infrastructure(
                "expected a group operator state".to_string(),
            )),
        }
    }

    /// Consumes a window node entry.
    fn take_window(&mut self) -> Result<&'a WindowState, EngineError> {
        match self.next()? {
            Some(OperatorState::Window(state)) => Ok(state),
            _ => Err(EngineError::Infrastructure(
                "expected a window operator state".to_string(),
            )),
        }
    }

    /// Consumes a join node entry.
    fn take_join(&mut self) -> Result<&'a JoinState, EngineError> {
        match self.next()? {
            Some(OperatorState::Join(state)) => Ok(state),
            _ => Err(EngineError::Infrastructure(
                "expected a join operator state".to_string(),
            )),
        }
    }
}
