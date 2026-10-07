use std::collections::HashMap;

/// Identifier of an input contributing to the frontier.
pub type InputKey = u64;

/// Per-input progress frontier.
///
/// The frontier stores, for every input, the greatest time it has announced as
/// complete. An epoch is settled once every input has advanced past it.
#[derive(Debug, Default, Clone)]
pub struct Frontier(HashMap<InputKey, u64>);

impl Frontier {
    /// Advances `input` to at least `time`; the frontier never moves backwards.
    pub fn advance(&mut self, input: InputKey, time: u64) {
        self.0
            .entry(input)
            .and_modify(|current| *current = (*current).max(time))
            .or_insert(time);
    }

    /// Smallest announced time across inputs, or `None` when unknown.
    pub fn min(&self) -> Option<u64> {
        self.0.values().copied().min()
    }

    /// Returns `true` when every input has advanced to at least `time`.
    pub fn settle(&self, time: u64) -> bool {
        self.0.values().all(|&current| current >= time)
    }
}

/// Returns `true` when every input in `frontier` has advanced to at least `time`.
pub fn settle(frontier: &Frontier, time: u64) -> bool {
    frontier.settle(time)
}

#[cfg(test)]
mod tests {
    use super::Frontier;

    #[test]
    fn frontier_tracks_minimum() {
        let mut frontier = Frontier::default();
        frontier.advance(1, 5);
        frontier.advance(2, 3);
        assert_eq!(frontier.min(), Some(3));
    }

    #[test]
    fn epochs_settle_when_all_inputs_pass() {
        let mut frontier = Frontier::default();
        frontier.advance(1, 5);
        frontier.advance(2, 3);
        assert!(!frontier.settle(4));
        assert!(frontier.settle(3));
        assert!(frontier.settle(2));
        frontier.advance(2, 7);
        assert!(frontier.settle(5));
    }

    #[test]
    fn advance_is_monotonic() {
        let mut frontier = Frontier::default();
        frontier.advance(1, 5);
        frontier.advance(1, 2);
        assert_eq!(frontier.min(), Some(5));
    }

    #[test]
    fn empty_frontier_settles_everything() {
        let frontier = Frontier::default();
        assert!(frontier.settle(u64::MAX));
    }
}
