//! Minimal plan IR: the operators the incremental core will compile to.

use crate::core::InputId;
use crate::row::{Row, Scalar};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Predicate {
    Eq(usize, Scalar),
    Gt(usize, i64),
}

impl Predicate {
    pub fn eval(&self, row: &Row) -> bool {
        match self {
            Predicate::Eq(col, want) => row.0.get(*col) == Some(want),
            Predicate::Gt(col, n) => matches!(row.0.get(*col), Some(Scalar::I64(v)) if v > n),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Plan {
    /// Leaf: the shared source registered under this id.
    Source(InputId),
    Filter {
        input: Box<Plan>,
        pred: Predicate,
    },
    Project {
        input: Box<Plan>,
        cols: Vec<usize>,
    },
    GroupCount {
        input: Box<Plan>,
        key: Vec<usize>,
    },
    /// Inner equi-join of two plans on `left_key`/`right_key`.
    ///
    /// Only key pairs where `left_key` and `right_key` select equal values produce
    /// output. The output row is **every column of the joined left row followed by
    /// every column of the joined right row** (`arity(left) + arity(right)`);
    /// multiplicities multiply, so retracting one side retracts its joined tuples.
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        left_key: Vec<usize>,
        right_key: Vec<usize>,
    },
    /// Agregado por ventana tumbling de `size` sobre `time_col`, agrupado por `key`.
    /// Salida: columnas de `key`, `window_start`, `count`. Emite cada ventana una vez
    /// al cerrarse (append-only).
    TumbleCount {
        input: Box<Plan>,
        key: Vec<usize>,
        time_col: usize,
        size: i64,
    },
}

/// ¿El plan contiene alguna ventana?
pub(crate) fn has_window(plan: &Plan) -> bool {
    match plan {
        Plan::TumbleCount { .. } => true,
        Plan::Filter { input, .. } | Plan::Project { input, .. } | Plan::GroupCount { input, .. } => {
            has_window(input)
        }
        Plan::Join { left, right, .. } => has_window(left) || has_window(right),
        Plan::Source(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i64) -> Row {
        Row(vec![Scalar::I64(v)])
    }

    #[test]
    fn predicate_eq_and_gt() {
        assert!(Predicate::Eq(0, Scalar::I64(1)).eval(&row(1)));
        assert!(!Predicate::Eq(0, Scalar::I64(2)).eval(&row(1)));
        assert!(Predicate::Gt(0, 0).eval(&row(1)));
        assert!(!Predicate::Gt(0, 1).eval(&row(1)));
    }
}
