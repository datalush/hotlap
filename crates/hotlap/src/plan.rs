//! Minimal plan IR: the operators the incremental core will compile to.

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
    Scan,
    Filter { input: Box<Plan>, pred: Predicate },
    Project { input: Box<Plan>, cols: Vec<usize> },
    GroupCount { input: Box<Plan>, key: Vec<usize> },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i64) -> Row { Row(vec![Scalar::I64(v)]) }

    #[test]
    fn predicate_eq_and_gt() {
        assert!(Predicate::Eq(0, Scalar::I64(1)).eval(&row(1)));
        assert!(!Predicate::Eq(0, Scalar::I64(2)).eval(&row(1)));
        assert!(Predicate::Gt(0, 0).eval(&row(1)));
        assert!(!Predicate::Gt(0, 1).eval(&row(1)));
    }
}
