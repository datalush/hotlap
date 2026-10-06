use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    I64(i64),
    Str(String),
    Bool(bool),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Row(pub Vec<Scalar>);

impl Row {
    /// Checked column access: an out-of-range `index` yields [`Scalar::Null`]
    /// instead of panicking. Plan validation rejects such indices before data
    /// flows; this is a last-resort safeguard so one bad index can never kill the
    /// worker thread (and degrade every later push to `Infrastructure`).
    pub fn col(&self, index: usize) -> Scalar {
        self.0.get(index).cloned().unwrap_or(Scalar::Null)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeBatch {
    /// (row, signed multiplicity): +1 insert, -1 retraction.
    pub rows: Vec<(Row, i64)>,
}

impl ChangeBatch {
    pub fn push(&mut self, row: Row, diff: i64) {
        self.rows.push((row, diff));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_orders_by_column() {
        let a = Row(vec![Scalar::I64(1)]);
        let b = Row(vec![Scalar::I64(2)]);
        assert!(a < b);
    }
}
