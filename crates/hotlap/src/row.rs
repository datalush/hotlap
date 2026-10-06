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
