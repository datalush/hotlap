//! Test-only differential harness: fixture changelog + full-recompute oracle.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fixture {
    pub key: i64,
    pub value: i64,
    pub diff: i64,
}

pub fn fixture() -> Vec<Fixture> {
    vec![
        Fixture {
            key: 1,
            value: 10,
            diff: 1,
        },
        Fixture {
            key: 1,
            value: 20,
            diff: 1,
        },
        Fixture {
            key: 2,
            value: 30,
            diff: 1,
        },
        Fixture {
            key: 1,
            value: 10,
            diff: -1,
        },
        Fixture {
            key: 3,
            value: 30,
            diff: 1,
        },
        Fixture {
            key: 2,
            value: 30,
            diff: -1,
        },
        Fixture {
            key: 1,
            value: 20,
            diff: 1,
        },
    ]
}

/// Full-recompute oracle for `SELECT key, COUNT(*) ... GROUP BY key`.
pub fn recompute_group_count(rows: &[Fixture]) -> Vec<(i64, i64)> {
    use std::collections::BTreeMap;
    let mut c: BTreeMap<i64, i64> = BTreeMap::new();
    for r in rows {
        *c.entry(r.key).or_default() += r.diff;
    }
    c.into_iter().filter(|(_, v)| *v != 0).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_is_well_formed() {
        assert_eq!(recompute_group_count(&fixture()), vec![(1, 2), (3, 1)]);
    }
}
