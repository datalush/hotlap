use hotlap::{ChangeBatch, Hotlap, InputId, Plan, Row, Scalar};

fn row(v: i64) -> Row {
    Row(vec![Scalar::I64(v)])
}

#[test]
fn changelog_reconstructs_snapshot() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("in").unwrap();
    h.create_view(
        "c",
        Plan::GroupCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
        },
    )
    .unwrap();
    h.tap_view("c").unwrap();
    let mut acc: std::collections::BTreeMap<Row, i64> = Default::default();
    for vals in [vec![1i64, 1, 2], vec![2, 3]] {
        let mut b = ChangeBatch::default();
        for v in vals {
            b.push(row(v), 1);
        }
        h.push("in", &b).unwrap();
        for (r, d) in h.take_changes("c").unwrap() {
            *acc.entry(r).or_insert(0) += d;
        }
    }
    let snap: std::collections::BTreeSet<Row> = h.snapshot("c").unwrap().into_iter().collect();
    let from_changes: std::collections::BTreeSet<Row> = acc
        .into_iter()
        .filter(|(_, d)| *d != 0)
        .map(|(r, _)| r)
        .collect();
    assert_eq!(snap, from_changes);
}

#[test]
fn tap_after_first_push_rejected() {
    let mut h = Hotlap::open().unwrap();
    h.register_input("in").unwrap();
    h.create_view(
        "c",
        Plan::GroupCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
        },
    )
    .unwrap();
    let mut b = ChangeBatch::default();
    b.push(row(1), 1);
    h.push("in", &b).unwrap(); // first push -> freeze
    assert!(h.tap_view("c").is_err()); // pre-freeze only
}
