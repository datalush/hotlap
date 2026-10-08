//! Float `GROUP BY` aggregates: the documented `NaN` semantics.
//!
//! `NaN` is treated as an ordinary non-null input by `sum`/`avg`/`count`, so it
//! propagates through them (IEEE-754). `min`/`max` ignore it, and a group whose
//! only non-null inputs are `NaN` yields null. This is order-independent.

mod float_support;

use std::collections::BTreeMap;

use float_support::{Row, started, wait_for};

#[tokio::test]
async fn nan_aggregates_follow_documented_semantics() {
    // NaN sits before and after real values, so ordering cannot matter.
    let data: &[(i64, f64)] = &[
        (1, f64::NAN),
        (1, 2.0),
        (1, 4.0),
        (2, f64::NAN),
        (3, 1.5),
        (3, f64::NAN),
        (4, f64::NAN),
        (4, -3.0),
    ];
    let mut session = started(data).await;
    let got = wait_for(&mut session, |got| got.len() == 4).await;
    assert_eq!(got.len(), 4);
    let by_key: BTreeMap<i64, Row> = got.into_iter().map(|row| (row.0, row)).collect();

    // NaN among real values: sum/avg are NaN, min/max keep the real extremes.
    let one = by_key[&1];
    assert!(one.2.is_nan() && one.3.is_nan());
    assert_eq!((one.1, one.4, one.5), (3, Some(2.0), Some(4.0)));

    // Only NaN: min/max are null, sum/avg still NaN.
    let two = by_key[&2];
    assert!(two.2.is_nan() && two.3.is_nan());
    assert_eq!((two.1, two.4, two.5), (1, None, None));

    let three = by_key[&3];
    assert_eq!((three.4, three.5), (Some(1.5), Some(1.5)));
    let four = by_key[&4];
    assert_eq!((four.4, four.5), (Some(-3.0), Some(-3.0)));

    session.shutdown().await.unwrap();
}
