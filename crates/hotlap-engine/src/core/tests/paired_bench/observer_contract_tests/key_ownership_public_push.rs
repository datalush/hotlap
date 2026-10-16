use super::*;
use arrow::array::StringArray;

const ROWS: usize = 256;
const LONG_KEY: &str = "fluss/borrowed-key/κλειδί/";

fn keyed_batch(keys: Vec<String>, values: Vec<i64>, diffs: Vec<i64>) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn keyed_plan() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        }),
        key: vec![0],
        aggs: vec![AggSpec::count(), AggSpec::sum(1), AggSpec::sum(3)],
    }
}

fn keyed_core() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(ViewId(0), &keyed_plan()).unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core
}

fn dense_mode() -> ModeGuard {
    ModeGuard::new(true)
}

fn key_mode(owned: bool) -> Option<crate::ops::join::test_support::OwnedDenseJoinKeyGuard> {
    owned.then(crate::ops::join::test_support::OwnedDenseJoinKeyGuard::force)
}

type KeyedAggregateRow = (String, Option<i64>, Option<i64>, Option<i64>, i64);

fn expected_rows(batch: &ZSetBatch) -> Vec<KeyedAggregateRow> {
    let key = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let arrays = batch.batch.columns()[1..]
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect::<Vec<_>>();
    let diff = batch.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..batch.len())
        .map(|row| {
            (
                key.value(row).to_owned(),
                (!arrays[0].is_null(row)).then(|| arrays[0].value(row)),
                (!arrays[1].is_null(row)).then(|| arrays[1].value(row)),
                (!arrays[2].is_null(row)).then(|| arrays[2].value(row)),
                diff.value(row),
            )
        })
        .collect()
}

#[derive(Clone, Copy)]
struct Case {
    id: &'static str,
    left_keys: usize,
    match_index: usize,
    right_value: i64,
}

const CASES: [Case; 3] = [
    Case {
        id: "narrow-1x1",
        left_keys: 1,
        match_index: 0,
        right_value: 7,
    },
    Case {
        id: "repeated-long-256x1",
        left_keys: ROWS,
        match_index: ROWS - 1,
        right_value: 1000,
    },
    Case {
        id: "distinct-long-256x1",
        left_keys: ROWS,
        match_index: 137,
        right_value: 1000,
    },
];

fn select_case(id: &str) -> Case {
    CASES
        .iter()
        .copied()
        .find(|case| case.id == id)
        .unwrap_or_else(|| panic!("unknown KEY_OWNERSHIP_CASE {id:?}"))
}

#[test]
fn key_ownership_case_selector_accepts_only_stable_case_ids() {
    assert_eq!(select_case("narrow-1x1").id, "narrow-1x1");
    assert_eq!(select_case("repeated-long-256x1").id, "repeated-long-256x1");
    assert_eq!(select_case("distinct-long-256x1").id, "distinct-long-256x1");
}

#[test]
#[should_panic(expected = "unknown KEY_OWNERSHIP_CASE")]
fn key_ownership_case_selector_rejects_unknown_ids() {
    select_case("unknown");
}

fn inputs(case: Case) -> (ZSetBatch, ZSetBatch, ZSetBatch) {
    let left_keys = (0..case.left_keys)
        .map(|index| {
            if case.id == "narrow-1x1" {
                "k".to_owned()
            } else if case.id == "repeated-long-256x1" {
                format!("{LONG_KEY}repeated")
            } else {
                format!("{LONG_KEY}{index:04}-distinct-suffix")
            }
        })
        .collect::<Vec<_>>();
    let left_values = (0..case.left_keys as i64).collect::<Vec<_>>();
    let seed = keyed_batch(left_keys.clone(), left_values, vec![1; case.left_keys]);
    let seed_right = keyed_batch(vec![], vec![], vec![]);
    let key = left_keys[case.match_index].clone();
    let delta = keyed_batch(vec![key], vec![case.right_value], vec![1]);
    (seed, seed_right, delta)
}

fn run_case(case: Case, owned: bool) {
    let _dense = dense_mode();
    let _keys = key_mode(owned);
    let (left, empty_right, delta) = inputs(case);
    let mut core = keyed_core();
    crate::ops::join::test_support::reset_counts();
    core.push(InputId(0), &left).unwrap();
    core.push(InputId(1), &empty_right).unwrap();
    let seed_routes = crate::ops::join::test_support::counts();
    assert!(seed_routes.direct > 0);
    assert_eq!(seed_routes.forced_legacy, 0);
    assert_eq!(
        seed_routes.borrowed_key_buckets,
        if owned { 0 } else { seed_routes.direct }
    );
    assert_eq!(
        seed_routes.owned_key_buckets,
        if owned { seed_routes.direct } else { 0 }
    );
    assert!(core.take_changes(ViewId(0)).unwrap().is_empty());

    crate::ops::join::test_support::reset_counts();
    let started = Instant::now();
    let result = core.push(InputId(1), &delta);
    let elapsed_ns = started.elapsed().as_nanos();
    result.unwrap();
    let routes = crate::ops::join::test_support::counts();
    let expected_pairs = if case.id == "repeated-long-256x1" {
        ROWS
    } else {
        1
    };
    assert!(routes.direct > 0, "{} uses direct-index path", case.id);
    assert_eq!(routes.forced_legacy, 0);
    assert_eq!(routes.direct_pairs, expected_pairs);
    assert_eq!(
        routes.borrowed_key_buckets,
        if owned { 0 } else { routes.direct }
    );
    assert_eq!(
        routes.owned_key_buckets,
        if owned { routes.direct } else { 0 }
    );
    let change = core.take_changes(ViewId(0)).unwrap();
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    let expected = vec![(
        if case.id == "narrow-1x1" {
            "k".to_owned()
        } else if case.id == "repeated-long-256x1" {
            format!("{LONG_KEY}repeated")
        } else {
            format!("{LONG_KEY}{:04}-distinct-suffix", case.match_index)
        },
        Some(expected_pairs as i64),
        Some(if case.id == "repeated-long-256x1" {
            (0..ROWS as i64).sum()
        } else {
            case.match_index as i64
        }),
        Some(case.right_value * expected_pairs as i64),
        1,
    )];
    assert_eq!(
        expected_rows(&change),
        expected,
        "{} exact typed delta",
        case.id
    );
    assert_eq!(
        expected_rows(&snapshot),
        expected,
        "{} exact typed snapshot",
        case.id
    );
    println!(
        "key_ownership_smoke case={} mode={} seed_left_rows={} seed_right_rows=0 delta_rows=1 key_bytes={} expected_pairs={} direct_calls={} borrowed_buckets={} owned_buckets={} elapsed_ns={}",
        case.id,
        if owned { "owned" } else { "borrowed" },
        case.left_keys,
        expected[0].0.len(),
        expected_pairs,
        routes.direct,
        routes.borrowed_key_buckets,
        routes.owned_key_buckets,
        elapsed_ns
    );
}

#[test]
fn key_ownership_public_push_contracts_all_cases_both_modes() {
    for case in CASES {
        run_case(case, false);
        run_case(case, true);
    }
}

#[test]
#[ignore = "single-case key-ownership clock smoke; choose exactly one mode per fresh test process"]
fn key_ownership_public_push_clock_smoke() {
    let case_id =
        std::env::var("KEY_OWNERSHIP_CASE").expect("set KEY_OWNERSHIP_CASE to a known case ID");
    let case = select_case(&case_id);
    let mode = std::env::var("KEY_OWNERSHIP_MODE").expect("set KEY_OWNERSHIP_MODE=borrowed|owned");
    let owned = match mode.as_str() {
        "owned" => true,
        "borrowed" => false,
        _ => panic!("invalid mode {mode}"),
    };
    run_case(case, owned);
}
