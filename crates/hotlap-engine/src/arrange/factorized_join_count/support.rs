use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, new_empty_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap_core::ZSetBatch;

pub(super) type Key = Option<i64>;
pub(super) type Row = (Key, Option<i64>, i64);
pub(super) type Output = BTreeMap<(Key, i64), i64>;

pub(super) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("payload", DataType::Int64, true),
    ]))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("count", DataType::Int64, true),
    ]))
}

pub(super) fn batch(rows: &[Row]) -> ZSetBatch {
    batch_at(rows, 0)
}

pub(super) fn batch_at(rows: &[Row], key_index: usize) -> ZSetBatch {
    assert!(key_index <= 1);
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    let mut fields = vec![
        Field::new("k", DataType::Int64, true),
        Field::new("payload", DataType::Int64, true),
    ];
    if key_index == 1 {
        columns.swap(0, 1);
        fields.swap(0, 1);
    }
    ZSetBatch::new(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

pub(super) fn empty_batch(schema: SchemaRef) -> ZSetBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect();
    ZSetBatch::new(
        RecordBatch::try_new(schema, columns).unwrap(),
        Arc::new(Int64Array::from(Vec::<i64>::new())),
    )
    .unwrap()
}

pub(super) fn output(z: &ZSetBatch) -> Output {
    let keys = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out = Output::new();
    for i in 0..z.len() {
        assert!(!counts.is_null(i) && !diffs.is_null(i));
        let key = if keys.is_null(i) {
            None
        } else {
            Some(keys.value(i))
        };
        let weight = out.entry((key, counts.value(i))).or_insert(0_i64);
        *weight = weight
            .checked_add(diffs.value(i))
            .expect("test output weight bound");
    }
    out.retain(|_, weight| *weight != 0);
    out
}

pub(super) fn fold_output(out: &mut Output, z: &ZSetBatch) {
    for (row, diff) in output(z) {
        let weight = out.entry(row).or_insert(0_i64);
        *weight = weight.checked_add(diff).expect("test output weight bound");
    }
    out.retain(|_, weight| *weight != 0);
}

pub(super) fn oracle(left: &[Row], right: &[Row]) -> Output {
    fn retained(rows: &[Row]) -> BTreeMap<(Key, Option<i64>), i128> {
        let mut out = BTreeMap::new();
        for &(key, payload, diff) in rows {
            *out.entry((key, payload)).or_insert(0_i128) += i128::from(diff);
        }
        out.retain(|_, weight| *weight != 0);
        assert!(out.values().all(|weight| *weight >= 0));
        out
    }
    let l = retained(left);
    let r = retained(right);
    let mut groups = BTreeMap::new();
    for (&(lk, _), &ld) in &l {
        for (&(rk, _), &rd) in &r {
            if lk == rk {
                let count = groups.entry(lk).or_insert(0_i128);
                *count = count.checked_add(ld.checked_mul(rd).unwrap()).unwrap();
            }
        }
    }
    groups
        .into_iter()
        .filter(|(_, count)| *count != 0)
        .map(|(key, count)| ((key, i64::try_from(count).unwrap()), 1))
        .collect()
}

pub(super) struct Reference {
    join: crate::ops::join::Join,
    group: crate::ops::group_aggregate::GroupAggregate,
}

impl Reference {
    pub(super) fn new(left_key: usize, right_key: usize) -> Self {
        Self {
            join: crate::ops::join::Join::new(&[left_key], &[right_key]),
            group: crate::ops::group_aggregate::GroupAggregate::new(
                &[left_key],
                vec![hotlap_core::AggSpec::count()],
            ),
        }
    }
    pub(super) fn apply(
        &mut self,
        l: &ZSetBatch,
        r: &ZSetBatch,
    ) -> Result<ZSetBatch, crate::EngineError> {
        self.group.apply(&self.join.apply(l, r)?)
    }
    pub(super) fn work(&self) -> u64 {
        self.join.work()
    }
}

#[test]
fn nullable_join_behavior_is_confirmed_against_reference() {
    let left = [(None, Some(10), 2)];
    let right = [(None, Some(20), 3)];
    let mut reference = Reference::new(0, 0);
    let result = reference.apply(&batch(&left), &batch(&right)).unwrap();
    let actual = output(&result);
    assert_eq!(actual, [((None, 6), 1)].into());
    assert_eq!(result.batch.schema(), output_schema());
}

#[test]
fn reference_matches_oracle_for_count_rows() {
    let left = [(Some(1), Some(10), 2)];
    let right = [(Some(1), Some(20), 3)];
    let mut reference = Reference::new(0, 0);
    let actual = output(&reference.apply(&batch(&left), &batch(&right)).unwrap());
    assert_eq!(actual, [((Some(1), 6), 1)].into());
    assert_eq!(actual, oracle(&left, &right));
}

#[test]
fn swapped_right_key_preserves_exact_output_schema() {
    let left = [(Some(1), Some(10), 2)];
    let right = [(Some(1), Some(20), 3)];
    let mut reference = Reference::new(0, 1);
    let result = reference
        .apply(&batch_at(&left, 0), &batch_at(&right, 1))
        .unwrap();

    assert_eq!(output(&result), [((Some(1), 6), 1)].into());
    assert_eq!(result.batch.schema(), output_schema());
}

#[test]
fn empty_batches_preserve_schema_and_output_weights() {
    let left = empty_batch(schema());
    let right = empty_batch(schema());
    let mut reference = Reference::new(0, 0);
    let result = reference.apply(&left, &right).unwrap();
    assert_eq!(result.batch.schema(), output_schema());
    assert_eq!(output(&result), Output::new());
    let mut folded = [((Some(8), 2), 3)].into();
    fold_output(&mut folded, &result);
    assert_eq!(folded, [((Some(8), 2), 3)].into());
    assert_eq!(reference.work(), 0);
}
