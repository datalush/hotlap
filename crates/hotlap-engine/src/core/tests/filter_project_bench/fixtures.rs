use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, StringDictionaryBuilder};
use arrow::datatypes::{DataType, Field, Int8Type, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::snapshot::{AggValue, EngineSnapshot, OperatorState};
use hotlap_core::{AggSpec, InputId, Plan, ZSetBatch};

#[derive(Clone, Copy)]
pub(super) struct Case {
    pub name: &'static str,
    pub shape: Shape,
    pub rows: usize,
    pub payload_bytes: usize,
    pub selection: Selection,
    pub projection: Projection,
    pub expected_fused: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Shape {
    Narrow,
    WideDiscardedUtf8,
    Repeated,
    WideDistinct,
    GroupFloat,
    DictionaryFallback,
}

impl Shape {
    pub(super) fn source_columns(self) -> usize {
        match self {
            Self::Narrow | Self::Repeated => 3,
            Self::WideDiscardedUtf8 | Self::DictionaryFallback => 4,
            Self::WideDistinct => 258,
            Self::GroupFloat => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Selection {
    AllTrue,
    AllFalse,
    MixedNullable,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Projection {
    Narrow,
    Repeated,
    WideDistinct,
}

pub(super) const CASES: [Case; 10] = [
    Case {
        name: "tiny-1-alltrue",
        shape: Shape::Narrow,
        rows: 1,
        payload_bytes: 0,
        selection: Selection::AllTrue,
        projection: Projection::Narrow,
        expected_fused: true,
    },
    Case {
        name: "narrow-mixed",
        shape: Shape::Narrow,
        rows: 2048,
        payload_bytes: 0,
        selection: Selection::MixedNullable,
        projection: Projection::Narrow,
        expected_fused: true,
    },
    Case {
        name: "wide-utf8-discard-mixed",
        shape: Shape::WideDiscardedUtf8,
        rows: 4096,
        payload_bytes: 4096,
        selection: Selection::MixedNullable,
        projection: Projection::Narrow,
        expected_fused: true,
    },
    Case {
        name: "wide-utf8-discard-alltrue",
        shape: Shape::WideDiscardedUtf8,
        rows: 4096,
        payload_bytes: 4096,
        selection: Selection::AllTrue,
        projection: Projection::Narrow,
        expected_fused: true,
    },
    Case {
        name: "wide-utf8-discard-allfalse",
        shape: Shape::WideDiscardedUtf8,
        rows: 4096,
        payload_bytes: 4096,
        selection: Selection::AllFalse,
        projection: Projection::Narrow,
        expected_fused: true,
    },
    Case {
        name: "repeated-projection",
        shape: Shape::Repeated,
        rows: 4096,
        payload_bytes: 0,
        selection: Selection::MixedNullable,
        projection: Projection::Repeated,
        expected_fused: true,
    },
    Case {
        name: "wide-distinct-256",
        shape: Shape::WideDistinct,
        rows: 512,
        payload_bytes: 0,
        selection: Selection::MixedNullable,
        projection: Projection::WideDistinct,
        expected_fused: true,
    },
    Case {
        name: "group-float-sum-avg",
        shape: Shape::GroupFloat,
        rows: 2048,
        payload_bytes: 0,
        selection: Selection::MixedNullable,
        projection: Projection::Narrow,
        expected_fused: true,
    },
    Case {
        name: "narrow-allfalse-control",
        shape: Shape::Narrow,
        rows: 2048,
        payload_bytes: 0,
        selection: Selection::AllFalse,
        projection: Projection::Narrow,
        expected_fused: true,
    },
    Case {
        name: "dictionary-discard-fallback",
        shape: Shape::DictionaryFallback,
        rows: 512,
        payload_bytes: 0,
        selection: Selection::MixedNullable,
        projection: Projection::Narrow,
        expected_fused: false,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EngineMode {
    Candidate,
    ForceEager,
}

impl EngineMode {
    pub(super) fn from_env() -> Self {
        match std::env::var("FILTER_PROJECT_MODE").as_deref() {
            Ok("candidate") => Self::Candidate,
            Ok("force-eager") => Self::ForceEager,
            other => panic!("FILTER_PROJECT_MODE must be candidate|force-eager, got {other:?}"),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::ForceEager => "force-eager",
        }
    }

    pub(super) fn guard(self) -> ForceEagerGuard {
        let old = crate::core::graph::FORCE_DENSE_FILTER_PROJECT
            .with(|flag| flag.replace(self == Self::ForceEager));
        ForceEagerGuard(old)
    }
}

pub(super) struct ForceEagerGuard(bool);

impl Drop for ForceEagerGuard {
    fn drop(&mut self) {
        crate::core::graph::FORCE_DENSE_FILTER_PROJECT.with(|flag| flag.set(self.0));
    }
}

pub(super) struct CaseSpec {
    pub case: Case,
    pub plan: Plan,
    pub predicate_column: usize,
    pub source_payload_bytes: usize,
    pub group_float_contract: Vec<u64>,
}

pub(super) struct GeneratedInputs {
    pub seed: ZSetBatch,
    pub change: ZSetBatch,
    pub continuation: ZSetBatch,
}

fn predicate_column_for(case: Case) -> usize {
    if matches!(case.shape, Shape::GroupFloat) {
        2
    } else {
        1
    }
}

pub(super) fn case_spec(case: Case) -> CaseSpec {
    let predicate_column = predicate_column_for(case);
    let plan = plan(case, predicate_column);
    CaseSpec {
        case,
        plan,
        predicate_column,
        source_payload_bytes: case.rows * case.payload_bytes,
        group_float_contract: vec![
            (9_007_199_254_740_992.0_f64).to_bits(),
            1.0_f64.to_bits(),
            (-9_007_199_254_740_992.0_f64).to_bits(),
        ],
    }
}

pub(super) fn generated_inputs(spec: &CaseSpec) -> GeneratedInputs {
    let case = spec.case;
    let data_rows = if matches!(case.shape, Shape::GroupFloat) {
        12
    } else {
        case.rows
    };
    let ids = (0..data_rows as i64).collect::<Vec<_>>();
    let mut seed = batch(case, &ids, None);
    let diff = if matches!(case.shape, Shape::GroupFloat) {
        vec![1; ids.len()]
    } else {
        ids.iter()
            .enumerate()
            .map(|(row, _)| match row % 3 {
                0 => -1,
                1 => 0,
                _ => 1,
            })
            .collect()
    };
    seed.diff = Arc::new(Int64Array::from(vec![1; ids.len()]));
    let continuation_ids = if matches!(case.selection, Selection::AllFalse) {
        vec![(case.rows * 2 + 101) as i64]
    } else if matches!(case.shape, Shape::GroupFloat) {
        vec![0, 1, 2, 3, 4, 5]
    } else {
        vec![(case.rows * 2 + 5001) as i64]
    };
    let continuation = batch(case, &continuation_ids, Some(case.rows * 2));
    if matches!(case.shape, Shape::GroupFloat) {
        let mask = hotlap_core::Predicate::Cmp {
            op: hotlap_core::CmpOp::Eq,
            col: spec.predicate_column,
            scalar: hotlap_core::Scalar::I64(1),
        }
        .eval(&seed.batch)
        .unwrap();
        let selected = mask.iter().filter(|item| *item == Some(true)).count();
        assert_eq!(selected, 3, "exactly M, 1, -M must be retained");
        let float_values = [
            Some((1_i64 << 53) as f64),
            None,
            None,
            Some(1.0),
            None,
            None,
            Some(-((1_i64 << 53) as f64)),
            None,
            None,
            Some((1_i64 << 53) as f64),
            None,
            None,
        ];
        let values = Arc::new(Float64Array::from(float_values.to_vec())) as ArrayRef;
        let mut seed_columns = seed.batch.columns().to_vec();
        seed_columns[3] = values.clone();
        let batch = RecordBatch::try_new(seed.batch.schema(), seed_columns).unwrap();
        seed.batch = batch;
        assert_eq!(
            [0, 3, 6].map(|index| mask.iter().nth(index).unwrap()),
            [Some(true); 3],
            "M, 1, -M operands must be retained"
        );
        assert!(mask.iter().any(|item| item == Some(false)));
        assert!(mask.iter().any(|item| item.is_none()));
        let m = (1_i64 << 53) as f64;
        let negative_m = -m;
        assert_ne!(
            (m + 1.0 + negative_m).to_bits(),
            (m + negative_m + 1.0).to_bits()
        );
    }
    let change = ZSetBatch::new(seed.batch.clone(), Arc::new(Int64Array::from(diff))).unwrap();
    GeneratedInputs {
        seed,
        change,
        continuation,
    }
}

fn predicate_value(selection: Selection, id: i64) -> Option<i64> {
    match selection {
        Selection::AllTrue => Some(1),
        Selection::AllFalse => Some(0),
        Selection::MixedNullable if id >= 5000 => Some(1),
        Selection::MixedNullable => {
            if id >= 5001 {
                Some(1)
            } else {
                match id.unsigned_abs() % 3 {
                    0 => Some(1),
                    1 => Some(0),
                    _ => None,
                }
            }
        }
    }
}

fn batch(case: Case, ids: &[i64], retraction_base: Option<usize>) -> ZSetBatch {
    let mut id_values = Vec::with_capacity(ids.len());
    let mut flags = Vec::with_capacity(ids.len());
    let mut keys = Vec::with_capacity(ids.len());
    let mut floats = Vec::with_capacity(ids.len());
    let mut diffs = Vec::with_capacity(ids.len());
    for &id in ids {
        let retract = id < 0;
        let row_id = if retract {
            id.unsigned_abs() as usize - 1
        } else {
            id as usize
        };
        id_values.push(row_id as i64);
        flags.push(
            if (matches!(case.selection, Selection::AllFalse) && retraction_base.is_some()) {
                Some(1)
            } else if matches!(case.shape, Shape::GroupFloat) && row_id == 9 {
                None
            } else {
                predicate_value(case.selection, row_id as i64)
            },
        );
        keys.push(if matches!(case.shape, Shape::GroupFloat) {
            0
        } else {
            (row_id % 8) as i64
        });
        floats.push(match row_id % 3 {
            0 => Some((1_i64 << 53) as f64),
            1 => Some(1.0),
            _ => Some(-((1_i64 << 53) as f64)),
        });
        diffs.push(if retract {
            -1
        } else if retraction_base.is_some() && row_id % 17 == 0 {
            0
        } else {
            1
        });
    }
    let (schema, mut columns) = match case.shape {
        Shape::Narrow | Shape::Repeated => (
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("predicate", DataType::Int64, true),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(id_values.clone())) as ArrayRef,
                Arc::new(Int64Array::from(flags.clone())) as ArrayRef,
                Arc::new(Int64Array::from(
                    id_values
                        .iter()
                        .map(|id| id.wrapping_mul(3))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        ),
        Shape::WideDiscardedUtf8 => {
            let mut fields = vec![
                Field::new("id", DataType::Int64, false),
                Field::new("predicate", DataType::Int64, true),
                Field::new("value", DataType::Int64, false),
                Field::new("discarded_wide_utf8", DataType::Utf8, false),
            ];
            let payload = id_values
                .iter()
                .map(|id| {
                    format!(
                        "{id:016x}{}",
                        "x".repeat(case.payload_bytes.saturating_sub(16))
                    )
                })
                .collect::<Vec<_>>();
            let arrays = vec![
                Arc::new(Int64Array::from(id_values.clone())) as ArrayRef,
                Arc::new(Int64Array::from(flags.clone())) as ArrayRef,
                Arc::new(Int64Array::from(
                    id_values
                        .iter()
                        .map(|id| id.wrapping_mul(3))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(payload)) as ArrayRef,
            ];
            (Arc::new(Schema::new(std::mem::take(&mut fields))), arrays)
        }
        Shape::WideDistinct => {
            let mut fields = vec![
                Field::new("id", DataType::Int64, false),
                Field::new("predicate", DataType::Int64, true),
            ];
            let mut arrays = vec![
                Arc::new(Int64Array::from(id_values.clone())) as ArrayRef,
                Arc::new(Int64Array::from(flags.clone())) as ArrayRef,
            ];
            for column in 0..256 {
                fields.push(Field::new(
                    format!("value_{column}"),
                    DataType::Int64,
                    false,
                ));
                arrays.push(Arc::new(Int64Array::from(
                    id_values
                        .iter()
                        .map(|id| id.wrapping_add(column as i64))
                        .collect::<Vec<_>>(),
                )) as ArrayRef);
            }
            (Arc::new(Schema::new(fields)), arrays)
        }
        Shape::DictionaryFallback => {
            let mut dictionary = StringDictionaryBuilder::<Int8Type>::new();
            for id in &id_values {
                dictionary.append(format!("category-{}", id % 8)).unwrap();
            }
            let dictionary = Arc::new(dictionary.finish()) as ArrayRef;
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("predicate", DataType::Int64, true),
                Field::new("value", DataType::Int64, false),
                Field::new(
                    "discarded_dictionary",
                    dictionary.data_type().clone(),
                    false,
                ),
            ]));
            (
                schema,
                vec![
                    Arc::new(Int64Array::from(id_values.clone())) as ArrayRef,
                    Arc::new(Int64Array::from(flags.clone())) as ArrayRef,
                    Arc::new(Int64Array::from(
                        id_values
                            .iter()
                            .map(|id| id.wrapping_mul(3))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    dictionary,
                ],
            )
        }
        Shape::GroupFloat => (
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("key", DataType::Int64, false),
                Field::new("predicate", DataType::Int64, true),
                Field::new("value", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(id_values)) as ArrayRef,
                Arc::new(Int64Array::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(flags)) as ArrayRef,
                Arc::new(Float64Array::from(floats)) as ArrayRef,
            ],
        ),
    };
    let batch = RecordBatch::try_new(schema, std::mem::take(&mut columns)).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn plan(case: Case, predicate_column: usize) -> Plan {
    let filtered = Plan::Filter {
        input: Box::new(Plan::Source(InputId(0))),
        pred: hotlap_core::Predicate::Cmp {
            op: hotlap_core::CmpOp::Eq,
            col: predicate_column,
            scalar: hotlap_core::Scalar::I64(1),
        },
    };
    if matches!(case.shape, Shape::GroupFloat) {
        return Plan::GroupAggregate {
            input: Box::new(Plan::Project {
                input: Box::new(filtered),
                cols: vec![1, 3],
            }),
            key: vec![0],
            aggs: vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)],
        };
    }
    let cols = match case.projection {
        Projection::Narrow => vec![0, 2],
        Projection::Repeated => vec![2, 0, 2, 2],
        Projection::WideDistinct => (2..258).rev().chain([17, 255, 2, 17]).collect(),
    };
    Plan::Project {
        input: Box::new(filtered),
        cols,
    }
}

impl CaseSpec {
    pub(super) fn projected_columns(&self) -> Vec<usize> {
        match self.case.shape {
            Shape::GroupFloat => vec![1, 3],
            _ => match self.case.projection {
                Projection::Narrow => vec![0, 2],
                Projection::Repeated => vec![2, 0, 2, 2],
                Projection::WideDistinct => (2..258).rev().chain([17, 255, 2, 17]).collect(),
            },
        }
    }

    pub(super) fn encode_zset(&self, zset: &ZSetBatch) -> Vec<u8> {
        use arrow::ipc::writer::StreamWriter;
        let mut fields = zset.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Field::new("__diff", DataType::Int64, false).into());
        let mut columns = zset.batch.columns().to_vec();
        columns.push(zset.diff.clone());
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        bytes
    }

    pub(super) fn encode_checkpoint(&self, snapshot: &EngineSnapshot) -> Vec<u8> {
        bincode::serialize(snapshot).unwrap()
    }

    pub(super) fn group_float_bits(&self, snapshot: &EngineSnapshot) -> Vec<(u64, u64)> {
        if !matches!(self.case.shape, Shape::GroupFloat) {
            return Vec::new();
        }
        snapshot.views[0]
            .operators
            .iter()
            .flatten()
            .filter_map(|state| match state {
                OperatorState::Group(group) => Some(group),
                _ => None,
            })
            .flat_map(|group| group.groups.iter())
            .map(|(_, entry)| {
                let sum = match entry.values[1] {
                    AggValue::SumFloat { sum, .. } => sum.to_bits(),
                    ref other => panic!("expected float SUM, got {other:?}"),
                };
                let avg = match entry.values[2] {
                    AggValue::Avg { sum, .. } => sum.to_bits(),
                    ref other => panic!("expected AVG, got {other:?}"),
                };
                (sum, avg)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_inputs_release_owned_source_arrays_when_dropped() {
        let spec = case_spec(CASES[2]);
        let inputs = generated_inputs(&spec);
        let weak = Arc::downgrade(inputs.seed.batch.column(3));
        assert!(weak.upgrade().is_some());
        drop(inputs);
        assert!(
            weak.upgrade().is_none(),
            "generated input owner retained source array"
        );
    }

    #[test]
    fn seed_and_change_share_base_columns_but_own_distinct_diffs() {
        let spec = case_spec(CASES[2]);
        let inputs = generated_inputs(&spec);
        assert_eq!(inputs.seed.len(), CASES[2].rows);
        assert_eq!(inputs.change.len(), CASES[2].rows);
        for (seed, change) in inputs
            .seed
            .batch
            .columns()
            .iter()
            .zip(inputs.change.batch.columns())
        {
            assert!(Arc::ptr_eq(seed, change));
        }
        assert!(!Arc::ptr_eq(&inputs.seed.diff, &inputs.change.diff));
    }

    #[test]
    fn declared_extreme_masks_match_generated_input() {
        for (index, expected) in [(3, true), (4, false)] {
            let case = CASES[index];
            let spec = case_spec(case);
            let inputs = generated_inputs(&spec);
            let mask = hotlap_core::Predicate::Cmp {
                op: hotlap_core::CmpOp::Eq,
                col: spec.predicate_column,
                scalar: hotlap_core::Scalar::I64(1),
            }
            .eval(&inputs.change.batch)
            .unwrap();
            assert_eq!(
                mask.iter().filter(|item| *item == Some(true)).count(),
                if expected { case.rows } else { 0 }
            );
        }
    }
}
