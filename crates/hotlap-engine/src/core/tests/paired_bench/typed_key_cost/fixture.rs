use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, Int16Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Int16Type, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec};

use crate::core::EngineCore;

pub(super) const KEYS: usize = 8192;

fn input(case: &str) -> (RecordBatch, Arc<Schema>, Plan, bool) {
    let indexed = |data_type: DataType, array: ArrayRef| {
        let schema = Arc::new(Schema::new(vec![Field::new("key", data_type, true)]));
        (
            RecordBatch::try_new(schema.clone(), vec![array]).unwrap(),
            schema,
        )
    };
    let integers = || Int64Array::from_iter_values(0..KEYS as i64);
    let (batch, schema) = match case {
        "scalar-group" | "scalar-window" => indexed(DataType::Int64, Arc::new(integers())),
        "dictionary-window" => {
            let values = Arc::new(StringArray::from(
                (0..100)
                    .map(|i| Some(format!("key-{i:05}")))
                    .collect::<Vec<_>>(),
            ));
            let data_type =
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
            let keys = Int16Array::from_iter_values(0..100_i16);
            indexed(
                data_type,
                Arc::new(DictionaryArray::<Int16Type>::try_new(keys, values).unwrap()),
            )
        }
        "struct-group" => {
            let fields: arrow::datatypes::Fields =
                vec![Field::new("part", DataType::Int64, false)].into();
            let array =
                StructArray::try_new(fields.clone(), vec![Arc::new(integers())], None).unwrap();
            indexed(DataType::Struct(fields), Arc::new(array))
        }
        _ => panic!("unsupported TYPED_KEY_COST_CASE={case}"),
    };
    let windowed = case.ends_with("window");
    let (batch, schema) = if windowed {
        let schema = Arc::new(Schema::new(vec![
            schema.field(0).clone(),
            Field::new("time", DataType::Int64, false),
        ]));
        let time = Arc::new(Int64Array::from(vec![1_i64; batch.num_rows()])) as ArrayRef;
        (
            RecordBatch::try_new(schema.clone(), vec![batch.column(0).clone(), time]).unwrap(),
            schema,
        )
    } else {
        (batch, schema)
    };
    let source = Box::new(Plan::Source(InputId(0)));
    let plan = if windowed {
        Plan::TumbleCount {
            input: source,
            key: vec![0],
            time_col: 1,
            size: 10,
        }
    } else {
        Plan::GroupAggregate {
            input: source,
            key: vec![0],
            aggs: vec![AggSpec::count()],
        }
    };
    (batch, schema, plan, windowed)
}

pub(super) fn configured(case: &str) -> (EngineCore, RecordBatch, bool) {
    let (batch, _, plan, windowed) = input(case);
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    if windowed {
        core.declare_watermark(
            InputId(0),
            WatermarkSpec {
                time_col: 1,
                lag: 100,
            },
        )
        .unwrap();
    }
    core.build_view(ViewId(0), &plan).unwrap();
    core.tap_view(ViewId(0)).unwrap();
    (core, batch, windowed)
}

pub(super) fn seed_batches(case: &str, first: &RecordBatch) -> Vec<RecordBatch> {
    if case != "dictionary-window" {
        return vec![first.clone()];
    }
    (0..KEYS)
        .step_by(100)
        .map(|start| {
            let count = (KEYS - start).min(100);
            let values = Arc::new(StringArray::from(
                (start..start + count)
                    .map(|i| Some(format!("key-{i:05}")))
                    .collect::<Vec<_>>(),
            ));
            let dictionary = DictionaryArray::<Int16Type>::try_new(
                Int16Array::from_iter_values(0..count as i16),
                values,
            )
            .unwrap();
            let columns = vec![
                Arc::new(dictionary) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_i64; count])) as ArrayRef,
            ];
            RecordBatch::try_new(first.schema(), columns).unwrap()
        })
        .collect()
}
