//! Generic checkpoint/feed helpers shared by the recovery tests.

use arrow::array::Int64Array;
use futures::StreamExt;

use hotlap_runtime::runtime::checkpoint::Checkpointer;
use hotlap_runtime::runtime::pipeline;
use hotlap_runtime::runtime::sources::{InputStream, Sources};

/// Take a checkpoint synchronously and return its id.
pub fn take(checkpointer: &mut Checkpointer, engine: &hotlap::Hotlap, sources: &Sources) -> u64 {
    futures::executor::block_on(checkpointer.take(engine, sources)).unwrap()
}

/// Push up to `limit` readable events from `stream` into `hotlap`, acking each
/// one through the source that produced it once it is applied (mirrors runtime).
pub fn drain(
    hotlap: &mut hotlap::Hotlap,
    sources: &Sources,
    stream: &mut InputStream,
    limit: usize,
) {
    let mut pushed = 0;
    while pushed < limit {
        match futures::executor::block_on(stream.next()) {
            Some(Ok(event)) => {
                pipeline::ingest_event(hotlap, sources, &event).unwrap();
                sources
                    .get(event.input)
                    .unwrap()
                    .source
                    .commit(event.batch.split, event.batch.next_offset)
                    .unwrap();
                pushed += 1;
            }
            Some(Err(error)) => panic!("unexpected source error: {error}"),
            None => break,
        }
    }
}

/// Materialize a Z-set as sorted integer rows.
pub fn rows(zset: &hotlap::ZSetBatch) -> Vec<Vec<i64>> {
    let columns: Vec<&Int64Array> = zset
        .batch
        .columns()
        .iter()
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect();
    let mut out: Vec<Vec<i64>> = (0..zset.len())
        .map(|row| columns.iter().map(|column| column.value(row)).collect())
        .collect();
    out.sort();
    out
}
