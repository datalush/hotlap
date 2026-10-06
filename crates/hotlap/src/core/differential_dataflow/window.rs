//! Operador de ventana tumbling: estado por cubo, emisión al cerrar y GC.

use std::collections::HashMap;

use differential_dataflow::Collection;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;

use super::session::TIME_SCALE;
use crate::row::{Row, Scalar};

type Container = Vec<(Row, u64, isize)>;

/// Agrega por `key` y ventana tumbling de `size` sobre `time_col`. Emite
/// `key ++ [window_start, count]` una vez por ventana cerrada; libera el cubo.
/// El tiempo de DD está escalado: `t = wm_lógico * TIME_SCALE + tick`; el cierre
/// se registra en `(window_start + size) * TIME_SCALE - 1`, que dispara cuando la
/// frontera alcanza ese punto, i.e. `wm_lógico >= window_end`.
pub(super) fn tumble_count<'scope>(
    input: &Collection<'scope, u64, Container>,
    key: &[usize],
    time_col: usize,
    size: u64,
) -> Collection<'scope, u64, Container> {
    let key = key.to_vec();
    let mut windows: HashMap<(Row, u64), i64> = HashMap::new();
    let inner = input
        .inner
        .clone()
        .unary_notify::<CapacityContainerBuilder<Container>, _, _>(
            Pipeline,
            "tumble_count",
            None::<u64>,
            move |input, output, notificator| {
                let out_idx = output.output_index();
                input.for_each_time(|cap, data| {
                    let mut pending: Vec<u64> = Vec::new();
                    for container in data {
                        for (row, _ts, diff) in container.drain(..) {
                            let (group, ws) = bucket(&row, &key, time_col, size);
                            *windows.entry((group, ws)).or_insert(0) += diff as i64;
                            pending.push(close_time(ws, size));
                        }
                    }
                    for t in pending {
                        notificator.notify_at(cap.delayed(&t, out_idx));
                    }
                });
                notificator.for_each(|cap, _count, _not| {
                    let t = *cap.time();
                    let wm = (t + 1) / TIME_SCALE;
                    let mut session = output.session(&cap);
                    for bucket in closed_buckets(&windows, wm, size) {
                        if let Some(count) = windows.remove(&bucket)
                            && count != 0
                        {
                            let (group, ws) = bucket;
                            let mut row = group.0;
                            row.push(Scalar::I64(ws as i64));
                            row.push(Scalar::I64(count));
                            session.give((Row(row), t, 1isize));
                        }
                    }
                });
            },
        );
    Collection { inner }
}

/// Bucket a record as `(key_row, window_start)` for a `size`-wide tumbling window.
fn bucket(row: &Row, key: &[usize], time_col: usize, size: u64) -> (Row, u64) {
    let event_ts = match row.col(time_col) {
        Scalar::I64(v) => v.max(0) as u64,
        _ => 0,
    };
    (Row(key.iter().map(|&c| row.col(c)).collect()), (event_ts / size) * size)
}

/// The DD time at which the window starting at `window_start` closes. Saturating:
/// an event-time near `u64::MAX` must not wrap the scaled frontier (a wrapped
/// `t` below the input capability would make `cap.delayed` panic).
fn close_time(window_start: u64, size: u64) -> u64 {
    window_start
        .saturating_add(size)
        .saturating_mul(TIME_SCALE)
        .saturating_sub(1)
}

/// `(key_row, window_start)` pairs whose end the watermark `wm` has reached.
fn closed_buckets(windows: &HashMap<(Row, u64), i64>, wm: u64, size: u64) -> Vec<(Row, u64)> {
    windows
        .keys()
        .filter(|(_, ws)| ws.saturating_add(size) <= wm)
        .cloned()
        .collect()
}
