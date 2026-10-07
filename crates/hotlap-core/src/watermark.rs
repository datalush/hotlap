//! Event-time watermark declaration for a source.

use serde::{Deserialize, Serialize};

/// Watermark declaration for a source: event-time column and lag.
///
/// The watermark follows `max(event_ts) - lag`, clamped at zero and monotonic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkSpec {
    /// Column holding the event timestamp.
    pub time_col: usize,
    /// Allowed lateness, in event-time units.
    pub lag: i64,
}
