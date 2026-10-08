//! Versioned export/import of [`TumbleCount`]'s open windows and progress.

use hotlap_core::snapshot::{WindowBucket, WindowState};

use super::TumbleCount;
use crate::core::ipc::{decode_schema, encode_schema};
use crate::error::EngineError;
use crate::keys::converter_for;

impl TumbleCount {
    /// Exports the schema, progress counters and every open bucket.
    pub(crate) fn export_state(&self) -> Result<WindowState, EngineError> {
        let schema = match &self.schema {
            Some(schema) => Some(encode_schema(schema)?),
            None => None,
        };
        let mut open = Vec::new();
        for (start, buckets) in &self.windows {
            for (key, (_, count)) in buckets {
                open.push(WindowBucket {
                    start: *start,
                    key: key.clone(),
                    count: *count,
                });
            }
        }
        Ok(WindowState {
            schema,
            watermark: self.watermark,
            dropped_late: self.dropped_late,
            dropped_closed: self.dropped_closed,
            open,
        })
    }

    /// Restores the schema, counters and open buckets.
    pub(crate) fn import_state(&mut self, state: &WindowState) -> Result<(), EngineError> {
        self.windows.clear();
        self.watermark = state.watermark;
        self.dropped_late = state.dropped_late;
        self.dropped_closed = state.dropped_closed;
        let Some(bytes) = &state.schema else {
            self.converter = None;
            self.schema = None;
            return Ok(());
        };
        let schema = decode_schema(bytes)?;
        let converter = converter_for(&schema, &self.key)?;
        for bucket in &state.open {
            let key = converter.parser().parse(&bucket.key).owned();
            let entry = self.windows.entry(bucket.start).or_default();
            entry.insert(bucket.key.clone(), (key, bucket.count));
        }
        self.converter = Some(converter);
        self.schema = Some(schema);
        Ok(())
    }
}
