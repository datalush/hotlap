//! Hotlap connectors: source/sink SPI, Fluss source and DataFusion adapter.
#![forbid(unsafe_code)]

pub mod error;
pub mod sink;
pub mod source;

pub use error::ConnectorError;
pub use sink::{ChangeStream, Sink};
pub use source::{Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId};
