//! Hotlap connectors: source/sink SPI, Fluss source and DataFusion adapter.
#![forbid(unsafe_code)]

pub mod convert;
pub mod datafusion;
pub mod error;
pub mod fluss;
pub mod runtime;
pub mod sink;
pub mod source;

pub use error::ConnectorError;
pub use sink::{ChangeStream, Sink, SinkCapabilities};
pub use source::{Offset, Source, SourceBatch, SourceState, SourceStream, Split, SplitId};
