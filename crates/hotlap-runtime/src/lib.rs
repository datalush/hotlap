//! Hotlap runtime: the kernel composition root, engine thread and checkpointing.
//!
//! This crate owns the choice of engine kernel ([`hotlap_engine::EngineCore`])
//! and wires the generic connectors SPI ([`hotlap_connectors`]) into a running
//! engine, so the connector crate only knows the `Source`/`Sink` contracts.
//! It also hosts the embedded [`Session`] API over the SQL layer.
#![forbid(unsafe_code)]

pub mod error;
pub mod runtime;
pub mod session;
pub mod session_config;

pub use error::SessionError;
pub use session::{
    FlussSinkFactory, FlussSourceFactory, QueryResult, Session, SinkFactory, Snapshotter,
    SourceFactory, SqlSession,
};
pub use session_config::SessionConfig;
