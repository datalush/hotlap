//! Hotlap runtime: the kernel composition root, engine thread and checkpointing.
//!
//! This crate owns the choice of engine kernel ([`hotlap_engine::EngineCore`])
//! and wires the generic connectors SPI ([`hotlap_connectors`]) into a running
//! engine, so the connector crate only knows the `Source`/`Sink` contracts.
#![forbid(unsafe_code)]

pub mod runtime;
