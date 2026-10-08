//! Hotlap command-line interface: a REPL over the embedded session API.
//!
//! The crate is split into a library (testable REPL, rendering and bootstrap
//! wrappers) and a thin binary that parses flags and wires up a [`Session`].
#![forbid(unsafe_code)]

pub mod bootstrap;
pub mod render;
pub mod repl;
