//! Hotlap SQL: embedded SQL/DDL surface over the kernel and connectors.
#![forbid(unsafe_code)]

pub mod catalog;
pub mod error;
pub mod watermark;

pub use catalog::{Catalog, MvDef, SourceDef};
pub use error::SqlError;
