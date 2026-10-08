//! Hotlap SQL: DDL parsing, catalog and logical-plan translation.
//!
//! The embedded session that drives the engine lives in `hotlap-runtime`,
//! which composes this crate with the runtime and the metrics registry.

#![forbid(unsafe_code)]

pub mod catalog;
pub mod convert;
pub mod ddl;
mod ddl_scan;
pub mod error;
pub mod mv_schema;
pub mod translate;
mod translate_expr;
pub mod tumble;
pub mod watermark;

pub use catalog::{Catalog, MvDef, SourceDef};
pub use ddl::CreateSink;
pub use error::SqlError;
