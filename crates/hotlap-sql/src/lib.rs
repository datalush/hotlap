//! Hotlap SQL: embedded SQL/DDL surface over the kernel and connectors.
#![forbid(unsafe_code)]

pub mod catalog;
pub mod convert;
pub mod ddl;
mod ddl_scan;
pub mod error;
pub mod translate;
mod translate_expr;
pub mod watermark;

pub use catalog::{Catalog, MvDef, SourceDef};
pub use error::SqlError;
