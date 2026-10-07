//! Hotlap SQL: embedded SQL/DDL surface over the kernel and connectors.
#![forbid(unsafe_code)]

pub mod catalog;
pub mod convert;
pub mod ddl;
mod ddl_scan;
pub mod error;
pub mod mv_provider;
mod mv_schema;
pub mod session;
mod session_source;
pub mod translate;
mod translate_expr;
mod tumble;
pub mod watermark;

pub use catalog::{Catalog, MvDef, SourceDef};
pub use ddl::CreateSink;
pub use error::SqlError;
pub use mv_provider::MvTableProvider;
pub use session::{
    FlussSinkFactory, FlussSourceFactory, QueryResult, SinkFactory, Snapshotter, SourceFactory,
    SqlSession,
};
