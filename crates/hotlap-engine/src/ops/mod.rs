//! Stateless operators over Z-sets: filtering, projection, and keyed counting.

pub mod filter_project;
pub mod groupcount;

pub use filter_project::{filter, project};
pub use groupcount::group_count;
