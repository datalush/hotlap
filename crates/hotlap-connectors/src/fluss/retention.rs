//! Broker retention probe: earliest retained offset per bucket.

use std::collections::HashMap;

use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss::rpc::message::OffsetSpec;

/// Ask the broker for the earliest retained offset of each bucket.
///
/// Uses `FlussAdmin::list_offsets` with `OffsetSpec::Earliest` (the Rust client
/// has no dedicated `list_earliest_offsets` helper). Returns `None` when the
/// admin or the RPC is unavailable, so recovery treats retention as unverified.
pub(crate) async fn fetch_earliest(
    connection: &FlussConnection,
    table_path: &TablePath,
    buckets: &[i32],
) -> Option<HashMap<i32, i64>> {
    let admin = connection.get_admin().ok()?;
    admin
        .list_offsets(table_path, buckets, OffsetSpec::Earliest)
        .await
        .ok()
}
