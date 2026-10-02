// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::client::WriteRecord;
use crate::client::admin::FlussAdmin;
use crate::client::metadata::Metadata;
use crate::client::table::read_context_resolver::ReadContextResolver;
use crate::metadata::{DataTypes, PhysicalTablePath, Schema, TableInfo, TablePath};
use crate::proto::{PbFetchLogRespForBucket, PbFetchLogRespForTable};
use crate::record::MemoryLogRecordsArrowBuilder;
use crate::row::{Datum, GenericRow};
use crate::rpc::FlussError;
use crate::test_utils::{
    assert_scanner_entries_labeled, build_cluster_arc, build_table_info, test_scanner_metrics,
    uncompressed_arrow_batch_config,
};

fn test_admin(metadata: &Arc<Metadata>) -> Arc<FlussAdmin> {
    Arc::new(FlussAdmin::new(
        Arc::new(RpcClient::new()),
        metadata.clone(),
    ))
}

fn test_schema_getter(table_info: &TableInfo, metadata: &Arc<Metadata>) -> Arc<ClientSchemaGetter> {
    let latest = SchemaInfo::new(table_info.get_schema().clone(), table_info.get_schema_id());
    Arc::new(ClientSchemaGetter::new(
        table_info.table_path.clone(),
        test_admin(metadata),
        latest,
    ))
}

fn test_resolver(table_info: &TableInfo) -> Arc<ReadContextResolver> {
    let row_type = table_info.get_row_type();
    let arrow_schema = to_arrow_schema(row_type).unwrap();
    let row_type_arc = Arc::new(row_type.clone());
    let local_ctx = Arc::new(
        ReadContext::new(arrow_schema.clone(), row_type_arc.clone(), false)
            .with_fluss_row_type(row_type_arc.clone()),
    );
    let remote_ctx = Arc::new(
        ReadContext::new(arrow_schema, row_type_arc.clone(), true)
            .with_fluss_row_type(row_type_arc),
    );
    Arc::new(ReadContextResolver::new(
        table_info.get_schema_id() as i16,
        local_ctx,
        remote_ctx,
        None,
    ))
}

fn build_records(table_info: &TableInfo, table_path: Arc<TablePath>) -> Result<Vec<u8>> {
    let mut builder = MemoryLogRecordsArrowBuilder::new(
        uncompressed_arrow_batch_config(1, table_info.get_row_type(), usize::MAX),
        false,
    )?;
    let physical_table_path = Arc::new(PhysicalTablePath::of(table_path));
    let row = GenericRow {
        values: vec![Datum::Int32(1)],
    };
    let record =
        WriteRecord::for_append(Arc::new(table_info.clone()), physical_table_path, 1, &row);
    builder.append(&record)?;
    builder.build()
}

/// Builds the fetcher used by the filter tests, encoding `predicate` the way
/// `TableScan::filter` does.
fn filtering_fetcher(
    table_info: &TableInfo,
    metadata: &Arc<Metadata>,
    status: Arc<LogScannerStatus>,
    predicate: Option<Predicate>,
) -> Result<LogFetcher> {
    let filter = predicate
        .map(|p| to_pb_predicate(&p, table_info.get_row_type()))
        .transpose()?;
    LogFetcher::new(
        table_info.clone(),
        Arc::new(RpcClient::new()),
        metadata.clone(),
        status,
        &Config::default(),
        None,
        false,
        filter,
        test_scanner_metrics(&table_info.table_path),
        test_schema_getter(table_info, metadata),
    )
}

/// A response for bucket 0 of table 1 that carries no records, standing in
/// for a fetch whose batches the server pruned entirely.
fn filtered_response(
    filtered_end_offset: Option<i64>,
    high_watermark: Option<i64>,
) -> FetchLogResponse {
    FetchLogResponse {
        tables_resp: vec![PbFetchLogRespForTable {
            table_id: 1,
            buckets_resp: vec![PbFetchLogRespForBucket {
                partition_id: None,
                bucket_id: 0,
                error_code: None,
                error_message: None,
                high_watermark,
                log_start_offset: None,
                remote_log_fetch_info: None,
                records: None,
                filtered_end_offset,
                min_retain_offset: None,
            }],
        }],
    }
}

fn test_response_context(fetcher: &LogFetcher, metadata: &Arc<Metadata>) -> FetchResponseContext {
    FetchResponseContext {
        metadata: metadata.clone(),
        log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
        log_scanner_status: fetcher.log_scanner_status.clone(),
        resolver: Arc::clone(&fetcher.resolver),
        remote_log_downloader: fetcher.remote_log_downloader.clone(),
        metrics: Arc::clone(&fetcher.metrics),
        request_start_time: Instant::now(),
    }
}

fn create_test_table_info(
    has_primary_key: bool,
    log_format: Option<&str>,
) -> (TableInfo, TablePath) {
    let mut schema_builder = Schema::builder()
        .column("id", DataTypes::int())
        .column("name", DataTypes::string());

    if has_primary_key {
        schema_builder = schema_builder.primary_key(vec!["id"]).unwrap();
    }

    let schema = schema_builder.build().unwrap();
    let table_path = TablePath::new("test_db", "test_table");

    let mut properties = HashMap::new();
    if let Some(format) = log_format {
        properties.insert("table.log.format".to_string(), format.to_string());
    }

    let table_info = TableInfo::new(
        table_path.clone(),
        1,
        1,
        schema,
        vec![],
        Arc::from(vec![]),
        1,
        properties,
        HashMap::new(),
        None,
        0,
        0,
    );

    (table_info, table_path)
}

/// Builds a self-contained `LogScannerInner` for poll-timing tests
/// inside a `current_thread` runtime so callers can drive `PollGuard`
/// lifecycles synchronously.
fn with_test_log_scanner_inner<F: FnOnce(&LogScannerInner)>(body: F) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current_thread runtime");
    rt.block_on(async {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let rpc_client = Arc::new(RpcClient::new());
        let admin = Arc::new(crate::client::admin::FlussAdmin::new(
            rpc_client.clone(),
            metadata.clone(),
        ));
        let inner = LogScannerInner::new(
            &table_info,
            metadata,
            rpc_client,
            &Config::default(),
            None,
            false,
            None,
            admin,
        )
        .expect("build LogScannerInner");
        body(&inner);
    });
}

fn snapshot_gauge(snapshotter: &metrics_util::debugging::Snapshotter, name: &str) -> Option<f64> {
    use metrics_util::debugging::DebugValue;
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, val)| {
            if key.key().name() == name {
                if let DebugValue::Gauge(g) = val {
                    return Some(g.into_inner());
                }
            }
            None
        })
}

mod checks;
mod fetching;
mod timing;
