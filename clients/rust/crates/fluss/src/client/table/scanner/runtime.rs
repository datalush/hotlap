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

//! Shared scanner construction and lifecycle for both public APIs.

use super::{
    Arc, AtomicI64, ClientSchemaGetter, Config, LogFetcher, LogScannerInner, LogScannerStatus,
    Metadata, Mutex, PbPredicate, PollState, Result, RpcClient, ScannerMetrics, SchemaInfo,
    TableInfo, spawn_last_poll_seconds_ago_ticker, to_arrow_schema,
};

impl Drop for LogScannerInner {
    fn drop(&mut self) {
        self.last_poll_seconds_ago_task.abort();
    }
}

impl LogScannerInner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        table_info: &TableInfo,
        metadata: Arc<Metadata>,
        connections: Arc<RpcClient>,
        config: &Config,
        projected_fields: Option<Vec<usize>>,
        fixed_schema: bool,
        filter: Option<PbPredicate>,
        admin: Arc<crate::client::admin::FlussAdmin>,
    ) -> Result<Self> {
        let log_scanner_status = Arc::new(LogScannerStatus::new());

        let full_row_type = table_info.get_row_type();
        let arrow_schema = match &projected_fields {
            Some(indices) => {
                let projected_fields_vec: Vec<_> = indices
                    .iter()
                    .map(|&i| full_row_type.fields()[i].clone())
                    .collect();
                let projected_row_type = crate::metadata::RowType::new(projected_fields_vec);
                to_arrow_schema(&projected_row_type)?
            }
            None => to_arrow_schema(full_row_type)?,
        };

        // Create schema getter for schema evolution support
        let latest_schema =
            SchemaInfo::new(table_info.get_schema().clone(), table_info.get_schema_id());
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            table_info.table_path.clone(),
            admin,
            latest_schema,
        ));

        let metrics = Arc::new(ScannerMetrics::new(&table_info.table_path));
        let last_poll_unix_ms = Arc::new(AtomicI64::new(0));
        let last_poll_seconds_ago_task = spawn_last_poll_seconds_ago_ticker(
            Arc::clone(&last_poll_unix_ms),
            Arc::clone(&metrics),
        );
        Ok(Self {
            table_path: table_info.table_path.clone(),
            table_id: table_info.table_id,
            num_buckets: table_info.get_num_buckets(),
            is_partitioned_table: table_info.is_partitioned(),
            metadata: metadata.clone(),
            log_scanner_status: log_scanner_status.clone(),
            log_fetcher: LogFetcher::new(
                table_info.clone(),
                connections,
                metadata,
                log_scanner_status.clone(),
                config,
                projected_fields,
                fixed_schema,
                filter,
                Arc::clone(&metrics),
                schema_getter,
            )?,
            arrow_schema,
            reader_active: std::sync::atomic::AtomicBool::new(false),
            subscription_lock: Mutex::new(()),
            poll_state: Mutex::new(PollState::default()),
            metrics,
            last_poll_unix_ms,
            last_poll_seconds_ago_task,
        })
    }
}
