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

//! Validate table/scan options and construct the appropriate scanner.

use super::*;

impl<'a> TableScan<'a> {
    pub fn new(conn: &'a FlussConnection, table_info: TableInfo, metadata: Arc<Metadata>) -> Self {
        Self {
            conn,
            table_info,
            metadata,
            projected_fields: None,
            fixed_schema: true,
            limit: None,
            filter: None,
        }
    }

    /// Controls how log scanners handle schema evolution.
    ///
    /// When enabled, batches written with older schemas are decoded with their
    /// write-time schema and then aligned to the schema captured when the
    /// scanner is created; missing columns are returned as nulls. This is the
    /// default for both row and batch log scanners, ensuring that a scan exposes
    /// one stable schema. When explicitly disabled, records and batches keep
    /// their write-time schema and may have different column counts across
    /// schema changes. Log-table [`LimitBatchScanner`]s require fixed-schema mode
    /// because they return all decoded log batches as one `RecordBatch`.
    pub fn with_fixed_schema(mut self, fixed_schema: bool) -> Self {
        self.fixed_schema = fixed_schema;
        self
    }

    /// Sets a row limit for the scan, enabling [`Self::create_bucket_batch_scanner`].
    ///
    /// The limit must be positive. A limit is incompatible with the log
    /// scanners, which reject it.
    pub fn limit(mut self, n: i32) -> Result<Self> {
        if n <= 0 {
            return Err(Error::IllegalArgument {
                message: format!("Scan limit must be positive, got {n}"),
            });
        }
        self.limit = Some(n);
        Ok(self)
    }

    /// Pushes `predicate` down to the log scanners, which skip whole record
    /// batches whose statistics cannot match.
    ///
    /// This only reduces what is fetched, so a scan still returns a superset of
    /// the matching rows and callers needing exact results must filter again.
    ///
    /// # Errors
    /// Returns an error if a column is missing from the table, has no schema
    /// field id, or holds a literal its declared type cannot represent exactly.
    pub fn filter(mut self, predicate: Predicate) -> Result<Self> {
        // Resolve against the full row type: the server evaluates the filter
        // before projection, so projected indices would name the wrong columns.
        self.filter = Some(to_pb_predicate(&predicate, self.table_info.get_row_type())?);
        Ok(self)
    }

    /// Batch scanners have no predicate field in their request; reject a
    /// configured filter rather than silently ignoring it.
    fn reject_filter(&self, scanner: &str) -> Result<()> {
        if self.filter.is_some() {
            return Err(Error::UnsupportedOperation {
                message: format!(
                    "{scanner} doesn't support filter pushdown. Table: {}",
                    self.table_info.table_path
                ),
            });
        }
        Ok(())
    }

    /// Log scanners don't support limit pushdown; reject a configured limit
    /// rather than silently ignoring it.
    fn reject_limit(&self, scanner: &str) -> Result<()> {
        if let Some(limit) = self.limit {
            return Err(Error::UnsupportedOperation {
                message: format!(
                    "{scanner} doesn't support limit pushdown. Table: {}, requested limit: {limit}",
                    self.table_info.table_path
                ),
            });
        }
        Ok(())
    }

    /// Creates a one-shot bounded scan of `table_bucket`.
    ///
    /// Requires a previously-configured limit via [`Self::limit`]. Creation is
    /// cheap; the `LimitScanRequest` runs on the first
    /// [`LimitBatchScanner::next_batch`].
    pub fn create_bucket_batch_scanner(
        self,
        table_bucket: TableBucket,
    ) -> Result<LimitBatchScanner> {
        self.reject_filter("BatchScanner")?;
        let limit = self.limit.ok_or_else(|| Error::IllegalArgument {
            message: "create_bucket_batch_scanner requires a limit configured via .limit(n)"
                .to_string(),
        })?;
        validate_limit_scan_fixed_schema(&self.table_info, self.fixed_schema)?;
        if table_bucket.table_id() != self.table_info.table_id {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Bucket table_id {} does not match scan table_id {}",
                    table_bucket.table_id(),
                    self.table_info.table_id
                ),
            });
        }
        let num_buckets = self.table_info.get_num_buckets();
        if table_bucket.bucket_id() < 0 || table_bucket.bucket_id() >= num_buckets {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Bucket id {} out of range for table with {num_buckets} buckets",
                    table_bucket.bucket_id()
                ),
            });
        }
        // Log tables decode as Arrow IPC, so only ARROW format is supported (KV
        // tables use the value-record path and are exempt).
        if !self.table_info.has_primary_key() {
            validate_scan_support(&self.table_info.table_path, &self.table_info)?;
        }
        // Pre-seed the current schema; older versions are fetched lazily while
        // decoding log or KV batches. Mirrors `Table::new_lookup`.
        let latest = SchemaInfo::new(
            self.table_info.get_schema().clone(),
            self.table_info.get_schema_id(),
        );
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            self.table_info.table_path.clone(),
            self.conn.get_admin()?,
            latest,
        ));
        Ok(LimitBatchScanner::new(
            self.conn.get_connections(),
            self.metadata.clone(),
            self.table_info,
            schema_getter,
            self.projected_fields,
            table_bucket,
            limit,
        ))
    }

    /// Opens a bounded, server-side snapshot scan for one KV bucket. The first
    /// request runs when the caller asks for its first Arrow batch. No SQL
    /// filter or row limit is pushed into this scan.
    pub fn create_kv_batch_scanner(self, bucket: TableBucket) -> Result<KvBatchScanner> {
        self.reject_filter("KvBatchScanner")?;
        self.reject_limit("KvBatchScanner")?;
        if !self.table_info.has_primary_key() {
            return Err(Error::UnsupportedOperation {
                message: "KV snapshot scans require a primary-key table".into(),
            });
        }
        if bucket.table_id() != self.table_info.table_id
            || bucket.partition_id().is_some() != self.table_info.is_partitioned()
            || bucket.bucket_id() < 0
            || bucket.bucket_id() >= self.table_info.get_num_buckets()
        {
            return Err(Error::IllegalArgument {
                message: format!("Invalid KV scan bucket: {bucket}"),
            });
        }
        let latest = SchemaInfo::new(
            self.table_info.get_schema().clone(),
            self.table_info.get_schema_id(),
        );
        let getter = Arc::new(ClientSchemaGetter::new(
            self.table_info.table_path.clone(),
            self.conn.get_admin()?,
            latest,
        ));
        Ok(KvBatchScanner::new(
            self.conn.get_connections(),
            self.metadata,
            self.table_info,
            getter,
            self.projected_fields,
            bucket,
        ))
    }

    /// Projects the scan to only include specified columns by their indices.
    ///
    /// # Arguments
    /// * `column_indices` - Zero-based indices of columns to include in the scan
    ///
    /// # Errors
    /// Returns an error if `column_indices` is empty or if any column index is out of range.
    ///
    /// # Example
    /// ```
    /// # use fluss::client::FlussConnection;
    /// # use fluss::config::Config;
    /// # use fluss::error::Result;
    /// # use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    /// # use fluss::row::{DataGetters, InternalRow};
    /// # use std::time::Duration;
    ///
    /// # pub async fn example() -> Result<()> {
    ///     let mut config = Config::default();
    ///     config.bootstrap_servers = "127.0.0.1:9123".to_string();
    ///     let conn = FlussConnection::new(config).await?;
    ///
    ///     let table_descriptor = TableDescriptor::builder()
    ///         .schema(
    ///             Schema::builder()
    ///                 .column("col1", DataTypes::int())
    ///                 .column("col2", DataTypes::string())
    ///                 .column("col3", DataTypes::string())
    ///                 .column("col4", DataTypes::string())
    ///             .build()?,
    ///         ).build()?;
    ///     let table_path = TablePath::new("fluss".to_owned(), "rust_test_long".to_owned());
    ///     let admin = conn.get_admin()?;
    ///     admin.create_table(&table_path, &table_descriptor, true)
    ///         .await?;
    ///     let table_info = admin.get_table_info(&table_path).await?;
    ///     let table = conn.get_table(&table_path).await?;
    ///
    ///     // Project columns by indices
    ///     let scanner = table.new_scan().project(&[0, 2, 3])?.create_log_scanner()?;
    ///     let scan_records = scanner.poll(Duration::from_secs(10)).await?;
    ///     for record in scan_records {
    ///         let row = record.row();
    ///         println!(
    ///             "{{{}, {}, {}}}@{}",
    ///             row.get_int(0)?,
    ///             row.get_string(2)?,
    ///             row.get_string(3)?,
    ///             record.offset()
    ///         );
    ///     }
    ///     # Ok(())
    /// # }
    /// ```
    pub fn project(mut self, column_indices: &[usize]) -> Result<Self> {
        if column_indices.is_empty() {
            return Err(Error::IllegalArgument {
                message: "Column indices cannot be empty".to_string(),
            });
        }
        let field_count = self.table_info.row_type().fields().len();
        for &idx in column_indices {
            if idx >= field_count {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Column index {} out of range (max: {})",
                        idx,
                        field_count - 1
                    ),
                });
            }
        }
        self.projected_fields = Some(column_indices.to_vec());
        Ok(self)
    }

    /// Projects the scan to only include specified columns by their names.
    ///
    /// # Arguments
    /// * `column_names` - Names of columns to include in the scan
    ///
    /// # Errors
    /// Returns an error if `column_names` is empty or if any column name is not found in the table schema.
    ///
    /// # Example
    /// ```
    /// # use fluss::client::FlussConnection;
    /// # use fluss::config::Config;
    /// # use fluss::error::Result;
    /// # use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    /// # use fluss::row::{DataGetters, InternalRow};
    /// # use std::time::Duration;
    ///
    /// # pub async fn example() -> Result<()> {
    ///     let mut config = Config::default();
    ///     config.bootstrap_servers = "127.0.0.1:9123".to_string();
    ///     let conn = FlussConnection::new(config).await?;
    ///
    ///     let table_descriptor = TableDescriptor::builder()
    ///         .schema(
    ///             Schema::builder()
    ///                 .column("col1", DataTypes::int())
    ///                 .column("col2", DataTypes::string())
    ///                 .column("col3", DataTypes::string())
    ///             .build()?,
    ///         ).build()?;
    ///     let table_path = TablePath::new("fluss".to_owned(), "rust_test_long".to_owned());
    ///     let admin = conn.get_admin()?;
    ///     admin.create_table(&table_path, &table_descriptor, true)
    ///         .await?;
    ///     let table = conn.get_table(&table_path).await?;
    ///
    ///     // Project columns by column names
    ///     let scanner = table.new_scan().project_by_name(&["col1", "col3"])?.create_log_scanner()?;
    ///     let scan_records = scanner.poll(Duration::from_secs(10)).await?;
    ///     for record in scan_records {
    ///         let row = record.row();
    ///         println!(
    ///             "{{{}, {}}}@{}",
    ///             row.get_int(0)?,
    ///             row.get_string(1)?,
    ///             record.offset()
    ///         );
    ///     }
    ///     # Ok(())
    /// # }
    /// ```
    pub fn project_by_name(mut self, column_names: &[&str]) -> Result<Self> {
        if column_names.is_empty() {
            return Err(Error::IllegalArgument {
                message: "Column names cannot be empty".to_string(),
            });
        }
        let row_type = self.table_info.row_type();
        let mut indices = Vec::new();

        for name in column_names {
            let idx = row_type
                .fields()
                .iter()
                .position(|f| f.name() == *name)
                .ok_or_else(|| Error::IllegalArgument {
                    message: format!("Column '{name}' not found"),
                })?;
            indices.push(idx);
        }

        self.projected_fields = Some(indices);
        Ok(self)
    }

    /// Creates a record-mode log scanner, polled for individual [`ScanRecord`]s.
    ///
    /// Works on log tables and on primary-key (KV) tables. For a primary-key
    /// table this subscribes to its CDC changelog: each [`ScanRecord`] carries a
    /// [`ChangeType`](crate::record::ChangeType) — `+I` (insert), `-U`
    /// (update-before), `+U` (update-after) or `-D` (delete). A log table yields
    /// `+A` (append-only) for every record. Requires the ARROW log format.
    pub fn create_log_scanner(self) -> Result<LogScanner> {
        self.reject_limit("LogScanner")?;
        validate_scan_support_inner(&self.table_info.table_path, &self.table_info, true)?;
        let admin = self.conn.get_admin()?;
        let inner = LogScannerInner::new(
            &self.table_info,
            self.metadata.clone(),
            self.conn.get_connections(),
            self.conn.config(),
            self.projected_fields,
            self.fixed_schema,
            self.filter,
            admin,
        )?;
        Ok(LogScanner {
            inner: Arc::new(inner),
        })
    }

    /// Creates a batch-mode log scanner that yields Arrow `RecordBatch`es.
    ///
    /// Log tables only. Primary-key tables are rejected because the Arrow batch
    /// path carries no per-record change types; read a primary-key table's
    /// changelog with [`create_log_scanner`](Self::create_log_scanner) instead.
    /// Requires the ARROW log format.
    pub fn create_record_batch_log_scanner(self) -> Result<RecordBatchLogScanner> {
        self.reject_limit("RecordBatchLogScanner")?;
        validate_scan_support(&self.table_info.table_path, &self.table_info)?;
        let admin = self.conn.get_admin()?;
        let inner = LogScannerInner::new(
            &self.table_info,
            self.metadata.clone(),
            self.conn.get_connections(),
            self.conn.config(),
            self.projected_fields,
            self.fixed_schema,
            self.filter,
            admin,
        )?;
        Ok(RecordBatchLogScanner {
            inner: Arc::new(inner),
        })
    }
}

/// Scanner for reading log records one at a time with per-record metadata.
///
/// Use this scanner when you need access to individual record offsets and timestamps.
/// For batch-level access, use [`RecordBatchLogScanner`] instead.
/// Validates that `table_info` can be scanned as a log, rejecting primary-key
/// tables (the default for batch-mode scans).
pub(super) fn validate_scan_support(table_path: &TablePath, table_info: &TableInfo) -> Result<()> {
    validate_scan_support_inner(table_path, table_info, false)
}

pub(super) fn validate_limit_scan_fixed_schema(
    table_info: &TableInfo,
    fixed_schema: bool,
) -> Result<()> {
    if !fixed_schema && !table_info.has_primary_key() {
        return Err(Error::IllegalArgument {
            message: "LimitBatchScanner doesn't support with_fixed_schema(false) for log tables"
                .to_string(),
        });
    }
    Ok(())
}

/// Validates that `table_info` can be scanned as a log. ARROW log format is
/// required; INDEXED is not supported by the client decoder.
///
/// When `allow_primary_key` is set, a primary-key table is accepted and its
/// changelog is read as a CDC stream (each record carries a `ChangeType`). It is
/// cleared for the Arrow batch path, which has no slot for per-record change
/// types, so reading a changelog there would silently drop them.
pub(super) fn validate_scan_support_inner(
    table_path: &TablePath,
    table_info: &TableInfo,
    allow_primary_key: bool,
) -> Result<()> {
    if !allow_primary_key && table_info.schema.primary_key().is_some() {
        return Err(UnsupportedOperation {
            message: format!(
                "Batch-mode log scan does not support primary-key table {table_path}; use create_log_scanner() to read its changelog record by record"
            ),
        });
    }

    let log_format = table_info.table_config.get_log_format()?;
    if LogFormat::ARROW != log_format {
        return Err(UnsupportedOperation {
            message: format!(
                "Log scan is only supported for ARROW log format, but table {table_path} uses {log_format} format"
            ),
        });
    }

    Ok(())
}
