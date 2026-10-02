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

#[test]
fn limit_batch_scanner_rejects_dynamic_schema_for_log_table() {
    let table_info = build_table_info(TablePath::new("db".to_string(), "tbl".to_string()), 1, 1);

    let error = validate_limit_scan_fixed_schema(&table_info, false)
        .expect_err("dynamic schema must be rejected for a log limit scan");

    assert!(matches!(
        error,
        Error::IllegalArgument { message }
            if message.contains("with_fixed_schema(false)")
    ));
}

#[tokio::test]
async fn unresolvable_filter_column_is_rejected() {
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = build_table_info(table_path.clone(), 1, 1);
    let cluster = build_cluster_arc(&table_path, 1, 1);
    let metadata = Arc::new(Metadata::new_for_test(cluster));
    let result = filtering_fetcher(
        &table_info,
        &metadata,
        Arc::new(LogScannerStatus::new()),
        Some(crate::predicate::col("nope").gt(5i32)),
    );
    assert!(matches!(result.err(), Some(Error::IllegalArgument { .. })));
}

#[test]
fn test_validate_scan_support() {
    // Record mode (allow_primary_key = true): a primary-key table's changelog
    // is scannable on ARROW or the default format.
    let (table_info, table_path) = create_test_table_info(true, Some("ARROW"));
    assert!(validate_scan_support_inner(&table_path, &table_info, true).is_ok());
    let (table_info, table_path) = create_test_table_info(true, None);
    assert!(validate_scan_support_inner(&table_path, &table_info, true).is_ok());

    // Batch mode (allow_primary_key = false): a primary-key table is rejected.
    let (table_info, table_path) = create_test_table_info(true, Some("ARROW"));
    let err = validate_scan_support(&table_path, &table_info).unwrap_err();
    assert!(matches!(err, UnsupportedOperation { .. }));
    assert!(err.to_string().contains("primary-key table"));

    // INDEXED is unsupported regardless of table type or mode.
    for allow_primary_key in [false, true] {
        let (table_info, table_path) = create_test_table_info(false, Some("INDEXED"));
        let err =
            validate_scan_support_inner(&table_path, &table_info, allow_primary_key).unwrap_err();
        assert!(matches!(err, UnsupportedOperation { .. }));
        assert!(err.to_string().contains("ARROW log format"));
    }

    // Log tables scan on ARROW or the default format.
    let (table_info, table_path) = create_test_table_info(false, None);
    assert!(validate_scan_support(&table_path, &table_info).is_ok());
    let (table_info, table_path) = create_test_table_info(false, Some("ARROW"));
    assert!(validate_scan_support(&table_path, &table_info).is_ok());
}
