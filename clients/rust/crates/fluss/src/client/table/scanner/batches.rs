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

//! Consume completed fetches as Arrow record batches.

use super::{CompletedFetch, FetchResult, LogFetcher, Result, ScanBatch, warn};

impl LogFetcher {
    /// Collect completed fetches as ScanBatches (with bucket and offset metadata)
    pub(super) async fn collect_batches_limited(
        &self,
        max_batches: usize,
    ) -> Result<Vec<ScanBatch>> {
        // Limit memory usage with both batch count and byte size constraints.
        // Max 100 batches per poll, but also check total bytes (soft cap ~64MB).
        const MAX_BYTES: usize = 64 * 1024 * 1024; // 64MB soft cap
        let mut result: Vec<ScanBatch> = Vec::new();
        let mut batches_remaining = max_batches;
        let mut bytes_consumed: usize = 0;

        {
            while batches_remaining > 0 && bytes_consumed < MAX_BYTES {
                let next_in_line = self.log_fetch_buffer.next_in_line_fetch();

                match next_in_line {
                    Some(mut next_fetch) if !next_fetch.is_consumed() => {
                        let raw_bytes = next_fetch.size_in_bytes();
                        let fetch_result =
                            self.fetch_batches_from_fetch(&mut next_fetch, batches_remaining)?;
                        match fetch_result {
                            FetchResult::Data(scan_batches) => {
                                let batch_count = scan_batches.len();

                                // Bound this poll by both raw fetch size and decoded
                                // Arrow size. The last fetch may exceed the soft cap.
                                let batch_bytes: usize = scan_batches
                                    .iter()
                                    .map(|sb| sb.batch().get_array_memory_size())
                                    .sum();
                                bytes_consumed =
                                    bytes_consumed.saturating_add(raw_bytes.max(batch_bytes));

                                if !scan_batches.is_empty() {
                                    result.extend(scan_batches);
                                    batches_remaining =
                                        batches_remaining.saturating_sub(batch_count);
                                }
                            }
                            FetchResult::SchemaRequired(schema_id) => {
                                // Preserve the current file-backed batch across await/cancel.
                                self.log_fetch_buffer
                                    .set_next_in_line_fetch(Some(next_fetch));

                                // Return already decoded batches before doing another async RPC,
                                // keeping cancellation from discarding user-visible progress.
                                if !result.is_empty() {
                                    return Ok(result);
                                }

                                self.resolver.fetch_and_register(schema_id).await?;
                                continue;
                            }
                        }

                        if !next_fetch.is_consumed() {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(next_fetch));
                        }
                    }
                    _ => {
                        if let Some(completed_fetch) = self.log_fetch_buffer.poll() {
                            if !completed_fetch.is_initialized() {
                                match self.initialize_fetch(completed_fetch) {
                                    Ok(initialized) => {
                                        self.log_fetch_buffer.set_next_in_line_fetch(initialized);
                                        continue;
                                    }
                                    Err(e) => return Err(e),
                                }
                            } else {
                                self.log_fetch_buffer
                                    .set_next_in_line_fetch(Some(completed_fetch));
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    fn fetch_batches_from_fetch(
        &self,
        next_in_line_fetch: &mut Box<dyn CompletedFetch>,
        max_batches: usize,
    ) -> Result<FetchResult<Vec<ScanBatch>>> {
        let table_bucket = next_in_line_fetch.table_bucket().clone();
        let current_offset = self.log_scanner_status.get_bucket_offset(&table_bucket);

        if current_offset.is_none() {
            warn!(
                "Ignoring fetched batches for {table_bucket:?} since the bucket has been unsubscribed"
            );
            next_in_line_fetch.drain();
            return Ok(FetchResult::Data(Vec::new()));
        }

        let current_offset = current_offset.unwrap();
        let fetch_offset = next_in_line_fetch.next_fetch_offset();

        if fetch_offset == current_offset {
            match next_in_line_fetch.fetch_batches(max_batches)? {
                FetchResult::Data(batches_with_offsets) => {
                    let next_fetch_offset = next_in_line_fetch.next_fetch_offset();

                    if next_fetch_offset > current_offset {
                        self.log_scanner_status
                            .update_offset(&table_bucket, next_fetch_offset);
                    }

                    // Convert to ScanBatch with bucket info
                    Ok(FetchResult::Data(
                        batches_with_offsets
                            .into_iter()
                            .map(|(batch, base_offset)| {
                                ScanBatch::new(table_bucket.clone(), batch, base_offset)
                            })
                            .collect(),
                    ))
                }
                FetchResult::SchemaRequired(schema_id) => {
                    Ok(FetchResult::SchemaRequired(schema_id))
                }
            }
        } else {
            warn!(
                "Ignoring fetched batches for {table_bucket:?} at offset {fetch_offset} since the current offset is {current_offset}"
            );
            next_in_line_fetch.drain();
            Ok(FetchResult::Data(Vec::new()))
        }
    }
}
