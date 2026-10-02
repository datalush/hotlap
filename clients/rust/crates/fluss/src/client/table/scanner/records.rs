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

//! Consume completed fetches as offset-bearing log records.

use super::*;

impl LogFetcher {
    /// Collect completed fetches from buffer
    /// Reference: LogFetchCollector.collectFetch in Java
    pub(super) async fn collect_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let mut result: HashMap<TableBucket, Vec<ScanRecord>> = HashMap::new();
        let mut records_remaining = self.max_poll_records;

        let collect_result: Result<()> = {
            while records_remaining > 0 {
                // Get the next in line fetch, or get a new one from buffer
                let next_in_line = self.log_fetch_buffer.next_in_line_fetch();

                if next_in_line.is_none() || next_in_line.as_ref().unwrap().is_consumed() {
                    // Get a new fetch from buffer
                    if let Some(completed_fetch) = self.log_fetch_buffer.poll() {
                        // Initialize the fetch if not already initialized
                        if !completed_fetch.is_initialized() {
                            let size_in_bytes = completed_fetch.size_in_bytes();
                            match self.initialize_fetch(completed_fetch) {
                                Ok(initialized) => {
                                    self.log_fetch_buffer.set_next_in_line_fetch(initialized);
                                    continue;
                                }
                                Err(e) => {
                                    // Remove a completedFetch upon a parse with exception if
                                    // (1) it contains no records, and
                                    // (2) there are no fetched records with actual content preceding this
                                    // exception.
                                    if result.is_empty() && size_in_bytes == 0 {
                                        // todo: do we need to consider it like java ?
                                        // self.log_fetch_buffer.poll();
                                    }
                                    return Err(e);
                                }
                            }
                        } else {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(completed_fetch));
                        }
                        // Note: poll() already removed the fetch from buffer, so no need to call poll()
                    } else {
                        // No more fetches available
                        break;
                    }
                } else {
                    // Fetch records from next_in_line
                    if let Some(mut next_fetch) = next_in_line {
                        let fetch_result = match self
                            .fetch_records_from_fetch(&mut next_fetch, records_remaining)
                        {
                            Ok(fetch_result) => fetch_result,
                            Err(e) => {
                                if !next_fetch.is_consumed() {
                                    self.log_fetch_buffer
                                        .set_next_in_line_fetch(Some(next_fetch));
                                }
                                return Err(e);
                            }
                        };

                        match fetch_result {
                            FetchResult::Data(records) => {
                                if !records.is_empty() {
                                    let table_bucket = next_fetch.table_bucket().clone();
                                    // Merge with existing records for this bucket
                                    let existing = result.entry(table_bucket).or_default();
                                    let records_count = records.len();
                                    existing.extend(records);

                                    records_remaining =
                                        records_remaining.saturating_sub(records_count);
                                }
                            }
                            FetchResult::SchemaRequired(schema_id) => {
                                // Put the fetch back before awaiting. If this poll future is
                                // cancelled, the exact raw batch remains available for retry.
                                self.log_fetch_buffer
                                    .set_next_in_line_fetch(Some(next_fetch));

                                // Never await after collecting user-visible records: dropping
                                // the future at that point would otherwise lose the local result.
                                if !result.is_empty() {
                                    return Ok(result);
                                }

                                self.resolver.fetch_and_register(schema_id).await?;
                                continue;
                            }
                        }

                        // If the fetch is not fully consumed, put it back for the next round
                        if !next_fetch.is_consumed() {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(next_fetch));
                        }
                        // If consumed, next_fetch will be dropped here (which is correct)
                    }
                }
            }
            Ok(())
        };

        match collect_result {
            Ok(()) => Ok(result),
            Err(e) => {
                if result.is_empty() {
                    Err(e)
                } else {
                    Ok(result)
                }
            }
        }
    }

    /// Initialize a completed fetch, checking offset match and updating high watermark
    pub(super) fn initialize_fetch(
        &self,
        mut completed_fetch: Box<dyn CompletedFetch>,
    ) -> Result<Option<Box<dyn CompletedFetch>>> {
        if let Some(error) = completed_fetch.take_error() {
            return Err(error);
        }

        let table_bucket = completed_fetch.table_bucket().clone();
        let fetch_offset = completed_fetch.next_fetch_offset();

        if let Some(api_error) = completed_fetch.api_error() {
            let error = FlussError::for_code(api_error.code);
            let error_message = api_error.message.as_str();
            self.log_scanner_status
                .move_bucket_to_end(table_bucket.clone());
            let action = completed_fetch
                .fetch_error_context()
                .map(|context| context.action)
                .unwrap_or(FetchErrorAction::Unexpected);
            match action {
                FetchErrorAction::Ignore => {
                    return Ok(None);
                }
                FetchErrorAction::LogOffsetOutOfRange => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "The fetching offset {fetch_offset} is out of range: {error_message}"
                        ),
                        source: None,
                    });
                }
                FetchErrorAction::Authorization => {
                    return Err(Error::FlussAPIError {
                        api_error: ApiError {
                            code: api_error.code,
                            message: api_error.message.to_string(),
                        },
                    });
                }
                FetchErrorAction::CorruptMessage => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Encountered corrupt message when fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                        ),
                        source: None,
                    });
                }
                FetchErrorAction::Unexpected => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Unexpected error code {error:?} while fetching at offset {fetch_offset} from bucket {table_bucket}: {error_message}"
                        ),
                        source: None,
                    });
                }
            }
        }

        // Check if bucket is still subscribed
        let Some(current_offset) = self.log_scanner_status.get_bucket_offset(&table_bucket) else {
            warn!(
                "Discarding stale fetch response for bucket {table_bucket:?} since the bucket has been unsubscribed"
            );
            return Ok(None);
        };

        // Check if offset matches
        if fetch_offset != current_offset {
            warn!(
                "Discarding stale fetch response for bucket {table_bucket:?} since its offset {fetch_offset} does not match the expected offset {current_offset}"
            );
            return Ok(None);
        }

        // Update high watermark
        let high_watermark = completed_fetch.high_watermark();
        if high_watermark >= 0 {
            self.log_scanner_status
                .update_high_watermark(&table_bucket, high_watermark);
        }

        completed_fetch.set_initialized();
        Ok(Some(completed_fetch))
    }

    /// Fetch records from a completed fetch, checking offset match
    pub(super) fn fetch_records_from_fetch(
        &self,
        next_in_line_fetch: &mut Box<dyn CompletedFetch>,
        max_records: usize,
    ) -> Result<FetchResult<Vec<ScanRecord>>> {
        let table_bucket = next_in_line_fetch.table_bucket().clone();
        let current_offset = self.log_scanner_status.get_bucket_offset(&table_bucket);

        if current_offset.is_none() {
            warn!(
                "Ignoring fetched records for {table_bucket:?} since the bucket has been unsubscribed"
            );
            next_in_line_fetch.drain();
            return Ok(FetchResult::Data(Vec::new()));
        }

        let current_offset = current_offset.unwrap();
        let fetch_offset = next_in_line_fetch.next_fetch_offset();

        // Check if this fetch is next in line
        if fetch_offset == current_offset {
            let fetch_result = next_in_line_fetch.fetch_records(max_records)?;
            if matches!(fetch_result, FetchResult::Data(_)) {
                let next_fetch_offset = next_in_line_fetch.next_fetch_offset();

                if next_fetch_offset > current_offset {
                    self.log_scanner_status
                        .update_offset(&table_bucket, next_fetch_offset);
                }

                if next_in_line_fetch.is_consumed() && next_in_line_fetch.records_read() > 0 {
                    self.log_scanner_status
                        .move_bucket_to_end(table_bucket.clone());
                }
            }

            Ok(fetch_result)
        } else {
            // These records aren't next in line, ignore them
            warn!(
                "Ignoring fetched records for {table_bucket:?} at offset {fetch_offset} since the current offset is {current_offset}"
            );
            next_in_line_fetch.drain();
            Ok(FetchResult::Data(Vec::new()))
        }
    }
}
