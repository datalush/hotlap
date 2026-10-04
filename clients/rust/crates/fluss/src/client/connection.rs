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

use crate::client::WriterClient;
use crate::client::admin::FlussAdmin;
use crate::client::lookup::LookupClient;
use crate::client::metadata::Metadata;
use crate::client::table::FlussTable;
use crate::config::Config;
use crate::error::{Error, FlussError, Result};
use crate::metadata::TablePath;

#[cfg(feature = "integration_tests")]
use crate::metadata::PhysicalTablePath;
use crate::rpc::RpcClient;
use crate::rpc::transport::TlsConfig;
use parking_lot::RwLock;
#[cfg(feature = "integration_tests")]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct FlussConnection {
    metadata: Arc<Metadata>,
    network_connects: Arc<RpcClient>,
    args: Config,
    writer_client: RwLock<Option<Arc<WriterClient>>>,
    admin_client: RwLock<Option<Arc<FlussAdmin>>>,
    lookup_client: RwLock<Option<Arc<LookupClient>>>,
}

impl FlussConnection {
    pub async fn new(arg: Config) -> Result<Self> {
        arg.validate_security()
            .map_err(|msg| Error::IllegalArgument { message: msg })?;
        arg.validate_scanner()
            .map_err(|msg| Error::IllegalArgument { message: msg })?;
        arg.validate_writer()
            .map_err(|msg| Error::IllegalArgument { message: msg })?;

        let timeout = Duration::from_millis(arg.connect_timeout_ms);
        // connect_timeout_ms: no lower-bound validation to match Java behavior.
        // Java allows 0 — tracked in https://github.com/apache/fluss/issues/3068
        let mut client = RpcClient::new().with_timeout(timeout);
        if arg.security_ssl_enabled {
            let tls = TlsConfig::new(
                arg.security_ssl_ca_file.as_deref(),
                arg.security_ssl_handshake_timeout_ms,
            )
            .map_err(|message| Error::IllegalArgument { message })?;
            client = client.with_tls(tls);
        }
        if arg.is_sasl_enabled() {
            client = client.with_sasl(
                arg.security_sasl_username.clone(),
                arg.security_sasl_password.clone(),
            );
        }
        let connections = Arc::new(client);
        let metadata = Metadata::new(arg.bootstrap_servers.as_str(), connections.clone()).await?;

        Ok(FlussConnection {
            metadata: Arc::new(metadata),
            network_connects: connections.clone(),
            args: arg.clone(),
            writer_client: Default::default(),
            admin_client: RwLock::new(None),
            lookup_client: Default::default(),
        })
    }

    /// Gracefully shut down the connection, draining any pending write batches.
    ///
    /// If a writer client has been created, this method will signal it to drain
    /// its buffers and wait for the background sender task to complete, bounded
    /// by the provided timeout.
    pub async fn close(&self, timeout: Duration) -> Result<()> {
        let writer_client = self.writer_client.write().take();
        if let Some(client) = writer_client {
            client.close(timeout).await?;
        }
        Ok(())
    }

    /// Abort this connection's writer without waiting for a graceful drain.
    /// Reads are unaffected. In-flight server writes cannot be rolled back.
    pub fn abort_writes(&self) {
        let writer = self.writer_client.write().take();
        if let Some(writer) = writer {
            writer.abort();
        }
    }

    pub fn get_metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }

    pub fn get_connections(&self) -> Arc<RpcClient> {
        self.network_connects.clone()
    }

    pub fn config(&self) -> &Config {
        &self.args
    }

    pub fn get_admin(&self) -> Result<Arc<FlussAdmin>> {
        // 1. Fast path: return cached instance if already initialized.
        if let Some(admin) = self.admin_client.read().as_ref() {
            return Ok(admin.clone());
        }

        // 2. Slow path: acquire write lock.
        let mut admin_guard = self.admin_client.write();

        // 3. Double-check: another thread may have initialized while we waited.
        if let Some(admin) = admin_guard.as_ref() {
            return Ok(admin.clone());
        }

        // 4. Initialize and cache.
        let admin = Arc::new(FlussAdmin::new(
            self.network_connects.clone(),
            self.metadata.clone(),
        ));
        *admin_guard = Some(admin.clone());
        Ok(admin)
    }

    pub fn get_or_create_writer_client(&self) -> Result<Arc<WriterClient>> {
        // 1. Fast path: Attempt to acquire a read lock to check if the client already exists.
        if let Some(client) = self.writer_client.read().as_ref() {
            return Ok(client.clone());
        }

        // 2. Slow path: Acquire the write lock.
        let mut writer_guard = self.writer_client.write();

        // 3. Double-check: Another thread might have initialized the client
        // while this thread was waiting for the write lock.
        if let Some(client) = writer_guard.as_ref() {
            return Ok(client.clone());
        }

        // 4. Initialize the client since we are certain it doesn't exist yet.
        let new_client = Arc::new(WriterClient::new(self.args.clone(), self.metadata.clone())?);

        // 5. Store and return the newly created client.
        *writer_guard = Some(new_client.clone());
        Ok(new_client)
    }

    #[cfg(feature = "integration_tests")]
    pub fn estimated_batch_size_for_table(
        &self,
        table_path: &Arc<PhysicalTablePath>,
    ) -> Option<usize> {
        self.writer_client
            .read()
            .as_ref()
            .and_then(|c| c.estimated_batch_size_for_table(table_path))
    }

    /// Fetch the server-issued token so isolated tests can check its effective
    /// S3 permissions. Never enabled by the production connector build.
    #[cfg(feature = "integration_tests")]
    pub async fn remote_storage_credentials_for_test(&self) -> Result<HashMap<String, String>> {
        Ok(self.remote_storage_token_for_test().await?.0)
    }

    /// Fetch the actual expiration alongside a server-issued token for an
    /// isolated test that waits for its real STS session to expire.
    #[cfg(feature = "integration_tests")]
    pub async fn remote_storage_token_for_test(
        &self,
    ) -> Result<(HashMap<String, String>, Option<i64>)> {
        crate::client::credentials::SecurityTokenManager::fetch_token(
            &self.network_connects,
            &self.metadata,
        )
        .await
    }

    /// Gets or creates a lookup client for batched lookup operations.
    pub fn get_or_create_lookup_client(&self) -> Result<Arc<LookupClient>> {
        // 1. Fast path: Attempt to acquire a read lock to check if the client already exists.
        if let Some(client) = self.lookup_client.read().as_ref() {
            return Ok(client.clone());
        }

        // 2. Slow path: Acquire the write lock.
        let mut lookup_guard = self.lookup_client.write();

        // 3. Double-check: Another thread might have initialized the client
        // while this thread was waiting for the write lock.
        if let Some(client) = lookup_guard.as_ref() {
            return Ok(client.clone());
        }

        // 4. Initialize the client since we are certain it doesn't exist yet.
        let new_client = Arc::new(LookupClient::new(&self.args, self.metadata.clone()));

        // 5. Store and return the newly created client.
        *lookup_guard = Some(new_client.clone());
        Ok(new_client)
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>> {
        self.metadata.update_table_metadata(table_path).await?;
        let table_info = self
            .metadata
            .get_cluster()
            .get_table(table_path)
            .map_err(|e| {
                if e.api_error() == Some(FlussError::InvalidTableException) {
                    Error::table_not_exist(format!("Table not found: {table_path}"))
                } else {
                    e
                }
            })?
            .clone();
        Ok(FlussTable::new(self, self.metadata.clone(), table_info))
    }
}
