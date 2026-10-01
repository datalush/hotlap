// SPDX-License-Identifier: Apache-2.0
//! A read-only snapshot of Fluss database/table names for DataFusion SQL.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::TableType;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;

use crate::FlussLogTable;

/// Read-only catalog. Reload it to discover databases/tables created later.
pub struct FlussCatalog {
    schemas: HashMap<String, Arc<FlussSchema>>,
}

impl FlussCatalog {
    /// List names once, without reading rows or treating a KV table as a log.
    pub async fn load(connection: Arc<FlussConnection>, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(DataFusionError::Plan(
                "Fluss scan timeout must be positive".into(),
            ));
        }
        let admin = connection.get_admin().map_err(fluss_error)?;
        let databases = tokio::time::timeout(timeout, admin.list_databases())
            .await
            .map_err(|_| discovery_timeout())?
            .map_err(fluss_error)?;
        let mut schemas = HashMap::new();
        for database in databases {
            let tables = tokio::time::timeout(timeout, admin.list_tables(&database))
                .await
                .map_err(|_| discovery_timeout())?
                .map_err(fluss_error)?;
            schemas.insert(
                database.clone(),
                Arc::new(FlussSchema {
                    connection: Arc::clone(&connection),
                    database,
                    tables: tables.into_iter().collect(),
                    timeout,
                }),
            );
        }
        Ok(Self { schemas })
    }
}

impl fmt::Debug for FlussCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussCatalog")
            .field("databases", &self.schemas.keys())
            .finish()
    }
}

impl CatalogProvider for FlussCatalog {
    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.schemas.keys().cloned().collect();
        names.sort();
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|schema| Arc::clone(schema) as Arc<dyn SchemaProvider>)
    }
}

struct FlussSchema {
    connection: Arc<FlussConnection>,
    database: String,
    tables: HashSet<String>,
    timeout: Duration,
}

impl fmt::Debug for FlussSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussSchema")
            .field("database", &self.database)
            .field("tables", &self.tables)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for FlussSchema {
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.iter().cloned().collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if !self.tables.contains(name) {
            return Ok(None);
        }
        let table = FlussLogTable::open(
            Arc::clone(&self.connection),
            TablePath::new(&self.database, name),
            self.timeout,
        )
        .await?;
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains(name)
    }

    async fn table_type(&self, name: &str) -> Result<Option<TableType>> {
        Ok(self.tables.contains(name).then_some(TableType::Base))
    }
}

fn fluss_error(error: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn discovery_timeout() -> DataFusionError {
    DataFusionError::Execution("Fluss catalog discovery timed out".into())
}
