use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;
use tracing::{info, warn, error};
use snowflake_connector_rs::{
    SnowflakeClient, SnowflakeClientConfig, SnowflakeAuthMethod,
    SnowflakeRow, SnowflakeSession,
};

#[derive(Debug, Error)]
pub enum SnowflakeMapperError {
    #[error("Failed to connect to Snowflake: {0}")]
    ConnectionError(String),
    
    #[error("Failed to execute query: {0}")]
    QueryError(String),
    
    #[error("Failed to read column {column}: {message}")]
    ColumnError {
        column: String,
        message: String,
    },
    
    #[error("Failed to write output: {0}")]
    OutputError(String),
    
    #[error("Missing required environment variable: {0}")]
    MissingEnvVar(String),
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "snowflake-mapper",
    about = "A tool to fetch and map Snowflake database schemas",
    version,
    author
)]
pub struct Args {
    /// Specific databases to process (comma-separated). If not provided, all accessible databases will be processed
    #[arg(short, long, value_delimiter = ',')]
    pub databases: Option<Vec<String>>,

    /// Output directory for the JSON files
    #[arg(short, long, default_value = "output")]
    pub output_dir: PathBuf,

    /// Number of retries for failed operations
    #[arg(short, long, default_value = "3")]
    pub retries: u32,

    /// Delay in seconds between retries
    #[arg(long, default_value = "5")]
    pub retry_delay: u64,

    /// Skip tables that fail to process
    #[arg(long)]
    pub skip_failed_tables: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub created_on: String,
    pub owner: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WarehouseInfo {
    pub name: String,
    pub size: String,
    pub state: String,
    pub type_: String,
}

#[async_trait]
pub trait SnowflakeOperations {
    async fn connect(&mut self) -> Result<()>;
    async fn set_warehouse(&mut self, warehouse: &str) -> Result<()>;
    async fn set_role(&mut self, role: &str) -> Result<()>;
    async fn get_all_databases(&mut self) -> Result<Vec<DatabaseInfo>>;
    async fn get_tables_for_database(&mut self, database: &str) -> Result<Vec<TableInfo>>;
    async fn list_warehouses(&mut self) -> Result<Vec<WarehouseInfo>>;
}

pub struct SnowflakeMapper {
    pub config: SnowflakeConfig,
    pub client: Option<SnowflakeClient>,
    pub session: Option<SnowflakeSession>,
    pub args: Args,
}

pub struct SnowflakeConfig {
    pub account: String,
    pub username: String,
    pub password: String,
    pub warehouse: String,
    pub database: Option<String>,
    pub role: Option<String>,
}

impl SnowflakeMapper {
    pub fn new(config: SnowflakeConfig, args: Args) -> Self {
        Self {
            config,
            client: None,
            session: None,
            args,
        }
    }

    #[allow(dead_code)]
    async fn with_retry<F, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Result<T> + Send + Sync,
    {
        let mut last_error = None;
        for attempt in 0..=self.args.retries {
            if attempt > 0 {
                warn!("Retry attempt {} of {}", attempt, self.args.retries);
                tokio::time::sleep(std::time::Duration::from_secs(self.args.retry_delay)).await;
            }

            match operation() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    error!("Operation failed: {}", last_error.as_ref().unwrap());
                }
            }
        }

        Err(last_error.unwrap())
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.client.is_none() {
            let client = SnowflakeClient::new(
                &self.config.username,
                SnowflakeAuthMethod::Password(self.config.password.clone()),
                SnowflakeClientConfig {
                    account: self.config.account.clone(),
                    role: self.config.role.clone(),
                    warehouse: Some(self.config.warehouse.clone()),
                    database: self.config.database.clone(),
                    schema: None,
                    timeout: Some(std::time::Duration::from_secs(30)),
                },
            ).context("Failed to create Snowflake client")?;

            let session = client.create_session()
                .await
                .context("Failed to create Snowflake session")?;

            self.client = Some(client);
            self.session = Some(session);
        }
        Ok(())
    }

    fn get_session(&self) -> Result<&SnowflakeSession> {
        self.session.as_ref().context("Not connected to Snowflake")
    }

    fn get_value_from_row(row: &SnowflakeRow, column: &str) -> Result<String> {
        match row.get::<Option<String>>(column) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Ok(String::new()),
            Err(e) => Err(SnowflakeMapperError::ColumnError {
                column: column.to_string(),
                message: e.to_string(),
            }.into())
        }
    }

    fn get_i32_from_row(row: &SnowflakeRow, column: &str) -> Result<Option<i32>> {
        match row.get::<Option<String>>(column) {
            Ok(Some(value)) if !value.is_empty() => {
                value.parse()
                    .map(Some)
                    .map_err(|e| SnowflakeMapperError::ColumnError {
                        column: column.to_string(),
                        message: format!("Failed to parse as i32: {}", e),
                    }.into())
            },
            Ok(_) => Ok(None),
            Err(e) => Err(SnowflakeMapperError::ColumnError {
                column: column.to_string(),
                message: e.to_string(),
            }.into())
        }
    }
}

#[async_trait]
impl SnowflakeOperations for SnowflakeMapper {
    async fn connect(&mut self) -> Result<()> {
        self.ensure_connected().await?;
        
        // Store values before using them to avoid borrowing issues
        let warehouse = self.config.warehouse.clone();
        let role = self.config.role.clone();
        
        // Set warehouse immediately after connection
        self.set_warehouse(&warehouse).await?;
        if let Some(role) = role {
            self.set_role(&role).await?;
        }
        Ok(())
    }

    async fn set_warehouse(&mut self, warehouse: &str) -> Result<()> {
        info!("Setting warehouse to: {}", warehouse);
        
        // Try to get list of warehouses first
        info!("Listing available warehouses...");
        let warehouses = self.list_warehouses().await?;
        let warehouse_names: Vec<String> = warehouses.iter().map(|w| w.name.clone()).collect();
        info!("Available warehouses: {:?}", warehouse_names);
        
        // Use specified warehouse or fall back to COMPUTE_WH
        let target_warehouse = if warehouse_names.iter().any(|w| w.eq_ignore_ascii_case(warehouse)) {
            warehouse.to_string()
        } else {
            warn!("Warehouse '{}' not found, falling back to COMPUTE_WH", warehouse);
            "COMPUTE_WH".to_string()
        };

        let query = format!("USE WAREHOUSE \"{}\"", target_warehouse);
        info!("Executing query: {}", query);
        self.get_session()?
            .query(query.as_str())
            .await
            .map_err(|e| SnowflakeMapperError::QueryError(format!("Failed to set warehouse: {}", e)))?;
        info!("Successfully set warehouse to: {}", target_warehouse);
        Ok(())
    }

    async fn set_role(&mut self, role: &str) -> Result<()> {
        info!("Setting role to: {}", role);
        let query = format!("USE ROLE \"{}\"", role);
        info!("Executing query: {}", query);
        self.get_session()?
            .query(query.as_str())
            .await
            .map_err(|e| SnowflakeMapperError::QueryError(format!("Failed to set role: {}", e)))?;
        info!("Successfully set role to: {}", role);
        Ok(())
    }

    async fn get_all_databases(&mut self) -> Result<Vec<DatabaseInfo>> {
        self.ensure_connected().await?;
        let rows = self.get_session()?
            .query("SHOW DATABASES")
            .await
            .map_err(|e| SnowflakeMapperError::QueryError(format!("Failed to list databases: {}", e)))?;
        
        let mut databases = Vec::new();
        for row in rows {
            databases.push(DatabaseInfo {
                name: Self::get_value_from_row(&row, "name")?,
                created_on: Self::get_value_from_row(&row, "created_on")?,
                owner: Self::get_value_from_row(&row, "owner")?,
            });
        }
        Ok(databases)
    }

    async fn get_tables_for_database(&mut self, database: &str) -> Result<Vec<TableInfo>> {
        self.ensure_connected().await?;
        let query = format!(
            "SELECT table_schema, table_name, column_name, data_type, 
             is_nullable, character_maximum_length, numeric_precision, numeric_scale
             FROM {}.information_schema.columns
             ORDER BY table_schema, table_name, ordinal_position",
            database
        );

        let rows = self.get_session()?
            .query(query.as_str())
            .await
            .map_err(|e| SnowflakeMapperError::QueryError(format!("Failed to get tables for database {}: {}", database, e)))?;

        let mut tables: Vec<TableInfo> = Vec::new();
        let mut current_table: Option<TableInfo> = None;

        for row in rows {
            let schema_name = Self::get_value_from_row(&row, "table_schema")?;
            let table_name = Self::get_value_from_row(&row, "table_name")?;

            if current_table.as_ref().map_or(true, |t| {
                t.schema_name != schema_name || t.table_name != table_name
            }) {
                if let Some(table) = current_table.take() {
                    tables.push(table);
                }
                current_table = Some(TableInfo {
                    database_name: database.to_string(),
                    schema_name,
                    table_name,
                    columns: Vec::new(),
                });
            }

            if let Some(table) = current_table.as_mut() {
                table.columns.push(ColumnInfo {
                    name: Self::get_value_from_row(&row, "column_name")?,
                    data_type: Self::get_value_from_row(&row, "data_type")?,
                    is_nullable: Self::get_value_from_row(&row, "is_nullable")?.eq_ignore_ascii_case("YES"),
                    character_maximum_length: Self::get_i32_from_row(&row, "character_maximum_length")?,
                    numeric_precision: Self::get_i32_from_row(&row, "numeric_precision")?,
                    numeric_scale: Self::get_i32_from_row(&row, "numeric_scale")?,
                });
            }
        }

        if let Some(table) = current_table.take() {
            tables.push(table);
        }

        Ok(tables)
    }

    async fn list_warehouses(&mut self) -> Result<Vec<WarehouseInfo>> {
        info!("Listing warehouses...");
        let rows = self.get_session()?
            .query("SHOW WAREHOUSES")
            .await
            .map_err(|e| SnowflakeMapperError::QueryError(format!("Failed to list warehouses: {}", e)))?;

        let mut warehouses = Vec::new();
        for row in rows {
            warehouses.push(WarehouseInfo {
                name: Self::get_value_from_row(&row, "name")?,
                size: Self::get_value_from_row(&row, "size")?,
                state: Self::get_value_from_row(&row, "state")?,
                type_: Self::get_value_from_row(&row, "type")?,
            });
        }
        info!("Found {} warehouses", warehouses.len());
        Ok(warehouses)
    }
}

async fn write_formatted_output(path: PathBuf, data: &impl Serialize) -> Result<()> {
    let json = serde_json::to_string_pretty(data)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, json)
        .map_err(|e| SnowflakeMapperError::OutputError(format!("Failed to write to {}: {}", path.display(), e)))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables
    dotenv::dotenv().ok();

    // Create Snowflake configuration from environment variables
    let config = SnowflakeConfig {
        account: std::env::var("SNOWFLAKE_ACCOUNT")
            .map_err(|_| SnowflakeMapperError::MissingEnvVar("SNOWFLAKE_ACCOUNT".to_string()))?,
        username: std::env::var("SNOWFLAKE_USERNAME")
            .map_err(|_| SnowflakeMapperError::MissingEnvVar("SNOWFLAKE_USERNAME".to_string()))?,
        password: std::env::var("SNOWFLAKE_PASSWORD")
            .map_err(|_| SnowflakeMapperError::MissingEnvVar("SNOWFLAKE_PASSWORD".to_string()))?,
        warehouse: std::env::var("SNOWFLAKE_WAREHOUSE")
            .map_err(|_| SnowflakeMapperError::MissingEnvVar("SNOWFLAKE_WAREHOUSE".to_string()))?,
        database: std::env::var("SNOWFLAKE_DATABASE").ok(),
        role: Some(std::env::var("SNOWFLAKE_ROLE").unwrap_or_else(|_| "SALES".to_string())),
    };

    let mut client = SnowflakeMapper::new(config, args.clone());
    client.connect().await?;

    // Get databases to process
    let databases = match &args.databases {
        Some(dbs) => dbs.iter().map(|name| DatabaseInfo {
            name: name.clone(),
            created_on: String::new(),
            owner: String::new(),
        }).collect(),
        None => client.get_all_databases().await?,
    };

    // Create progress bar
    let progress = ProgressBar::new(databases.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    // Process each database
    for db in databases {
        progress.set_message(format!("Processing database: {}", db.name));
        
        match client.get_tables_for_database(&db.name).await {
            Ok(tables) => {
                let output_path = args.output_dir
                    .join(&db.name)
                    .with_extension("json");
                
                if let Err(e) = write_formatted_output(output_path.clone(), &tables).await {
                    error!("Failed to write output for database {}: {}", db.name, e);
                    if !args.skip_failed_tables {
                        return Err(e);
                    }
                }
                info!("Processed database: {}", db.name);
            }
            Err(e) => {
                error!("Failed to process database {}: {}", db.name, e);
                if !args.skip_failed_tables {
                    return Err(e);
                }
            }
        }
        
        progress.inc(1);
    }

    progress.finish_with_message("Done!");
    Ok(())
}
