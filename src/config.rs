use solana_sdk::pubkey::Pubkey;
use std::env;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum IndexMode {
    Batch,
    Realtime,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub helius_rpc_url: String,
    pub helius_ws_url: String,
    pub postgres_url: String,
    pub program_id: Pubkey,
    pub program_id_str: String,
    pub idl_path: Option<String>,
    pub start_slot: Option<u64>,
    pub end_slot: Option<u64>,
    pub batch_size: usize,
    pub api_port: u16,
    pub mode: IndexMode,
    pub rpc_max_retries: u32,
    pub rpc_initial_backoff_ms: u64,
    pub rpc_max_backoff_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        dotenvy::dotenv().ok();

        let helius_rpc_url = require_env("HELIUS_RPC_URL")?;
        if !helius_rpc_url.starts_with("https://") {
            return Err(ConfigError::InvalidValue("HELIUS_RPC_URL must start with https://"));
        }

        let helius_ws_url = optional_env("HELIUS_WS_URL")
            .unwrap_or_else(|| helius_rpc_url.replacen("https://", "wss://", 1));

        let postgres_url = require_env("POSTGRES_URL")?;

        let program_id_str = require_env("PROGRAM_ID")?;
        let program_id = Pubkey::from_str(&program_id_str)
            .map_err(|_| ConfigError::InvalidValue("PROGRAM_ID is not a valid base58 pubkey"))?;

        let idl_path = optional_env("IDL_PATH");
        let start_slot = optional_env("START_SLOT").and_then(|s| s.parse::<u64>().ok());
        let end_slot = optional_env("END_SLOT").and_then(|s| s.parse::<u64>().ok());

        let batch_size = optional_env("BATCH_SIZE")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(100);

        let api_port = optional_env("API_PORT")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(3000);

        let mode = match optional_env("INDEX_MODE").as_deref() {
            Some("batch") => IndexMode::Batch,
            _ => IndexMode::Realtime,
        };

        let rpc_max_retries = optional_env("RPC_MAX_RETRIES")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(8);

        let rpc_initial_backoff_ms = optional_env("RPC_INITIAL_BACKOFF_MS")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(250);

        let rpc_max_backoff_ms = optional_env("RPC_MAX_BACKOFF_MS")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30_000);

        Ok(Config {
            helius_rpc_url,
            helius_ws_url,
            postgres_url,
            program_id,
            program_id_str,
            idl_path,
            start_slot,
            end_slot,
            batch_size,
            api_port,
            mode,
            rpc_max_retries,
            rpc_initial_backoff_ms,
            rpc_max_backoff_ms,
        })
    }
}

fn require_env(key: &'static str) -> Result<String, ConfigError> {
    match env::var(key) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(ConfigError::Missing(key)),
    }
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|s| !s.is_empty())
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    Missing(&'static str),
    #[error("invalid configuration value: {0}")]
    InvalidValue(&'static str),
}
