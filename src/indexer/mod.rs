pub mod backfill;
pub mod batch;
pub mod realtime;

use std::sync::Arc;

use sqlx::PgPool;

use crate::config::Config;
use crate::idl::Idl;
use crate::rpc::RpcClient;

#[derive(Clone)]
pub struct IndexerContext {
    pub config: Arc<Config>,
    pub idl: Arc<Idl>,
    pub pool: PgPool,
    pub rpc: RpcClient,
}

impl IndexerContext {
    pub fn new(config: Config, idl: Idl, pool: PgPool) -> Self {
        let rpc = RpcClient::new(
            &config.helius_rpc_url,
            config.rpc_max_retries,
            config.rpc_initial_backoff_ms,
            config.rpc_max_backoff_ms,
        );
        Self {
            config: Arc::new(config),
            idl: Arc::new(idl),
            pool,
            rpc,
        }
    }
}
