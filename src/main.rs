mod api;
mod config;
mod db;
mod decoder;
mod idl;
mod indexer;
mod logging;
mod rpc;

use std::sync::Arc;

use anyhow::Result;
use tokio::signal;

use crate::api::AppState;
use crate::config::IndexMode;

#[tokio::main]
async fn main() -> Result<()> {
    logging::init();

    let config = config::Config::from_env().map_err(|e| {
        tracing::error!(error = %e, "failed to load config");
        e
    })?;

    tracing::info!(
        program = %config.program_id_str,
        mode = ?config.mode,
        api_port = config.api_port,
        "frostgum starting"
    );

    let pool = db::connect(&config.postgres_url).await.map_err(|e| {
        tracing::error!(error = %e, "failed to connect to postgres");
        e
    })?;

    tracing::info!("connected to postgres");

    let idl = idl::loader::load_idl(
        &config.program_id_str,
        config.idl_path.as_deref(),
        &config.helius_rpc_url,
    )
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to load IDL");
        e
    })?;

    tracing::info!(
        program_name = %idl.metadata.name,
        instructions = idl.instructions.len(),
        account_types = idl.accounts.len(),
        "IDL loaded"
    );

    db::schema::apply_schema_for_idl(&pool, &idl).await.map_err(|e| {
        tracing::error!(error = %e, "failed to apply dynamic schema");
        e
    })?;

    let idl_json = serde_json::to_value(&idl)?;
    db::postgres::register_program(
        &pool,
        &config.program_id_str,
        &idl.metadata.name,
        &idl_json,
    )
    .await?;

    let idl = Arc::new(idl);
    let config = Arc::new(config);

    let app_state = Arc::new(AppState {
        pool: pool.clone(),
        idl: idl.clone(),
        config: config.clone(),
    });

    let router = api::build_router(app_state);
    let bind_addr = format!("0.0.0.0:{}", config.api_port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await.map_err(|e| {
        tracing::error!(addr = %bind_addr, error = %e, "failed to bind API server");
        e
    })?;

    tracing::info!(addr = %bind_addr, "API server listening");

    let indexer_ctx = indexer::IndexerContext::new(
        (*config).clone(),
        (*idl).clone(),
        pool,
    );

    let mode = config.mode.clone();

    let indexer_handle = tokio::spawn(async move {
        let result = match mode {
            IndexMode::Batch => indexer::batch::run_batch(&indexer_ctx).await,
            IndexMode::Realtime => indexer::realtime::run_realtime(&indexer_ctx).await,
        };

        if let Err(e) = result {
            tracing::error!(error = %e, "indexer exited with error");
        }
    });

    tokio::select! {
        result = axum::serve(listener, router) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "API server error");
            }
        }
        _ = signal::ctrl_c() => {
            tracing::info!("received SIGINT, shutting down");
        }
    }

    indexer_handle.abort();
    tracing::info!("frostgum stopped");

    Ok(())
}
