pub mod aggregation;
pub mod filters;
pub mod routes;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use sqlx::PgPool;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;

use crate::config::Config;
use crate::idl::Idl;

pub struct AppState {
    pub pool: PgPool,
    pub idl: Arc<Idl>,
    pub config: Arc<Config>,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(routes::health))
        .route("/api/meta", get(routes::get_meta))
        .route("/programs", get(routes::list_programs))
        .route("/programs/:program_id/stats", get(routes::get_program_stats))
        .route("/programs/:program_id/instructions", get(routes::list_instructions))
        .route("/programs/:program_id/instructions/:name", get(routes::query_instruction))
        .route("/programs/:program_id/instructions/:name/aggregate", get(routes::aggregate_instruction))
        .route("/programs/:program_id/accounts", get(routes::list_account_types))
        .route("/programs/:program_id/accounts/:type", get(routes::query_accounts_by_type))
        .route("/programs/:program_id/accounts/:type/:address", get(routes::get_account_by_address))
        .route("/api/sql", post(routes::raw_sql))
        .nest_service("/", ServeDir::new("static").append_index_html_on_directories(true))
        .layer(cors)
        .with_state(state)
}
