use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::db::postgres;
use crate::idl::schema_gen::{account_table_name, instruction_table_name};

use super::aggregation::{run_aggregation, AggregationQuery};
use super::filters::{AccountListQuery, InstructionQuery, RawSqlBody};
use super::AppState;

pub async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

pub async fn list_programs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match postgres::list_programs(&state.pool).await {
        Ok(programs) => (StatusCode::OK, Json(json!({ "programs": programs }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

pub async fn get_program_stats(
    State(state): State<Arc<AppState>>,
    Path(program_id): Path<String>,
) -> impl IntoResponse {
    if program_id != state.config.program_id_str {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "program not indexed" })),
        );
    }

    let ix_names: Vec<String> = state
        .idl
        .instructions
        .iter()
        .map(|i| i.name.clone())
        .collect();

    match postgres::get_program_stats(&state.pool, &program_id, &ix_names).await {
        Ok(stats) => (StatusCode::OK, Json(stats)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

pub async fn list_instructions(
    State(state): State<Arc<AppState>>,
    Path(program_id): Path<String>,
) -> impl IntoResponse {
    if program_id != state.config.program_id_str {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "program not indexed" })),
        );
    }

    let instructions: Vec<Value> = state
        .idl
        .instructions
        .iter()
        .map(|ix| {
            json!({
                "name": ix.name,
                "args": ix.args.iter().map(|a| json!({ "name": a.name })).collect::<Vec<_>>(),
                "num_accounts": ix.accounts.len(),
                "table": instruction_table_name(&program_id, &ix.name),
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({ "program_id": program_id, "instructions": instructions })),
    )
}

pub async fn query_instruction(
    State(state): State<Arc<AppState>>,
    Path((program_id, ix_name)): Path<(String, String)>,
    Query(query): Query<InstructionQuery>,
) -> impl IntoResponse {
    if program_id != state.config.program_id_str {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "program not indexed" })),
        );
    }

    if !state.idl.instructions.iter().any(|i| i.name == ix_name) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("instruction '{}' not found in IDL", ix_name) })),
        );
    }

    let table = instruction_table_name(&program_id, &ix_name);
    let conditions = query.conditions();
    let conditions_ref: Vec<(&str, String)> = conditions.clone();

    let mut slot_conditions = Vec::new();
    if let Some(start) = query.start_slot {
        slot_conditions.push(("start_slot", start.to_string()));
    }
    if let Some(end) = query.end_slot {
        slot_conditions.push(("end_slot", end.to_string()));
    }

    let all_conds: Vec<(&str, String)> = conditions_ref;

    match postgres::query_instructions(
        &state.pool,
        &table,
        &all_conds,
        query.order_clause(),
        query.limit(),
        query.offset(),
    )
    .await
    {
        Ok(rows) => (
            StatusCode::OK,
            Json(json!({
                "program_id": program_id,
                "instruction": ix_name,
                "count": rows.len(),
                "data": rows,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

pub async fn aggregate_instruction(
    State(state): State<Arc<AppState>>,
    Path((program_id, ix_name)): Path<(String, String)>,
    Query(query): Query<AggregationQuery>,
) -> impl IntoResponse {
    if program_id != state.config.program_id_str {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "program not indexed" })),
        );
    }

    if !state.idl.instructions.iter().any(|i| i.name == ix_name) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("instruction '{}' not in IDL", ix_name) })),
        );
    }

    match run_aggregation(&state.pool, &program_id, &ix_name, &query).await {
        Ok(result) => (StatusCode::OK, Json(result)),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

pub async fn list_account_types(
    State(state): State<Arc<AppState>>,
    Path(program_id): Path<String>,
) -> impl IntoResponse {
    if program_id != state.config.program_id_str {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "program not indexed" })),
        );
    }

    let account_types: Vec<Value> = state
        .idl
        .accounts
        .iter()
        .map(|a| {
            json!({
                "name": a.name,
                "table": account_table_name(&program_id, &a.name),
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({ "program_id": program_id, "account_types": account_types })),
    )
}

pub async fn query_accounts_by_type(
    State(state): State<Arc<AppState>>,
    Path((program_id, account_type)): Path<(String, String)>,
    Query(query): Query<AccountListQuery>,
) -> impl IntoResponse {
    if program_id != state.config.program_id_str {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "program not indexed" })),
        );
    }

    if !state.idl.accounts.iter().any(|a| a.name == account_type) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("account type '{}' not in IDL", account_type) })),
        );
    }

    let table = account_table_name(&program_id, &account_type);

    match postgres::query_accounts(&state.pool, &table, query.limit(), query.offset()).await {
        Ok(rows) => (
            StatusCode::OK,
            Json(json!({
                "program_id": program_id,
                "account_type": account_type,
                "count": rows.len(),
                "data": rows,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

pub async fn get_account_by_address(
    State(state): State<Arc<AppState>>,
    Path((program_id, account_type, address)): Path<(String, String, String)>,
) -> impl IntoResponse {
    if program_id != state.config.program_id_str {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "program not indexed" })),
        );
    }

    if !state.idl.accounts.iter().any(|a| a.name == account_type) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("account type '{}' not in IDL", account_type) })),
        );
    }

    let table = account_table_name(&program_id, &account_type);

    match postgres::get_account_by_address(&state.pool, &table, &address).await {
        Ok(Some(row)) => (StatusCode::OK, Json(row)),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "account not found" })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

pub async fn raw_sql(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RawSqlBody>,
) -> impl IntoResponse {
    let sql = body.sql.trim();

    let lower = sql.to_lowercase();
    if lower.starts_with("drop ")
        || lower.starts_with("delete ")
        || lower.starts_with("truncate ")
        || lower.starts_with("alter ")
        || lower.starts_with("create ")
        || lower.starts_with("insert ")
        || lower.starts_with("update ")
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "only SELECT queries are allowed" })),
        );
    }

    match postgres::execute_raw_sql(&state.pool, sql).await {
        Ok(rows) => (
            StatusCode::OK,
            Json(json!({ "count": rows.len(), "data": rows })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}
