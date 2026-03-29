use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use sqlx::postgres::PgArguments;
use sqlx::{Arguments as _, PgPool, Row};

use crate::decoder::{DecodedAccount, DecodedInstruction};
use crate::idl::schema_gen::{instruction_table_name, account_table_name};

pub async fn register_program(
    pool: &PgPool,
    program_id: &str,
    name: &str,
    idl: &Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO programs (program_id, name, idl) VALUES ($1, $2, $3)
         ON CONFLICT (program_id) DO UPDATE SET name = EXCLUDED.name, idl = EXCLUDED.idl",
    )
    .bind(program_id)
    .bind(name)
    .bind(sqlx::types::Json(idl))
    .execute(pool)
    .await
    .context("failed to register program")?;
    Ok(())
}

pub async fn list_programs(pool: &PgPool) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT row_to_json(t) AS row FROM (
            SELECT program_id, name, registered_at FROM programs ORDER BY registered_at
         ) t",
    )
    .fetch_all(pool)
    .await
    .context("failed to list programs")?;

    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<Value, _>("row").ok())
        .collect())
}

pub async fn get_checkpoint(pool: &PgPool, program_id: &str, key: &str) -> Result<Option<i64>> {
    let row = sqlx::query("SELECT value FROM checkpoints WHERE program_id = $1 AND key = $2")
        .bind(program_id)
        .bind(key)
        .fetch_optional(pool)
        .await
        .context("failed to get checkpoint")?;

    Ok(row.map(|r| r.get::<i64, _>("value")))
}

pub async fn set_checkpoint(pool: &PgPool, program_id: &str, key: &str, value: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO checkpoints (program_id, key, value) VALUES ($1, $2, $3)
         ON CONFLICT (program_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(program_id)
    .bind(key)
    .bind(value)
    .execute(pool)
    .await
    .context("failed to set checkpoint")?;
    Ok(())
}

pub async fn insert_instruction(
    pool: &PgPool,
    decoded: &DecodedInstruction,
) -> Result<()> {
    let table = instruction_table_name(&decoded.program_id, &decoded.instruction_name);

    let mut col_names = vec![
        "signature".to_string(),
        "slot".to_string(),
        "block_time".to_string(),
        "signer".to_string(),
    ];

    let mut sql_args: Vec<String> = vec!["$1".to_string(), "$2".to_string(), "$3".to_string(), "$4".to_string()];
    let mut param_idx = 5usize;

    for field in &decoded.args {
        let col = crate::idl::schema_gen::sanitize_name(&field.name);
        col_names.push(col);
        let cast = field.value.sql_cast_suffix();
        sql_args.push(format!("${}{}", param_idx, cast));
        param_idx += 1;
    }

    col_names.push("accounts".to_string());
    sql_args.push(format!("${}::jsonb", param_idx));

    let cols = col_names.join(", ");
    let vals = sql_args.join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING",
        table, cols, vals
    );

    macro_rules! add {
        ($args:expr, $v:expr) => {
            $args.add($v).map_err(|e| anyhow::anyhow!("{}", e))?
        };
    }

    let mut args = PgArguments::default();
    add!(args, decoded.signature.clone());
    add!(args, decoded.slot as i64);
    add!(args, decoded.block_time);
    add!(args, decoded.signer.clone());

    for field in &decoded.args {
        field.value.add_to_args(&mut args)?;
    }

    add!(args, sqlx::types::Json(decoded.accounts.clone()));

    sqlx::query_with(&sql, args)
        .execute(pool)
        .await
        .with_context(|| format!("failed to insert instruction into {}", table))?;

    Ok(())
}

pub async fn upsert_account(pool: &PgPool, decoded: &DecodedAccount, program_id: &str) -> Result<()> {
    let table = account_table_name(program_id, &decoded.account_name);

    let mut col_names = vec![
        "address".to_string(),
        "slot_updated".to_string(),
    ];

    let mut sql_args: Vec<String> = vec!["$1".to_string(), "$2".to_string()];
    let mut param_idx = 3usize;

    for field in &decoded.fields {
        let col = crate::idl::schema_gen::sanitize_name(&field.name);
        col_names.push(col);
        let cast = field.value.sql_cast_suffix();
        sql_args.push(format!("${}{}", param_idx, cast));
        param_idx += 1;
    }

    col_names.push("raw".to_string());
    sql_args.push(format!("${}::jsonb", param_idx));

    col_names.push("updated_at".to_string());
    sql_args.push("NOW()".to_string());

    let cols = col_names.join(", ");
    let vals = sql_args.join(", ");

    let update_clauses: Vec<String> = col_names
        .iter()
        .skip(2)
        .zip(sql_args.iter().skip(2))
        .map(|(col, val)| format!("{} = {}", col, val))
        .collect();
    let update_set = update_clauses.join(", ");

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (address) DO UPDATE SET {} WHERE {}.slot_updated <= EXCLUDED.slot_updated",
        table, cols, vals, update_set, table
    );

    macro_rules! add {
        ($args:expr, $v:expr) => {
            $args.add($v).map_err(|e| anyhow::anyhow!("{}", e))?
        };
    }

    let mut args = PgArguments::default();
    add!(args, decoded.address.clone());
    add!(args, decoded.slot_updated as i64);

    for field in &decoded.fields {
        field.value.add_to_args(&mut args)?;
    }

    add!(args, sqlx::types::Json(decoded.raw.clone()));

    sqlx::query_with(&sql, args)
        .execute(pool)
        .await
        .with_context(|| format!("failed to upsert account into {}", table))?;

    Ok(())
}

pub async fn query_instructions(
    pool: &PgPool,
    table: &str,
    conditions: &[(&str, String)],
    order_by: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Value>> {
    if !is_valid_identifier(table) {
        return Err(anyhow!("invalid table name: {}", table));
    }

    let mut where_clauses = Vec::new();
    let mut args = PgArguments::default();
    let mut param_idx = 1usize;

    for (col, val) in conditions {
        if !is_valid_identifier(col) {
            return Err(anyhow!("invalid column name: {}", col));
        }
        where_clauses.push(format!("{} = ${}", col, param_idx));
        args.add(val.clone()).map_err(|e| anyhow!("{}", e))?;
        param_idx += 1;
    }

    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_clauses.join(" AND "))
    };

    let safe_order = if order_by == "slot ASC" || order_by == "slot DESC" || order_by == "created_at DESC" {
        order_by
    } else {
        "slot DESC"
    };

    let inner_sql = format!(
        "SELECT * FROM {} {} ORDER BY {} LIMIT {} OFFSET {}",
        table, where_sql, safe_order, limit, offset
    );
    let sql = format!("SELECT row_to_json(t) AS row FROM ({}) t", inner_sql);

    let rows = sqlx::query_with(&sql, args)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to query {}", table))?;

    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<Value, _>("row").ok())
        .collect())
}

pub async fn query_accounts(
    pool: &PgPool,
    table: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Value>> {
    if !is_valid_identifier(table) {
        return Err(anyhow!("invalid table name: {}", table));
    }

    let sql = format!(
        "SELECT row_to_json(t) AS row FROM (
            SELECT * FROM {} ORDER BY slot_updated DESC LIMIT {} OFFSET {}
         ) t",
        table, limit, offset
    );

    let rows = sqlx::query(&sql)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to query accounts from {}", table))?;

    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<Value, _>("row").ok())
        .collect())
}

pub async fn get_account_by_address(
    pool: &PgPool,
    table: &str,
    address: &str,
) -> Result<Option<Value>> {
    if !is_valid_identifier(table) {
        return Err(anyhow!("invalid table name: {}", table));
    }

    let sql = format!(
        "SELECT row_to_json(t) AS row FROM (
            SELECT * FROM {} WHERE address = $1 LIMIT 1
         ) t",
        table
    );

    let row = sqlx::query(&sql)
        .bind(address)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("failed to fetch account {} from {}", address, table))?;

    Ok(row.and_then(|r| r.try_get::<Value, _>("row").ok()))
}

pub async fn get_program_stats(
    pool: &PgPool,
    program_id: &str,
    instruction_names: &[String],
) -> Result<Value> {
    let label = crate::idl::schema_gen::program_label(program_id);
    let mut instruction_counts = serde_json::Map::new();

    for ix_name in instruction_names {
        let table = instruction_table_name(program_id, ix_name);
        if !is_valid_identifier(&table) {
            continue;
        }

        let sql = format!("SELECT COUNT(*) as cnt FROM {}", table);
        let row = sqlx::query(&sql).fetch_one(pool).await;
        if let Ok(row) = row {
            let cnt: i64 = row.try_get("cnt").unwrap_or(0);
            instruction_counts.insert(ix_name.clone(), Value::Number(cnt.into()));
        }
    }

    Ok(serde_json::json!({
        "program_id": program_id,
        "label": label,
        "instruction_counts": instruction_counts,
    }))
}

pub async fn execute_raw_sql(pool: &PgPool, sql: &str) -> Result<Vec<Value>> {
    let rows = sqlx::query(&format!(
        "SELECT row_to_json(t) AS row FROM ({}) t",
        sql
    ))
    .fetch_all(pool)
    .await
    .context("raw SQL query failed")?;

    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<Value, _>("row").ok())
        .collect())
}

fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}
