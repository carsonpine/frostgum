use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::idl::schema_gen::instruction_table_name;

#[derive(Debug, Deserialize)]
pub struct AggregationQuery {
    pub metric: Option<String>,
    pub window: Option<String>,
    pub start_slot: Option<i64>,
    pub end_slot: Option<i64>,
    pub group_by: Option<String>,
}

pub async fn run_aggregation(
    pool: &PgPool,
    program_id: &str,
    ix_name: &str,
    query: &AggregationQuery,
) -> Result<Value> {
    let table = instruction_table_name(program_id, ix_name);

    if !is_valid_identifier(&table) {
        return Err(anyhow!("invalid table: {}", table));
    }

    let metric = query.metric.as_deref().unwrap_or("count");

    let mut conditions = Vec::new();
    if let Some(start) = query.start_slot {
        conditions.push(format!("slot >= {}", start));
    }
    if let Some(end) = query.end_slot {
        conditions.push(format!("slot <= {}", end));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    match metric {
        "count" => {
            if let Some(window) = &query.window {
                let _interval = parse_window_interval(window)?;
                let bucket_seconds = interval_to_seconds(window)?;

                let group_expr = format!("(slot / {}) * {}", bucket_seconds / 400, bucket_seconds / 400);

                let sql = format!(
                    "SELECT {} AS slot_bucket, COUNT(*) AS count, MIN(slot) AS min_slot, MAX(slot) AS max_slot
                     FROM {} {}
                     GROUP BY slot_bucket
                     ORDER BY slot_bucket DESC
                     LIMIT 100",
                    group_expr, table, where_clause
                );

                let rows = sqlx::query(&sql).fetch_all(pool).await
                    .map_err(|e| anyhow!("aggregation query failed: {}", e))?;

                let buckets: Vec<Value> = rows.iter().map(|r| {
                    serde_json::json!({
                        "slot_bucket": r.try_get::<i64, _>("slot_bucket").unwrap_or(0),
                        "count": r.try_get::<i64, _>("count").unwrap_or(0),
                        "min_slot": r.try_get::<i64, _>("min_slot").unwrap_or(0),
                        "max_slot": r.try_get::<i64, _>("max_slot").unwrap_or(0),
                    })
                }).collect();

                return Ok(serde_json::json!({
                    "metric": "count",
                    "window": window,
                    "buckets": buckets,
                }));
            }

            let sql = format!("SELECT COUNT(*) AS count, MIN(slot) AS first_slot, MAX(slot) AS last_slot FROM {} {}", table, where_clause);
            let row = sqlx::query(&sql).fetch_one(pool).await
                .map_err(|e| anyhow!("count query failed: {}", e))?;

            Ok(serde_json::json!({
                "metric": "count",
                "count": row.try_get::<i64, _>("count").unwrap_or(0),
                "first_slot": row.try_get::<i64, _>("first_slot").ok(),
                "last_slot": row.try_get::<i64, _>("last_slot").ok(),
            }))
        }
        "unique_signers" => {
            let sql = format!("SELECT COUNT(DISTINCT signer) AS count FROM {} {}", table, where_clause);
            let row = sqlx::query(&sql).fetch_one(pool).await
                .map_err(|e| anyhow!("unique_signers query failed: {}", e))?;

            Ok(serde_json::json!({
                "metric": "unique_signers",
                "count": row.try_get::<i64, _>("count").unwrap_or(0),
            }))
        }
        "top_signers" => {
            let sql = format!(
                "SELECT signer, COUNT(*) AS count FROM {} {} GROUP BY signer ORDER BY count DESC LIMIT 20",
                table, where_clause
            );

            let rows = sqlx::query(&sql).fetch_all(pool).await
                .map_err(|e| anyhow!("top_signers query failed: {}", e))?;

            let top: Vec<Value> = rows.iter().map(|r| {
                serde_json::json!({
                    "signer": r.try_get::<String, _>("signer").unwrap_or_default(),
                    "count": r.try_get::<i64, _>("count").unwrap_or(0),
                })
            }).collect();

            Ok(serde_json::json!({
                "metric": "top_signers",
                "results": top,
            }))
        }
        _ => Err(anyhow!("unsupported metric '{}'. supported: count, unique_signers, top_signers", metric)),
    }
}

fn parse_window_interval(window: &str) -> Result<String> {
    match window {
        "1m" => Ok("1 minute".to_string()),
        "5m" => Ok("5 minutes".to_string()),
        "15m" => Ok("15 minutes".to_string()),
        "1h" => Ok("1 hour".to_string()),
        "6h" => Ok("6 hours".to_string()),
        "1d" => Ok("1 day".to_string()),
        _ => Err(anyhow!("unsupported window '{}'. supported: 1m, 5m, 15m, 1h, 6h, 1d", window)),
    }
}

fn interval_to_seconds(window: &str) -> Result<i64> {
    match window {
        "1m" => Ok(60),
        "5m" => Ok(300),
        "15m" => Ok(900),
        "1h" => Ok(3600),
        "6h" => Ok(21600),
        "1d" => Ok(86400),
        _ => Err(anyhow!("unsupported window: {}", window)),
    }
}

fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}
