use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::db::postgres;

use super::{backfill, batch, IndexerContext};

pub async fn run_realtime(ctx: &IndexerContext) -> Result<()> {
    let tip_slot = backfill::run_backfill(ctx).await?;

    tracing::info!(
        program = %ctx.config.program_id_str,
        slot = tip_slot,
        "backfill done, switching to real-time WebSocket"
    );

    let mut reconnect_delay = Duration::from_millis(500);
    let max_delay = Duration::from_secs(60);

    loop {
        match subscribe_loop(ctx).await {
            Ok(()) => {
                tracing::info!("WebSocket stream ended cleanly");
                break;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    reconnect_delay_ms = reconnect_delay.as_millis(),
                    "WebSocket error, reconnecting"
                );

                let jitter = rand::thread_rng().gen_range(0.0..0.1) * reconnect_delay.as_secs_f64();
                tokio::time::sleep(reconnect_delay + Duration::from_secs_f64(jitter)).await;

                reconnect_delay = (reconnect_delay * 2).min(max_delay);

                if let Err(backfill_err) = backfill::run_backfill(ctx).await {
                    tracing::warn!(error = %backfill_err, "backfill during reconnect failed");
                }
            }
        }
    }

    Ok(())
}

async fn subscribe_loop(ctx: &IndexerContext) -> Result<()> {
    let ws_url = &ctx.config.helius_ws_url;

    let (ws_stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| anyhow!("WebSocket connect failed: {}", e))?;

    tracing::info!(url = %ws_url, "WebSocket connected");

    let (mut write, mut read) = ws_stream.split();

    let subscribe_msg = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            { "mentions": [ctx.config.program_id_str] },
            { "commitment": "confirmed" }
        ]
    });

    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await
        .map_err(|e| anyhow!("failed to send subscribe message: {}", e))?;

    let mut subscription_id: Option<u64> = None;
    let ping_interval = Duration::from_secs(30);
    let mut last_ping = tokio::time::Instant::now();

    loop {
        let timeout = tokio::time::sleep(ping_interval);
        tokio::pin!(timeout);

        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(e) = handle_ws_message(ctx, &text, &mut subscription_id).await {
                            tracing::warn!(error = %e, "error handling WebSocket message");
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await
                            .map_err(|e| anyhow!("failed to send pong: {}", e))?;
                    }
                    Some(Ok(Message::Close(_))) => {
                        return Err(anyhow!("WebSocket closed by server"));
                    }
                    Some(Err(e)) => {
                        return Err(anyhow!("WebSocket read error: {}", e));
                    }
                    None => {
                        return Err(anyhow!("WebSocket stream ended"));
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                write.send(Message::Ping(vec![])).await
                    .map_err(|e| anyhow!("failed to send ping: {}", e))?;
                last_ping = tokio::time::Instant::now();
            }
        }
    }
}

async fn handle_ws_message(
    ctx: &IndexerContext,
    text: &str,
    subscription_id: &mut Option<u64>,
) -> Result<()> {
    let msg: Value = serde_json::from_str(text)
        .map_err(|e| anyhow!("failed to parse WebSocket message: {}", e))?;

    if let Some(result) = msg.get("result") {
        if let Some(id) = result.as_u64() {
            *subscription_id = Some(id);
            tracing::info!(subscription_id = id, "WebSocket subscription confirmed");
        }
        return Ok(());
    }

    if msg.get("method").and_then(|m| m.as_str()) == Some("logsNotification") {
        let value = msg
            .pointer("/params/result/value")
            .ok_or_else(|| anyhow!("missing value in logsNotification"))?;

        if value.get("err").map(|e| !e.is_null()).unwrap_or(false) {
            return Ok(());
        }

        let signature = value
            .get("signature")
            .and_then(|s| s.as_str())
            .ok_or_else(|| anyhow!("missing signature in logsNotification"))?;

        let slot = msg
            .pointer("/params/result/context/slot")
            .and_then(|s| s.as_u64())
            .unwrap_or(0);

        tracing::debug!(sig = %signature, slot, "received log notification");

        match ctx.rpc.get_transaction(signature).await? {
            Some(tx) => {
                let sigs = vec![signature.to_string()];
                if let Err(e) = batch::index_signatures(ctx, &sigs).await {
                    tracing::warn!(sig = %signature, error = %e, "failed to index realtime tx");
                } else {
                    postgres::set_checkpoint(
                        &ctx.pool,
                        &ctx.config.program_id_str,
                        "last_slot",
                        slot as i64,
                    )
                    .await?;
                }
            }
            None => {
                tracing::warn!(sig = %signature, "realtime tx not found");
            }
        }
    }

    Ok(())
}
