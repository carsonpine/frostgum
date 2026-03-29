use anyhow::Result;
use solana_sdk::signature::Signature;
use std::str::FromStr;

use crate::db::postgres;

use super::{batch, IndexerContext};

pub async fn run_backfill(ctx: &IndexerContext) -> Result<u64> {
    let program_id_str = &ctx.config.program_id_str;
    let program_id = &ctx.config.program_id;

    let last_indexed = postgres::get_checkpoint(&ctx.pool, program_id_str, "last_slot")
        .await?
        .map(|v| v as u64);

    let current_slot = ctx.rpc.get_slot().await?;

    let from_slot = match last_indexed {
        Some(last) if last >= current_slot => {
            tracing::info!(program = %program_id_str, "already at tip, no backfill needed");
            return Ok(current_slot);
        }
        Some(last) => last + 1,
        None => ctx.config.start_slot.unwrap_or(0),
    };

    tracing::info!(
        program = %program_id_str,
        from_slot,
        current_slot,
        slots_behind = current_slot.saturating_sub(from_slot),
        "starting backfill"
    );

    let batch_size = ctx.config.batch_size;
    let mut before_sig: Option<Signature> = None;
    let mut total_ixs = 0usize;
    let mut total_accts = 0usize;
    let mut page = 0usize;
    let mut last_processed_slot = from_slot;

    loop {
        let sigs = ctx
            .rpc
            .get_signatures_for_address(program_id, before_sig, None, batch_size)
            .await?;

        if sigs.is_empty() {
            break;
        }

        let last_sig_str = sigs.last().map(|s| s.signature.clone()).unwrap_or_default();

        let oldest_slot_in_batch = sigs
            .last()
            .and_then(|s| s.slot)
            .unwrap_or(u64::MAX);

        let relevant: Vec<String> = sigs
            .into_iter()
            .filter(|s| {
                if s.err.is_some() {
                    return false;
                }
                if let Some(slot) = s.slot {
                    slot >= from_slot && slot <= current_slot
                } else {
                    false
                }
            })
            .map(|s| s.signature)
            .collect();

        if !relevant.is_empty() {
            let (ixs, accts) = batch::index_signatures(ctx, &relevant).await?;
            total_ixs += ixs;
            total_accts += accts;

            postgres::set_checkpoint(
                &ctx.pool,
                program_id_str,
                "last_slot",
                current_slot as i64,
            )
            .await?;

            last_processed_slot = oldest_slot_in_batch;
        }

        page += 1;

        tracing::info!(
            page,
            batch_sigs = relevant.len(),
            oldest_slot_in_batch,
            total_ixs,
            total_accts,
            "backfill page complete"
        );

        if oldest_slot_in_batch <= from_slot {
            break;
        }

        if relevant.len() < batch_size {
            break;
        }

        match Signature::from_str(&last_sig_str) {
            Ok(sig) => before_sig = Some(sig),
            Err(_) => break,
        }
    }

    postgres::set_checkpoint(
        &ctx.pool,
        program_id_str,
        "last_slot",
        current_slot as i64,
    )
    .await?;

    tracing::info!(
        program = %program_id_str,
        total_ixs,
        total_accts,
        current_slot,
        "backfill complete"
    );

    Ok(current_slot)
}
