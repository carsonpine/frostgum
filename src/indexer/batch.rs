use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::str::FromStr;

use crate::db::postgres;
use crate::decoder::account::try_decode_account;
use crate::decoder::instruction::decode_instructions_from_tx;

use super::IndexerContext;

pub async fn index_signatures(
    ctx: &IndexerContext,
    signatures: &[String],
) -> Result<(usize, usize)> {
    let mut ix_count = 0usize;
    let mut acct_count = 0usize;

    for signature in signatures {
        let tx = match ctx.rpc.get_transaction(signature).await? {
            Some(tx) => tx,
            None => {
                tracing::warn!(sig = %signature, "transaction not found, skipping");
                continue;
            }
        };

        if let Some(meta) = &tx.transaction.meta {
            if meta.err.is_some() {
                continue;
            }
        }

        match decode_instructions_from_tx(
            &tx,
            &ctx.config.program_id_str,
            &ctx.idl,
            signature,
        ) {
            Ok(decoded_ixs) => {
                for decoded in &decoded_ixs {
                    match postgres::insert_instruction(&ctx.pool, decoded).await {
                        Ok(_) => ix_count += 1,
                        Err(e) => tracing::warn!(sig = %signature, error = %e, "failed to insert instruction"),
                    }
                }
            }
            Err(e) => tracing::warn!(sig = %signature, error = %e, "failed to decode transaction"),
        }

        let writable_accounts = extract_writable_accounts(&tx);
        let pubkeys: Vec<Pubkey> = writable_accounts
            .iter()
            .filter_map(|s| Pubkey::from_str(s).ok())
            .collect();

        if !pubkeys.is_empty() {
            match ctx.rpc.get_multiple_account_data(&pubkeys).await {
                Ok(account_data_list) => {
                    for (pubkey, maybe_data) in pubkeys.iter().zip(account_data_list.iter()) {
                        if let Some(data) = maybe_data {
                            if let Some(decoded_acct) = try_decode_account(
                                &pubkey.to_string(),
                                data,
                                tx.slot,
                                &ctx.idl,
                            ) {
                                match postgres::upsert_account(
                                    &ctx.pool,
                                    &decoded_acct,
                                    &ctx.config.program_id_str,
                                )
                                .await
                                {
                                    Ok(_) => acct_count += 1,
                                    Err(e) => tracing::warn!(
                                        account = %pubkey,
                                        error = %e,
                                        "failed to upsert account"
                                    ),
                                }
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "failed to fetch account data batch"),
            }
        }
    }

    Ok((ix_count, acct_count))
}

pub async fn run_batch(ctx: &IndexerContext) -> Result<()> {
    let program_id = &ctx.config.program_id;
    let program_id_str = &ctx.config.program_id_str;
    let batch_size = ctx.config.batch_size;

    let start_slot = ctx.config.start_slot;
    let end_slot = ctx.config.end_slot;

    tracing::info!(
        program = %program_id_str,
        start_slot = ?start_slot,
        end_slot = ?end_slot,
        "starting batch indexing"
    );

    let mut before_sig: Option<Signature> = None;
    let mut total_ixs = 0usize;
    let mut total_accts = 0usize;
    let mut page = 0usize;

    loop {
        let sigs = ctx
            .rpc
            .get_signatures_for_address(program_id, before_sig, None, batch_size)
            .await?;

        if sigs.is_empty() {
            break;
        }

        let last_sig_str = sigs.last().map(|s| s.signature.clone()).unwrap_or_default();

        let filtered: Vec<String> = sigs
            .into_iter()
            .filter(|s| {
                if s.err.is_some() {
                    return false;
                }
                if let Some(slot_start) = start_slot {
                    if s.slot < slot_start {
                        return false;
                    }
                }
                if let Some(slot_end) = end_slot {
                    if s.slot > slot_end {
                        return false;
                    }
                }
                true
            })
            .map(|s| s.signature)
            .collect();

        let (ixs, accts) = index_signatures(ctx, &filtered).await?;
        total_ixs += ixs;
        total_accts += accts;
        page += 1;

        tracing::info!(
            page,
            batch_sigs = filtered.len(),
            total_ixs,
            total_accts,
            "batch page indexed"
        );

        if let Some(_end) = end_slot {
            if let Ok(sig) = Signature::from_str(&last_sig_str) {
                before_sig = Some(sig);
            }
        } else {
            match Signature::from_str(&last_sig_str) {
                Ok(sig) => before_sig = Some(sig),
                Err(_) => break,
            }
        }

        if filtered.len() < batch_size {
            break;
        }
    }

    tracing::info!(
        program = %program_id_str,
        total_ixs,
        total_accts,
        "batch indexing complete"
    );

    Ok(())
}

fn extract_writable_accounts(
    tx: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
) -> Vec<String> {
    use solana_transaction_status::{EncodedTransaction, UiMessage};

    let ui_tx = match &tx.transaction.transaction {
        EncodedTransaction::Json(t) => t,
        _ => return vec![],
    };

    match &ui_tx.message {
        UiMessage::Raw(msg) => {
            let num_signers = msg.header.num_required_signatures as usize;
            let num_readonly_signed = msg.header.num_readonly_signed_accounts as usize;
            let num_readonly_unsigned = msg.header.num_readonly_unsigned_accounts as usize;

            let total = msg.account_keys.len();
            let num_unsigned = total.saturating_sub(num_signers);
            let num_writable_signed = num_signers.saturating_sub(num_readonly_signed);
            let num_writable_unsigned = num_unsigned.saturating_sub(num_readonly_unsigned);

            let mut writable = Vec::new();
            for i in 0..num_writable_signed {
                if let Some(k) = msg.account_keys.get(i) {
                    writable.push(k.clone());
                }
            }
            for i in num_signers..num_signers + num_writable_unsigned {
                if let Some(k) = msg.account_keys.get(i) {
                    writable.push(k.clone());
                }
            }
            writable
        }
        _ => vec![],
    }
}
