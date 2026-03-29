use anyhow::{anyhow, Context, Result};
use rand::Rng;
use solana_rpc_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
use solana_rpc_client_api::config::{RpcTransactionConfig, RpcSignaturesForAddressConfig};
use solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<SolanaRpcClient>,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl RpcClient {
    pub fn new(rpc_url: &str, max_retries: u32, initial_backoff_ms: u64, max_backoff_ms: u64) -> Self {
        let inner = SolanaRpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        );
        Self {
            inner: Arc::new(inner),
            max_retries,
            initial_backoff: Duration::from_millis(initial_backoff_ms),
            max_backoff: Duration::from_millis(max_backoff_ms),
        }
    }

    async fn with_retry<F, Fut, T>(&self, operation: &str, f: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0u32;
        let mut backoff = self.initial_backoff;

        loop {
            match f().await {
                Ok(val) => return Ok(val),
                Err(e) if attempt < self.max_retries => {
                    let jitter_secs = rand::thread_rng().gen_range(0.0..0.1) * backoff.as_secs_f64();
                    let sleep_time = backoff + Duration::from_secs_f64(jitter_secs);

                    tracing::warn!(
                        operation,
                        attempt,
                        backoff_ms = sleep_time.as_millis(),
                        error = %e,
                        "RPC call failed, retrying"
                    );

                    tokio::time::sleep(sleep_time).await;
                    backoff = (backoff * 2).min(self.max_backoff);
                    attempt += 1;
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("RPC operation '{}' failed after {} attempts", operation, attempt + 1)
                    });
                }
            }
        }
    }

    pub async fn get_slot(&self) -> Result<u64> {
        self.with_retry("getSlot", || async {
            self.inner
                .get_slot()
                .await
                .map_err(|e| anyhow!("getSlot failed: {}", e))
        })
        .await
    }

    pub async fn get_signatures_for_address(
        &self,
        pubkey: &Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let config = RpcSignaturesForAddressConfig {
            before: before.map(|s| s.to_string()),
            until: until.map(|s| s.to_string()),
            limit: Some(limit),
            commitment: Some(CommitmentConfig::confirmed()),
            min_context_slot: None,
        };

        self.with_retry("getSignaturesForAddress", || {
            let inner = self.inner.clone();
            let config = config.clone();
            async move {
                inner
                    .get_signatures_for_address_with_config(pubkey, config)
                    .await
                    .map_err(|e| anyhow!("getSignaturesForAddress failed: {}", e))
            }
        })
        .await
    }

    pub async fn get_transaction(
        &self,
        signature: &str,
    ) -> Result<Option<EncodedConfirmedTransactionWithStatusMeta>> {
        let sig = Signature::from_str(signature)
            .map_err(|e| anyhow!("invalid signature '{}': {}", signature, e))?;

        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        self.with_retry("getTransaction", || {
            let inner = self.inner.clone();
            let config = config.clone();
            async move {
                inner
                    .get_transaction_with_config(&sig, config)
                    .await
                    .map(Some)
                    .or_else(|e| {
                        let msg = e.to_string();
                        if msg.contains("not found") || msg.contains("was not confirmed") {
                            Ok(None)
                        } else {
                            Err(anyhow!("getTransaction failed: {}", msg))
                        }
                    })
            }
        })
        .await
    }

    pub async fn get_account_data(&self, pubkey_str: &str) -> Result<Option<Vec<u8>>> {
        let pubkey = Pubkey::from_str(pubkey_str)
            .map_err(|e| anyhow!("invalid pubkey '{}': {}", pubkey_str, e))?;

        self.with_retry("getAccountInfo", || {
            let inner = self.inner.clone();
            async move {
                inner
                    .get_account(&pubkey)
                    .await
                    .map(|acct| Some(acct.data))
                    .or_else(|e| {
                        let msg = e.to_string();
                        if msg.contains("AccountNotFound") || msg.contains("not found") {
                            Ok(None)
                        } else {
                            Err(anyhow!("getAccountInfo failed: {}", msg))
                        }
                    })
            }
        })
        .await
    }

    pub async fn get_multiple_account_data(
        &self,
        pubkeys: &[Pubkey],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        if pubkeys.is_empty() {
            return Ok(vec![]);
        }

        self.with_retry("getMultipleAccounts", || {
            let inner = self.inner.clone();
            let pubkeys = pubkeys.to_vec();
            async move {
                inner
                    .get_multiple_accounts(&pubkeys)
                    .await
                    .map(|accounts| accounts.into_iter().map(|a| a.map(|acct| acct.data)).collect())
                    .map_err(|e| anyhow!("getMultipleAccounts failed: {}", e))
            }
        })
        .await
    }
}
