use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use flate2::read::ZlibDecoder;
use reqwest::Client;
use serde_json::{json, Value};
use std::io::Read;
use std::path::Path;

use super::Idl;

pub async fn load_idl(program_id: &str, idl_path: Option<&str>, rpc_url: &str) -> Result<Idl> {
    if let Some(path) = idl_path {
        load_from_file(path).await
    } else {
        load_from_chain(program_id, rpc_url).await
    }
}

async fn load_from_file(path: &str) -> Result<Idl> {
    let canonical = Path::new(path)
        .canonicalize()
        .with_context(|| format!("IDL file not found: {}", path))?;

    let contents = tokio::fs::read_to_string(&canonical)
        .await
        .with_context(|| format!("failed to read IDL file: {}", canonical.display()))?;

    let idl: Idl = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse IDL JSON from: {}", canonical.display()))?;

    tracing::info!(path = %canonical.display(), "loaded IDL from file");
    Ok(idl)
}

async fn load_from_chain(program_id: &str, rpc_url: &str) -> Result<Idl> {
    let idl_address = derive_idl_address(program_id)?;
    tracing::info!(program = %program_id, idl_account = %idl_address, "fetching IDL from chain");

    let client = Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            idl_address,
            {
                "encoding": "base64",
                "commitment": "confirmed"
            }
        ]
    });

    let resp: Value = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("failed to send getAccountInfo request")?
        .json()
        .await
        .context("failed to parse getAccountInfo response")?;

    let data_b64 = resp
        .pointer("/result/value/data/0")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("IDL account not found on chain for program {}", program_id))?;

    let raw = BASE64
        .decode(data_b64)
        .context("failed to base64-decode IDL account data")?;

    let idl = decode_anchor_idl_account(&raw)
        .context("failed to decode Anchor IDL account data")?;

    tracing::info!(program = %program_id, name = %idl.metadata.name, "loaded IDL from chain");
    Ok(idl)
}

fn decode_anchor_idl_account(data: &[u8]) -> Result<Idl> {
    if data.len() < 8 {
        return Err(anyhow!("IDL account data too short"));
    }

    let data = &data[8..];

    if data.len() < 4 {
        return Err(anyhow!("IDL account data missing authority"));
    }
    let _authority = &data[..32.min(data.len())];

    let offset = if data.len() >= 32 { 32 } else { 0 };
    let remaining = &data[offset..];

    if remaining.len() < 4 {
        return Err(anyhow!("IDL account data missing data length"));
    }
    let data_len = u32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]]) as usize;

    if remaining.len() < 4 + data_len {
        return Err(anyhow!("IDL account data truncated"));
    }

    let compressed = &remaining[4..4 + data_len];
    let mut decoder = ZlibDecoder::new(compressed);
    let mut json_bytes = Vec::new();
    decoder
        .read_to_end(&mut json_bytes)
        .context("failed to zlib-decompress IDL data")?;

    let idl: Idl = serde_json::from_slice(&json_bytes)
        .context("failed to parse decompressed IDL JSON")?;

    Ok(idl)
}

fn derive_idl_address(program_id: &str) -> Result<String> {
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    let program_pubkey = Pubkey::from_str(program_id)
        .with_context(|| format!("invalid program ID: {}", program_id))?;

    let base = Pubkey::find_program_address(&[b"anchor:idl"], &program_pubkey);
    Ok(base.0.to_string())
}
