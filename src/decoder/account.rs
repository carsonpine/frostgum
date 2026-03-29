use anyhow::{anyhow, Result};
use serde_json::{json, Value};

use crate::idl::{Idl, IdlAccountType, IdlTypeDefKind};

use super::{ColumnValue, DecodedAccount, DecodedField};
use super::instruction::{decode_value, json_to_column_value};

pub fn try_decode_account(
    address: &str,
    data: &[u8],
    slot: u64,
    idl: &Idl,
) -> Option<DecodedAccount> {
    if data.len() < 8 {
        return None;
    }

    let disc = &data[..8];

    let matched = idl
        .accounts
        .iter()
        .find(|a| a.effective_discriminator().as_slice() == disc)?;

    match decode_account_fields(address, data, slot, matched, idl) {
        Ok(decoded) => Some(decoded),
        Err(e) => {
            tracing::warn!(account = %address, error = %e, "failed to decode account");
            None
        }
    }
}

fn decode_account_fields(
    address: &str,
    data: &[u8],
    slot: u64,
    account_type: &IdlAccountType,
    idl: &Idl,
) -> Result<DecodedAccount> {
    let body = &data[8..];

    let type_def = idl
        .types
        .iter()
        .find(|t| t.name == account_type.name);

    let (fields, raw) = match type_def {
        Some(td) => {
            match &td.ty {
                IdlTypeDefKind::Struct { fields: schema_fields } => {
                    let mut cursor: &[u8] = body;
                    let mut decoded_fields = Vec::new();
                    let mut raw_obj = serde_json::Map::new();

                    for schema_field in schema_fields {
                        let value = decode_value(&mut cursor, &schema_field.ty, &idl.types)?;
                        let col_value = json_to_column_value(&value, &schema_field.ty);
                        raw_obj.insert(schema_field.name.clone(), value);
                        decoded_fields.push(DecodedField {
                            name: schema_field.name.clone(),
                            value: col_value,
                        });
                    }

                    (decoded_fields, Value::Object(raw_obj))
                }
                _ => {
                    let raw_hex = hex::encode(&body[..body.len().min(256)]);
                    (vec![], json!({ "raw_hex": raw_hex }))
                }
            }
        }
        None => {
            let raw_hex = hex::encode(&body[..body.len().min(256)]);
            (vec![], json!({ "raw_hex": raw_hex }))
        }
    };

    Ok(DecodedAccount {
        address: address.to_string(),
        slot_updated: slot,
        account_name: account_type.name.clone(),
        fields,
        raw,
    })
}
