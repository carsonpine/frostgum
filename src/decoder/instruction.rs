use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiMessage};

use crate::idl::{IdlField, IdlType, IdlTypeDef, IdlTypeDefKind, Idl};

use super::{ColumnValue, DecodedField, DecodedInstruction};

pub fn decode_instructions_from_tx(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    program_id: &str,
    idl: &Idl,
    signature: &str,
) -> Result<Vec<DecodedInstruction>> {
    let slot = tx.slot;
    let block_time = tx.block_time;

    let ui_tx = match &tx.transaction.transaction {
        EncodedTransaction::Json(tx) => tx,
        _ => return Err(anyhow!("transaction not in JSON encoding")),
    };

    let message = match &ui_tx.message {
        UiMessage::Raw(msg) => msg,
        _ => return Err(anyhow!("message not in raw encoding")),
    };

    let account_keys = &message.account_keys;
    let signer = account_keys.first().cloned().unwrap_or_default();
    let mut decoded = Vec::new();

    for ix in &message.instructions {
        let ix_program = account_keys
            .get(ix.program_id_index as usize)
            .map(|s| s.as_str())
            .unwrap_or("");

        if ix_program != program_id {
            continue;
        }

        let raw_data = bs58::decode(&ix.data)
            .into_vec()
            .map_err(|e| anyhow!("failed to decode ix data from bs58: {}", e))?;

        if raw_data.len() < 8 {
            continue;
        }

        let disc = &raw_data[..8];

        let idl_ix = match idl.instructions.iter().find(|i| i.effective_discriminator().as_slice() == disc) {
            Some(i) => i,
            None => continue,
        };

        let ix_accounts: Vec<Value> = ix
            .accounts
            .iter()
            .filter_map(|&idx| account_keys.get(idx as usize))
            .map(|k| json!(k))
            .collect();

        let body = &raw_data[8..];
        let mut cursor: &[u8] = body;
        let args = decode_instruction_args(&mut cursor, &idl_ix.args, &idl.types)?;

        decoded.push(DecodedInstruction {
            signature: signature.to_string(),
            slot,
            block_time,
            program_id: program_id.to_string(),
            signer: signer.clone(),
            instruction_name: idl_ix.name.clone(),
            args,
            accounts: Value::Array(ix_accounts),
        });
    }

    Ok(decoded)
}

fn decode_instruction_args(
    cursor: &mut &[u8],
    args: &[IdlField],
    types: &[IdlTypeDef],
) -> Result<Vec<DecodedField>> {
    let mut fields = Vec::new();
    for arg in args {
        let value = decode_value(cursor, &arg.ty, types)?;
        let col_value = json_to_column_value(&value, &arg.ty);
        fields.push(DecodedField {
            name: arg.name.clone(),
            value: col_value,
        });
    }
    Ok(fields)
}

pub fn decode_value(cursor: &mut &[u8], ty: &IdlType, types: &[IdlTypeDef]) -> Result<Value> {
    match ty {
        IdlType::U8 => Ok(json!(read_u8(cursor)?)),
        IdlType::U16 => Ok(json!(read_u16(cursor)?)),
        IdlType::U32 => Ok(json!(read_u32(cursor)?)),
        IdlType::U64 => Ok(json!(read_u64(cursor)?)),
        IdlType::U128 => Ok(Value::String(read_u128(cursor)?.to_string())),
        IdlType::I8 => Ok(json!(read_i8(cursor)?)),
        IdlType::I16 => Ok(json!(read_i16(cursor)?)),
        IdlType::I32 => Ok(json!(read_i32(cursor)?)),
        IdlType::I64 => Ok(json!(read_i64(cursor)?)),
        IdlType::I128 => Ok(Value::String(read_i128(cursor)?.to_string())),
        IdlType::F32 => {
            let b = read_bytes(cursor, 4)?;
            let v = f32::from_le_bytes(b.try_into().unwrap());
            Ok(Value::Number(
                serde_json::Number::from_f64(v as f64).unwrap_or(serde_json::Number::from(0)),
            ))
        }
        IdlType::F64 => {
            let b = read_bytes(cursor, 8)?;
            let v = f64::from_le_bytes(b.try_into().unwrap());
            Ok(Value::Number(
                serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)),
            ))
        }
        IdlType::Bool => Ok(Value::Bool(read_u8(cursor)? != 0)),
        IdlType::String => {
            let len = read_u32(cursor)? as usize;
            let bytes = read_bytes(cursor, len)?;
            Ok(Value::String(
                std::str::from_utf8(&bytes)
                    .map_err(|e| anyhow!("invalid UTF-8 in string field: {}", e))?
                    .to_string(),
            ))
        }
        IdlType::Bytes => {
            let len = read_u32(cursor)? as usize;
            let bytes = read_bytes(cursor, len)?;
            Ok(Value::String(hex::encode(bytes)))
        }
        IdlType::PublicKey => {
            let bytes = read_bytes(cursor, 32)?;
            Ok(Value::String(bs58::encode(bytes).into_string()))
        }
        IdlType::Option(inner) => {
            let tag = read_u8(cursor)?;
            if tag == 0 {
                Ok(Value::Null)
            } else {
                decode_value(cursor, inner, types)
            }
        }
        IdlType::COption(inner) => {
            let tag = read_u32(cursor)?;
            if tag == 0 {
                Ok(Value::Null)
            } else {
                decode_value(cursor, inner, types)
            }
        }
        IdlType::Vec(inner) => {
            let len = read_u32(cursor)? as usize;
            let mut arr = Vec::with_capacity(len);
            for _ in 0..len {
                arr.push(decode_value(cursor, inner, types)?);
            }
            Ok(Value::Array(arr))
        }
        IdlType::Array(inner, size) => {
            let mut arr = Vec::with_capacity(*size);
            for _ in 0..*size {
                arr.push(decode_value(cursor, inner, types)?);
            }
            Ok(Value::Array(arr))
        }
        IdlType::Defined(name) => decode_defined(cursor, name, types),
        IdlType::Unknown => Ok(Value::Null),
    }
}

fn decode_defined(cursor: &mut &[u8], name: &str, types: &[IdlTypeDef]) -> Result<Value> {
    let type_def = types
        .iter()
        .find(|t| t.name == name)
        .ok_or_else(|| anyhow!("undefined IDL type: {}", name))?;

    match &type_def.ty {
        IdlTypeDefKind::Struct { fields } => {
            let mut obj = serde_json::Map::new();
            for field in fields {
                let val = decode_value(cursor, &field.ty, types)?;
                obj.insert(field.name.clone(), val);
            }
            Ok(Value::Object(obj))
        }
        IdlTypeDefKind::Enum { variants } => {
            let variant_idx = read_u8(cursor)? as usize;
            let variant = variants
                .get(variant_idx)
                .ok_or_else(|| anyhow!("enum variant {} out of bounds for type {}", variant_idx, name))?;

            match &variant.fields {
                None => Ok(Value::String(variant.name.clone())),
                Some(crate::idl::IdlEnumFields::Named(fields)) => {
                    let mut inner = serde_json::Map::new();
                    for field in fields {
                        let val = decode_value(cursor, &field.ty, types)?;
                        inner.insert(field.name.clone(), val);
                    }
                    Ok(json!({ variant.name.clone(): inner }))
                }
                Some(crate::idl::IdlEnumFields::Tuple(tys)) => {
                    let mut arr = Vec::new();
                    for ty in tys {
                        arr.push(decode_value(cursor, ty, types)?);
                    }
                    Ok(json!({ variant.name.clone(): arr }))
                }
            }
        }
        IdlTypeDefKind::Unknown => Ok(Value::Null),
    }
}

pub fn json_to_column_value(value: &Value, ty: &IdlType) -> ColumnValue {
    match ty {
        IdlType::U8 | IdlType::U16 | IdlType::U32 | IdlType::I8 | IdlType::I16 | IdlType::I32 => {
            ColumnValue::Int(value.as_i64().unwrap_or(0) as i32)
        }
        IdlType::U64 | IdlType::I64 => ColumnValue::BigInt(value.as_i64().unwrap_or(0)),
        IdlType::U128 | IdlType::I128 => {
            ColumnValue::Text(value.as_str().unwrap_or("0").to_string())
        }
        IdlType::F32 | IdlType::F64 => ColumnValue::Float(value.as_f64().unwrap_or(0.0)),
        IdlType::Bool => ColumnValue::Bool(value.as_bool().unwrap_or(false)),
        IdlType::String | IdlType::PublicKey => {
            ColumnValue::Text(value.as_str().unwrap_or("").to_string())
        }
        IdlType::Bytes => {
            if let Some(s) = value.as_str() {
                ColumnValue::Bytes(hex::decode(s).unwrap_or_default())
            } else {
                ColumnValue::Null
            }
        }
        IdlType::Option(inner) | IdlType::COption(inner) => {
            if value.is_null() {
                ColumnValue::Null
            } else {
                json_to_column_value(value, inner)
            }
        }
        _ => {
            if value.is_null() {
                ColumnValue::Null
            } else {
                ColumnValue::Json(value.clone())
            }
        }
    }
}

fn read_u8(cursor: &mut &[u8]) -> Result<u8> {
    if cursor.is_empty() {
        return Err(anyhow!("unexpected end of data reading u8"));
    }
    let v = cursor[0];
    *cursor = &cursor[1..];
    Ok(v)
}

fn read_u16(cursor: &mut &[u8]) -> Result<u16> {
    let b = read_bytes(cursor, 2)?;
    Ok(u16::from_le_bytes(b.try_into().unwrap()))
}

fn read_u32(cursor: &mut &[u8]) -> Result<u32> {
    let b = read_bytes(cursor, 4)?;
    Ok(u32::from_le_bytes(b.try_into().unwrap()))
}

fn read_u64(cursor: &mut &[u8]) -> Result<u64> {
    let b = read_bytes(cursor, 8)?;
    Ok(u64::from_le_bytes(b.try_into().unwrap()))
}

fn read_u128(cursor: &mut &[u8]) -> Result<u128> {
    let b = read_bytes(cursor, 16)?;
    Ok(u128::from_le_bytes(b.try_into().unwrap()))
}

fn read_i8(cursor: &mut &[u8]) -> Result<i8> {
    Ok(read_u8(cursor)? as i8)
}

fn read_i16(cursor: &mut &[u8]) -> Result<i16> {
    let b = read_bytes(cursor, 2)?;
    Ok(i16::from_le_bytes(b.try_into().unwrap()))
}

fn read_i32(cursor: &mut &[u8]) -> Result<i32> {
    let b = read_bytes(cursor, 4)?;
    Ok(i32::from_le_bytes(b.try_into().unwrap()))
}

fn read_i64(cursor: &mut &[u8]) -> Result<i64> {
    let b = read_bytes(cursor, 8)?;
    Ok(i64::from_le_bytes(b.try_into().unwrap()))
}

fn read_i128(cursor: &mut &[u8]) -> Result<i128> {
    let b = read_bytes(cursor, 16)?;
    Ok(i128::from_le_bytes(b.try_into().unwrap()))
}

fn read_bytes(cursor: &mut &[u8], n: usize) -> Result<Vec<u8>> {
    if cursor.len() < n {
        return Err(anyhow!(
            "unexpected end of data: need {} bytes, have {}",
            n,
            cursor.len()
        ));
    }
    let out = cursor[..n].to_vec();
    *cursor = &cursor[n..];
    Ok(out)
}
