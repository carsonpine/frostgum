pub mod account;
pub mod instruction;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct DecodedInstruction {
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub program_id: String,
    pub signer: String,
    pub instruction_name: String,
    pub args: Vec<DecodedField>,
    pub accounts: Value,
}

#[derive(Debug, Clone)]
pub struct DecodedAccount {
    pub address: String,
    pub slot_updated: u64,
    pub account_name: String,
    pub fields: Vec<DecodedField>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedField {
    pub name: String,
    pub value: ColumnValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnValue {
    Int(i32),
    BigInt(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
    Json(Value),
    Null,
}

impl ColumnValue {
    pub fn sql_cast_suffix(&self) -> &'static str {
        match self {
            ColumnValue::Int(_) => "::integer",
            ColumnValue::BigInt(_) => "::bigint",
            ColumnValue::Float(_) => "::double precision",
            ColumnValue::Bool(_) => "::boolean",
            ColumnValue::Text(_) => "",
            ColumnValue::Bytes(_) => "",
            ColumnValue::Json(_) => "::jsonb",
            ColumnValue::Null => "",
        }
    }

    pub fn add_to_args(
        &self,
        args: &mut sqlx::postgres::PgArguments,
    ) -> anyhow::Result<()> {
        use sqlx::Arguments;
        match self {
            ColumnValue::Int(v) => args.add(*v)?,
            ColumnValue::BigInt(v) => args.add(*v)?,
            ColumnValue::Float(v) => args.add(*v)?,
            ColumnValue::Bool(v) => args.add(*v)?,
            ColumnValue::Text(v) => args.add(v.clone())?,
            ColumnValue::Bytes(v) => args.add(v.clone())?,
            ColumnValue::Json(v) => args.add(sqlx::types::Json(v.clone()))?,
            ColumnValue::Null => args.add(None::<String>)?,
        }
        Ok(())
    }

    pub fn from_json(v: &Value) -> Self {
        match v {
            Value::Bool(b) => ColumnValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        ColumnValue::Int(i as i32)
                    } else {
                        ColumnValue::BigInt(i)
                    }
                } else if let Some(f) = n.as_f64() {
                    ColumnValue::Float(f)
                } else {
                    ColumnValue::Text(n.to_string())
                }
            }
            Value::String(s) => ColumnValue::Text(s.clone()),
            Value::Null => ColumnValue::Null,
            other => ColumnValue::Json(other.clone()),
        }
    }
}
