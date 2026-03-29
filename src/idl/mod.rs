pub mod loader;
pub mod schema_gen;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize)]
pub struct Idl {
    pub address: String,
    pub metadata: IdlMetadata,
    pub instructions: Vec<IdlInstruction>,
    pub accounts: Vec<IdlAccountType>,
    pub types: Vec<IdlTypeDef>,
    pub events: Vec<IdlEvent>,
}

impl<'de> Deserialize<'de> for Idl {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(deserializer)?;

        let address = v.get("address")
            .and_then(|a| a.as_str())
            .unwrap_or("")
            .to_string();

        let metadata = if let Some(m) = v.get("metadata") {
            serde_json::from_value(m.clone()).map_err(serde::de::Error::custom)?
        } else {
            IdlMetadata {
                name: v.get("name").and_then(|n| n.as_str()).unwrap_or("unknown").to_string(),
                version: v.get("version").and_then(|n| n.as_str()).unwrap_or("").to_string(),
                spec: None,
                description: None,
            }
        };

        let instructions = v.get("instructions")
            .map(|x| serde_json::from_value(x.clone()).unwrap_or_default())
            .unwrap_or_default();

        let accounts = v.get("accounts")
            .map(|x| serde_json::from_value(x.clone()).unwrap_or_default())
            .unwrap_or_default();

        let types = v.get("types")
            .map(|x| serde_json::from_value(x.clone()).unwrap_or_default())
            .unwrap_or_default();

        let events = v.get("events")
            .map(|x| serde_json::from_value(x.clone()).unwrap_or_default())
            .unwrap_or_default();

        Ok(Idl { address, metadata, instructions, accounts, types, events })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlMetadata {
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub spec: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlInstruction {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
    #[serde(default)]
    pub accounts: Vec<IdlAccountItem>,
    #[serde(default)]
    pub args: Vec<IdlField>,
    #[serde(default)]
    pub returns: Option<IdlType>,
    #[serde(default)]
    pub docs: Vec<String>,
}

impl IdlInstruction {
    pub fn effective_discriminator(&self) -> Vec<u8> {
        if !self.discriminator.is_empty() {
            return self.discriminator.clone();
        }
        let preimage = format!("global:{}", self.name);
        let mut hasher = Sha256::new();
        hasher.update(preimage.as_bytes());
        hasher.finalize()[..8].to_vec()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlField {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: IdlType,
    #[serde(default)]
    pub docs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IdlAccountItem {
    Nested(IdlNestedAccounts),
    Single(IdlInstructionAccount),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlNestedAccounts {
    pub name: String,
    pub accounts: Vec<IdlAccountItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlInstructionAccount {
    pub name: String,
    #[serde(default)]
    pub writable: bool,
    #[serde(default, alias = "isMut")]
    pub is_mut: bool,
    #[serde(default)]
    pub signer: bool,
    #[serde(default, alias = "isSigner")]
    pub is_signer: bool,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub docs: Vec<String>,
}

impl IdlInstructionAccount {
    pub fn is_writable(&self) -> bool {
        self.writable || self.is_mut
    }

    pub fn is_signer_account(&self) -> bool {
        self.signer || self.is_signer
    }
}

pub fn flatten_account_items(items: &[IdlAccountItem]) -> Vec<&IdlInstructionAccount> {
    let mut result = Vec::new();
    for item in items {
        match item {
            IdlAccountItem::Single(acct) => result.push(acct),
            IdlAccountItem::Nested(group) => {
                result.extend(flatten_account_items(&group.accounts));
            }
        }
    }
    result
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlAccountType {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
}

impl IdlAccountType {
    pub fn effective_discriminator(&self) -> Vec<u8> {
        if !self.discriminator.is_empty() {
            return self.discriminator.clone();
        }
        let preimage = format!("account:{}", self.name);
        let mut hasher = Sha256::new();
        hasher.update(preimage.as_bytes());
        hasher.finalize()[..8].to_vec()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlTypeDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: IdlTypeDefKind,
    #[serde(default)]
    pub docs: Vec<String>,
    #[serde(default)]
    pub serialization: Option<String>,
    #[serde(default)]
    pub repr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum IdlTypeDefKind {
    Struct {
        #[serde(default)]
        fields: Vec<IdlField>,
    },
    Enum {
        variants: Vec<IdlEnumVariant>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlEnumVariant {
    pub name: String,
    #[serde(default)]
    pub fields: Option<IdlEnumFields>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IdlEnumFields {
    Named(Vec<IdlField>),
    Tuple(Vec<IdlType>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlEvent {
    pub name: String,
    #[serde(default)]
    pub discriminator: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub enum IdlType {
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    F32,
    F64,
    Bool,
    String,
    Bytes,
    PublicKey,
    Option(Box<IdlType>),
    COption(Box<IdlType>),
    Vec(Box<IdlType>),
    Array(Box<IdlType>, usize),
    Defined(std::string::String),
    Unknown,
}

impl<'de> Deserialize<'de> for IdlType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Ok(parse_idl_type(&value))
    }
}

pub fn parse_idl_type(value: &Value) -> IdlType {
    match value {
        Value::String(s) => match s.as_str() {
            "u8" => IdlType::U8,
            "u16" => IdlType::U16,
            "u32" => IdlType::U32,
            "u64" => IdlType::U64,
            "u128" => IdlType::U128,
            "i8" => IdlType::I8,
            "i16" => IdlType::I16,
            "i32" => IdlType::I32,
            "i64" => IdlType::I64,
            "i128" => IdlType::I128,
            "f32" => IdlType::F32,
            "f64" => IdlType::F64,
            "bool" => IdlType::Bool,
            "string" | "String" => IdlType::String,
            "bytes" | "Bytes" => IdlType::Bytes,
            "publicKey" | "pubkey" | "Pubkey" | "PublicKey" | "public_key" => IdlType::PublicKey,
            other => IdlType::Defined(other.to_string()),
        },
        Value::Object(map) => {
            if let Some(inner) = map.get("option").or_else(|| map.get("Option")) {
                return IdlType::Option(Box::new(parse_idl_type(inner)));
            }
            if let Some(inner) = map.get("coption").or_else(|| map.get("COption")) {
                return IdlType::COption(Box::new(parse_idl_type(inner)));
            }
            if let Some(inner) = map.get("vec").or_else(|| map.get("Vec")) {
                return IdlType::Vec(Box::new(parse_idl_type(inner)));
            }
            if let Some(arr) = map.get("array").or_else(|| map.get("Array")) {
                if let Some(arr) = arr.as_array() {
                    if arr.len() == 2 {
                        let inner_type = parse_idl_type(&arr[0]);
                        let size = arr[1].as_u64().unwrap_or(0) as usize;
                        return IdlType::Array(Box::new(inner_type), size);
                    }
                }
                return IdlType::Unknown;
            }
            if let Some(defined) = map.get("defined") {
                return match defined {
                    Value::String(name) => IdlType::Defined(name.clone()),
                    Value::Object(obj) => {
                        if let Some(name) = obj.get("name").and_then(|n| n.as_str()) {
                            IdlType::Defined(name.to_string())
                        } else {
                            IdlType::Unknown
                        }
                    }
                    _ => IdlType::Unknown,
                };
            }
            IdlType::Unknown
        }
        _ => IdlType::Unknown,
    }
}
