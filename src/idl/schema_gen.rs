use super::{Idl, IdlField, IdlType, IdlTypeDef, IdlTypeDefKind};

pub fn idl_type_to_sql(ty: &IdlType) -> &'static str {
    match ty {
        IdlType::U8 | IdlType::U16 | IdlType::U32 | IdlType::I8 | IdlType::I16 | IdlType::I32 => {
            "INTEGER"
        }
        IdlType::U64 | IdlType::I64 => "BIGINT",
        IdlType::U128 | IdlType::I128 => "TEXT",
        IdlType::F32 | IdlType::F64 => "DOUBLE PRECISION",
        IdlType::Bool => "BOOLEAN",
        IdlType::String => "TEXT",
        IdlType::PublicKey => "TEXT",
        IdlType::Bytes => "BYTEA",
        IdlType::Option(inner) | IdlType::COption(inner) => idl_type_to_sql(inner),
        _ => "JSONB",
    }
}

pub fn is_nullable(ty: &IdlType) -> bool {
    matches!(ty, IdlType::Option(_) | IdlType::COption(_))
}

pub fn generate_schema_for_program(idl: &Idl) -> Vec<String> {
    let label = program_label(&idl.address);
    let mut statements = Vec::new();

    for ix in &idl.instructions {
        let ddl = generate_instruction_table(&label, &ix.name, &ix.args);
        statements.push(ddl);
    }

    for acct in &idl.accounts {
        if let Some(type_def) = idl.types.iter().find(|t| t.name == acct.name) {
            let ddl = generate_account_table(&label, &acct.name, type_def);
            statements.push(ddl);
        } else {
            let ddl = generate_empty_account_table(&label, &acct.name);
            statements.push(ddl);
        }
    }

    statements
}

fn generate_instruction_table(program_label: &str, ix_name: &str, args: &[IdlField]) -> String {
    let table = format!("ix_{}_{}", program_label, sanitize_name(ix_name));

    let mut col_defs = vec![
        "id BIGSERIAL PRIMARY KEY".to_string(),
        "signature TEXT NOT NULL".to_string(),
        "slot BIGINT NOT NULL".to_string(),
        "block_time BIGINT".to_string(),
        "signer TEXT NOT NULL".to_string(),
    ];

    for field in args {
        let col = safe_col_name(&field.name);
        let sql_type = idl_type_to_sql(&field.ty);
        col_defs.push(format!("{} {}", col, sql_type));
    }

    col_defs.push("accounts JSONB NOT NULL DEFAULT '[]'".to_string());
    col_defs.push("created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()".to_string());

    let cols_sql = col_defs.join(",\n    ");

    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n    {cols}\n);\n\
         CREATE INDEX IF NOT EXISTS idx_{table}_slot ON {table}(slot);\n\
         CREATE INDEX IF NOT EXISTS idx_{table}_signer ON {table}(signer);\n\
         CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_sig ON {table}(signature);",
        table = table,
        cols = cols_sql,
    )
}

fn generate_account_table(program_label: &str, account_name: &str, type_def: &IdlTypeDef) -> String {
    let table = format!("acct_{}_{}", program_label, sanitize_name(account_name));

    let mut col_defs = vec![
        "id BIGSERIAL PRIMARY KEY".to_string(),
        "address TEXT NOT NULL".to_string(),
        "slot_updated BIGINT NOT NULL".to_string(),
    ];

    if let IdlTypeDefKind::Struct { fields } = &type_def.ty {
        for field in fields {
            let col = safe_col_name(&field.name);
            let sql_type = idl_type_to_sql(&field.ty);
            col_defs.push(format!("{} {}", col, sql_type));
        }
    }

    col_defs.push("raw JSONB NOT NULL DEFAULT '{}'".to_string());
    col_defs.push("updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()".to_string());

    let cols_sql = col_defs.join(",\n    ");

    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n    {cols}\n);\n\
         CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_addr ON {table}(address);\n\
         CREATE INDEX IF NOT EXISTS idx_{table}_slot ON {table}(slot_updated);",
        table = table,
        cols = cols_sql,
    )
}

fn generate_empty_account_table(program_label: &str, account_name: &str) -> String {
    let table = format!("acct_{}_{}", program_label, sanitize_name(account_name));

    let col_defs = vec![
        "id BIGSERIAL PRIMARY KEY".to_string(),
        "address TEXT NOT NULL".to_string(),
        "slot_updated BIGINT NOT NULL".to_string(),
        "raw JSONB NOT NULL DEFAULT '{}'".to_string(),
        "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()".to_string(),
    ];

    let cols_sql = col_defs.join(",\n    ");

    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n    {cols}\n);\n\
         CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_addr ON {table}(address);\n\
         CREATE INDEX IF NOT EXISTS idx_{table}_slot ON {table}(slot_updated);",
        table = table,
        cols = cols_sql,
    )
}

const RESERVED_COLS: &[&str] = &[
    "id", "signature", "slot", "block_time", "signer", "accounts", "created_at",
    "address", "slot_updated", "raw", "updated_at",
];

pub fn safe_col_name(name: &str) -> String {
    let sanitized = sanitize_name(name);
    if RESERVED_COLS.contains(&sanitized.as_str()) {
        format!("arg_{}", sanitized)
    } else {
        sanitized
    }
}

pub fn sanitize_name(name: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &ch) in chars.iter().enumerate() {
        if ch.is_uppercase() && i > 0 && !chars[i - 1].is_uppercase() {
            result.push('_');
            result.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == ' ' {
            result.push('_');
        } else if ch.is_alphanumeric() || ch == '_' {
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push('_');
        }
    }

    if result.starts_with(|c: char| c.is_numeric()) {
        result.insert(0, '_');
    }

    result
}

pub fn program_label(program_id: &str) -> String {
    let alnum: String = program_id
        .chars()
        .filter(|c| c.is_alphanumeric())
        .collect();
    let start = alnum.len().saturating_sub(10);
    alnum[start..].to_lowercase()
}

pub fn instruction_table_name(program_id: &str, ix_name: &str) -> String {
    let label = program_label(program_id);
    format!("ix_{}_{}", label, sanitize_name(ix_name))
}

pub fn account_table_name(program_id: &str, account_name: &str) -> String {
    let label = program_label(program_id);
    format!("acct_{}_{}", label, sanitize_name(account_name))
}
