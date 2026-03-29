use anyhow::Result;
use sqlx::PgPool;

use crate::idl::{schema_gen, Idl};

pub async fn apply_schema_for_idl(pool: &PgPool, idl: &Idl) -> Result<()> {
    let statements = schema_gen::generate_schema_for_program(idl);

    for stmt in &statements {
        sqlx::query(stmt)
            .execute(pool)
            .await
            .map_err(|e| anyhow::anyhow!("schema statement failed: {}\nSQL: {}", e, stmt))?;
    }

    tracing::info!(
        program = %idl.address,
        name = %idl.metadata.name,
        tables = statements.len(),
        "schema applied"
    );

    Ok(())
}
