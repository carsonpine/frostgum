use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct InstructionQuery {
    pub signer: Option<String>,
    pub start_slot: Option<i64>,
    pub end_slot: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub order: Option<String>,
}

impl InstructionQuery {
    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(50).min(1000).max(1)
    }

    pub fn offset(&self) -> i64 {
        self.offset.unwrap_or(0).max(0)
    }

    pub fn order_clause(&self) -> &'static str {
        match self.order.as_deref() {
            Some("asc") => "slot ASC",
            Some("created_asc") => "created_at ASC",
            _ => "slot DESC",
        }
    }

    pub fn conditions(&self) -> Vec<(&'static str, String)> {
        let mut conds = Vec::new();
        if let Some(ref s) = self.signer {
            conds.push(("signer", s.clone()));
        }
        conds
    }
}

#[derive(Debug, Deserialize)]
pub struct AccountListQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl AccountListQuery {
    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(50).min(1000).max(1)
    }

    pub fn offset(&self) -> i64 {
        self.offset.unwrap_or(0).max(0)
    }
}

#[derive(Debug, Deserialize)]
pub struct RawSqlBody {
    pub sql: String,
}
