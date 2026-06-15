use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TxStatus {
    Pending,
    Scheduled,
    Broadcast,
    Confirmed,
    Expired,
    Failed,
}

impl TxStatus {
    pub fn as_str(&self) -> &str {
        match self {
            TxStatus::Pending => "pending",
            TxStatus::Scheduled => "scheduled",
            TxStatus::Broadcast => "broadcast",
            TxStatus::Confirmed => "confirmed",
            TxStatus::Expired => "expired",
            TxStatus::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "pending" => TxStatus::Pending,
            "scheduled" => TxStatus::Scheduled,
            "broadcast" => TxStatus::Broadcast,
            "confirmed" => TxStatus::Confirmed,
            "expired" => TxStatus::Expired,
            "failed" => TxStatus::Failed,
            _ => TxStatus::Pending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastTx {
    pub id: String,
    pub tx_hex: String,
    pub txid: Option<String>,
    pub status: TxStatus,
    pub network: String,
    pub nlocktime: Option<u64>,
    pub broadcast_mode: Option<String>,
    pub scheduled_time: Option<DateTime<Utc>>,
    pub broadcast_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub block_height: Option<u64>,
    pub target_fee_rate: Option<f64>,
    pub actual_fee_rate: Option<f64>,
    pub source_label: Option<String>,
    pub destination_address: Option<String>,
    pub utxo_count: i32,
    pub total_value_btc: f64,
    pub replacement_of: Option<String>,
    pub error_message: Option<String>,
    pub retry_count: i32,
    pub broadcast_missed_at: Option<DateTime<Utc>>,
    pub original_scheduled_time: Option<DateTime<Utc>>,
    pub defer_until: Option<DateTime<Utc>>,
    /// `datetime` (default) or `price` — how this TX should be released for broadcast.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_trigger: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_price: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_currency: Option<String>,
    /// `above` or `below` — broadcast when BTC price crosses target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_condition: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Set at API/scheduler layer when scheduled time passed but chain locktime is not yet valid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locktime_waiting: Option<bool>,
    /// Scheduled broadcast missed because chain MTP has not reached nLockTime yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locktime_deferred: Option<bool>,
    /// User may pick a new broadcast attempt time (nLockTime in the TX is unchanged).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_reschedule: Option<bool>,
    /// Chain median time past (unix seconds), when available from indexer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_mtp: Option<u64>,
    /// Required nLockTime target (unix seconds for timestamp locktimes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locktime_target: Option<u64>,
    /// Seconds until chain MTP reaches locktime_target (0 when satisfied).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locktime_remaining_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locktime_satisfied: Option<bool>,
    /// Latest BTC/fiat price from price feed (enriched at API layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_btc_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewBroadcastTx {
    pub tx_hex: String,
    pub network: String,
    pub nlocktime: Option<u64>,
    pub broadcast_mode: Option<String>,
    pub scheduled_time: Option<DateTime<Utc>>,
    pub target_fee_rate: Option<f64>,
    pub source_label: Option<String>,
    pub destination_address: Option<String>,
    pub utxo_count: Option<i32>,
    pub total_value_btc: Option<f64>,
    pub replacement_of: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlanStatus {
    Draft,
    Active,
    Paused,
    Completed,
}

impl PlanStatus {
    pub fn as_str(&self) -> &str {
        match self {
            PlanStatus::Draft => "draft",
            PlanStatus::Active => "active",
            PlanStatus::Paused => "paused",
            PlanStatus::Completed => "completed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "draft" => PlanStatus::Draft,
            "active" => PlanStatus::Active,
            "paused" => PlanStatus::Paused,
            "completed" => PlanStatus::Completed,
            _ => PlanStatus::Draft,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub id: String,
    pub name: String,
    pub source_wallet: Option<String>,
    pub destination_wallet: Option<String>,
    pub network: String,
    pub status: PlanStatus,
    pub min_delay_hours: i32,
    pub max_delay_hours: i32,
    pub min_fee_rate: f64,
    pub max_fee_rate: f64,
    pub total_transactions: i32,
    pub completed_transactions: i32,
    pub total_value_migrated_btc: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewMigrationPlan {
    pub name: String,
    pub source_wallet: Option<String>,
    pub destination_wallet: Option<String>,
    pub network: String,
    pub min_delay_hours: Option<i32>,
    pub max_delay_hours: Option<i32>,
    pub min_fee_rate: Option<f64>,
    pub max_fee_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationUtxo {
    pub id: String,
    pub plan_id: String,
    pub txid: String,
    pub vout: i32,
    pub value_btc: f64,
    pub address: Option<String>,
    pub label: Option<String>,
    pub source_label: Option<String>,
    pub broadcast_pool_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewMigrationUtxo {
    pub txid: String,
    pub vout: i32,
    pub value_btc: f64,
    pub address: Option<String>,
    pub label: Option<String>,
    pub source_label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStats {
    pub total_transactions: usize,
    pub pending: usize,
    pub scheduled: usize,
    pub broadcast: usize,
    pub confirmed: usize,
    pub failed: usize,
    pub total_value_btc: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolStatus {
    pub available: bool,
    pub mempool_tx_count: Option<usize>,
    pub fee_rate_sat_vb: Option<f64>,
    /// `low`, `medium`, or `high` when fee data is available.
    pub congestion: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub connected: bool,
    pub blockchain_height: Option<u64>,
    pub mempool_size: Option<usize>,
    pub network: String,
    pub sync_percentage: Option<f64>,
}
