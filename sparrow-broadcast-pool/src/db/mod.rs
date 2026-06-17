pub mod models;
pub mod schema;

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;
use uuid::Uuid;

use self::models::*;

const BROADCAST_SELECT: &str = "id, tx_hex, txid, status, network, nlocktime, broadcast_mode, scheduled_time, broadcast_at, confirmed_at, block_height, target_fee_rate, actual_fee_rate, source_label, destination_address, utxo_count, total_value_btc, replacement_of, error_message, retry_count, broadcast_missed_at, original_scheduled_time, defer_until, schedule_trigger, target_price, price_currency, price_condition, created_at, updated_at";

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    fn lock_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|e| {
            // #region agent log
            crate::utils::debug_log::agent_log(
                "H1",
                "db/mod.rs:lock_conn",
                "database mutex poisoned",
                serde_json::json!({ "error": e.to_string() }),
            );
            // #endregion
            anyhow::anyhow!("Database lock poisoned: {}", e)
        })
    }

    pub fn open(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open database at {}", db_path.display()))?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .context("Failed to set pragmas")?;

        let db = Self {
            conn: Mutex::new(conn),
        };
        db.run_migrations()?;
        Ok(db)
    }

    fn run_migrations(&self) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute_batch(schema::MIGRATION_001)
            .context("Failed to run migrations")?;

        // Migration 002: add nlocktime column if it doesn't exist
        let add_col_result: Result<(), _> = conn.execute_batch(schema::MIGRATION_002).context("Migration 002");
        if let Err(e) = add_col_result {
            let err_str = e.to_string();
            if !err_str.contains("duplicate column") && !err_str.contains("already exists") {
                tracing::warn!("Migration 002 warning (non-fatal): {}", e);
            } else {
                tracing::debug!("nlocktime column already exists, skipping migration 002");
            }
        }

        // Migration 003: create nlocktime index (idempotent)
        let add_idx_result: Result<(), _> = conn.execute_batch(schema::MIGRATION_003).context("Migration 003");
        if let Err(e) = add_idx_result {
            let err_str = e.to_string();
            if !err_str.contains("already exists") {
                tracing::warn!("Migration 003 warning (non-fatal): {}", e);
            }
        }

        // Migration 004: add broadcast_mode column (older DBs created before this field)
        let add_mode_result: Result<(), _> =
            conn.execute_batch(schema::MIGRATION_004).context("Migration 004");
        if let Err(e) = add_mode_result {
            let err_str = e.to_string();
            if !err_str.contains("duplicate column") && !err_str.contains("already exists") {
                tracing::warn!("Migration 004 warning (non-fatal): {}", e);
            } else {
                tracing::debug!("broadcast_mode column already exists, skipping migration 004");
            }
        }

        // Migration 005: deferred broadcast tracking columns
        let add_defer_result: Result<(), _> =
            conn.execute_batch(schema::MIGRATION_005).context("Migration 005");
        if let Err(e) = add_defer_result {
            let err_str = e.to_string();
            if !err_str.contains("duplicate column") && !err_str.contains("already exists") {
                tracing::warn!("Migration 005 warning (non-fatal): {}", e);
            } else {
                tracing::debug!("defer columns already exist, skipping migration 005");
            }
        }

        // Migration 006: fiat price trigger columns
        let add_price_result: Result<(), _> =
            conn.execute_batch(schema::MIGRATION_006).context("Migration 006");
        if let Err(e) = add_price_result {
            let err_str = e.to_string();
            if !err_str.contains("duplicate column") && !err_str.contains("already exists") {
                tracing::warn!("Migration 006 warning (non-fatal): {}", e);
            } else {
                tracing::debug!("price trigger columns already exist, skipping migration 006");
            }
        }

        Ok(())
    }

    // ── Broadcast Pool ──────────────────────────────────────────────

    pub fn insert_broadcast_tx(&self, tx: &NewBroadcastTx) -> Result<BroadcastTx> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let status = TxStatus::Pending.as_str();
        let scheduled = tx.scheduled_time.map(|t| t.to_rfc3339());
        let nlocktime = tx.nlocktime.unwrap_or(0);
        let broadcast_mode = tx.broadcast_mode.clone().unwrap_or_else(|| "immediate".to_string());

        {
            let conn = self.lock_conn()?;
            conn.execute(
                "INSERT INTO broadcast_pool (id, tx_hex, status, network, nlocktime, broadcast_mode, scheduled_time, target_fee_rate, source_label, destination_address, utxo_count, total_value_btc, replacement_of, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    id,
                    tx.tx_hex,
                    status,
                    tx.network,
                    nlocktime,
                    broadcast_mode,
                    scheduled,
                    tx.target_fee_rate,
                    tx.source_label,
                    tx.destination_address,
                    tx.utxo_count.unwrap_or(1),
                    tx.total_value_btc.unwrap_or(0.0),
                    tx.replacement_of,
                    now,
                    now,
                ],
            )
            .context("Failed to insert broadcast tx")?;
        }

        self.get_broadcast_tx_by_id(&id)
    }

    pub fn get_broadcast_tx_by_id(&self, id: &str) -> Result<BroadcastTx> {
        let conn = self.lock_conn()?;
        conn.query_row(
            &format!("SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE id = ?1"),
            params![id],
            |row| map_broadcast_row(row),
        )
        .context("Failed to get broadcast tx")
    }

    pub fn list_broadcast_txs(&self, status_filter: Option<&str>, network: &str, limit: i32) -> Result<Vec<BroadcastTx>> {
        let conn = self.lock_conn()?;

        let map_row = map_broadcast_row;

        let mut txs = Vec::new();

        if let Some(status) = status_filter {
            let sql = format!("SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE network = ?1 AND status = ?2 ORDER BY created_at DESC LIMIT ?3");
            let mut stmt = conn.prepare(&sql).context("Failed to prepare list query")?;
            let rows = stmt.query_map(rusqlite::params![network, status, limit], map_row)
                .map_err(|e| anyhow::anyhow!("Failed to query broadcast txs: {}", e))?;
            for row in rows {
                txs.push(row.context("Failed to read row")?);
            }
        } else {
            let sql = format!("SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE network = ?1 ORDER BY created_at DESC LIMIT ?2");
            let mut stmt = conn.prepare(&sql).context("Failed to prepare list query")?;
            let rows = stmt.query_map(rusqlite::params![network, limit], map_row)
                .map_err(|e| anyhow::anyhow!("Failed to query broadcast txs: {}", e))?;
            for row in rows {
                txs.push(row.context("Failed to read row")?);
            }
        }

        Ok(txs)
    }

    pub fn update_tx_status(&self, id: &str, status: TxStatus, error: Option<&str>) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = ?1, error_message = ?2, updated_at = ?3 WHERE id = ?4",
            params![status.as_str(), error, now, id],
        )
        .context("Failed to update tx status")?;
        Ok(())
    }

    /// Move a failed tx back to scheduled so the scheduler can retry broadcast.
    pub fn reset_failed_to_scheduled(&self, id: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        let updated = conn
            .execute(
                "UPDATE broadcast_pool SET status = 'scheduled', error_message = NULL, updated_at = ?1 WHERE id = ?2 AND status = 'failed'",
                params![now, id],
            )
            .context("Failed to reset failed tx to scheduled")?;
        if updated == 0 {
            anyhow::bail!("Transaction {} is not in failed state", id);
        }
        Ok(())
    }

    pub fn mark_broadcast(&self, id: &str, txid: &str, fee_rate: f64) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = 'broadcast', txid = ?1, actual_fee_rate = ?2, broadcast_at = ?3, updated_at = ?3, broadcast_missed_at = NULL, defer_until = NULL WHERE id = ?4",
            params![txid, fee_rate, now, id],
        )
        .context("Failed to mark as broadcast")?;
        Ok(())
    }

    pub fn get_tx_hex_by_txid(&self, txid: &str) -> Result<Option<String>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare("SELECT tx_hex FROM broadcast_pool WHERE txid = ?1 AND status IN ('pending', 'scheduled') LIMIT 1")
            .context("Failed to prepare get_tx_hex query")?;
        let mut rows = stmt.query_map(params![txid], |row| row.get::<_, String>(0))
            .context("Failed to query tx_hex by txid")?;
        match rows.next() {
            Some(Ok(hex)) => Ok(Some(hex)),
            _ => Ok(None),
        }
    }

    pub fn mark_confirmed(&self, id: &str, block_height: u64) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = 'confirmed', block_height = ?1, confirmed_at = ?2, updated_at = ?2 WHERE id = ?3",
            params![block_height, now, id],
        )
        .context("Failed to mark as confirmed")?;
        Ok(())
    }

    pub fn get_due_transactions(&self, network: &str) -> Result<Vec<BroadcastTx>> {
        let conn = self.lock_conn()?;
        let sql = format!(
            "SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE status = 'scheduled' AND network = ?1"
        );
        let mut stmt = conn
            .prepare(&sql)
            .context("Failed to prepare due query")?;

        let rows = stmt
            .query_map(params![network], map_broadcast_row)
            .context("Failed to query due txs")?;

        let mut txs = Vec::new();
        for row in rows {
            txs.push(row.context("Failed to read row")?);
        }
        Ok(txs)
    }

    pub fn record_broadcast_miss(
        &self,
        id: &str,
        missed_at: &str,
        original_scheduled: Option<&str>,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        if let Some(orig) = original_scheduled {
            conn.execute(
                "UPDATE broadcast_pool SET broadcast_missed_at = ?1, original_scheduled_time = COALESCE(original_scheduled_time, ?2), updated_at = ?3 WHERE id = ?4 AND broadcast_missed_at IS NULL",
                params![missed_at, orig, now, id],
            )
            .context("Failed to record broadcast miss")?;
        } else {
            conn.execute(
                "UPDATE broadcast_pool SET broadcast_missed_at = ?1, updated_at = ?2 WHERE id = ?3 AND broadcast_missed_at IS NULL",
                params![missed_at, now, id],
            )
            .context("Failed to record broadcast miss")?;
        }
        Ok(())
    }

    pub fn update_reschedule(
        &self,
        id: &str,
        scheduled_time: &str,
        defer_until: Option<&str>,
        fee_rate: f64,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = 'scheduled', scheduled_time = ?1, defer_until = ?2, target_fee_rate = ?3, error_message = NULL, schedule_trigger = 'datetime', target_price = NULL, price_currency = NULL, price_condition = NULL, updated_at = ?4 WHERE id = ?5",
            params![scheduled_time, defer_until, fee_rate, now, id],
        )
        .context("Failed to update reschedule")?;
        Ok(())
    }

    pub fn update_price_schedule(
        &self,
        id: &str,
        target_price: f64,
        price_currency: &str,
        price_condition: &str,
        fee_rate: f64,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = 'pending', scheduled_time = NULL, defer_until = NULL, target_fee_rate = ?1, error_message = NULL, schedule_trigger = 'price', target_price = ?2, price_currency = ?3, price_condition = ?4, updated_at = ?5 WHERE id = ?6",
            params![fee_rate, target_price, price_currency, price_condition, now, id],
        )
        .context("Failed to update price schedule")?;
        Ok(())
    }

    pub fn get_price_triggered_pending(&self, network: &str) -> Result<Vec<BroadcastTx>> {
        let conn = self.lock_conn()?;
        let sql = format!(
            "SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE status IN ('pending', 'scheduled') AND schedule_trigger = 'price' AND target_price IS NOT NULL AND network = ?1"
        );
        let mut stmt = conn
            .prepare(&sql)
            .context("Failed to prepare price-triggered query")?;

        let rows = stmt
            .query_map(params![network], map_broadcast_row)
            .context("Failed to query price-triggered txs")?;

        let mut txs = Vec::new();
        for row in rows {
            txs.push(row.context("Failed to read row")?);
        }
        Ok(txs)
    }

    pub fn get_pending_by_block_height(&self, network: &str) -> Result<Vec<BroadcastTx>> {
        let conn = self.lock_conn()?;
        let sql = format!(
            "SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE status = 'pending' AND broadcast_mode = 'by_block' AND nlocktime > 0 AND nlocktime < 500000000 AND network = ?1"
        );
        let mut stmt = conn
            .prepare(&sql)
            .context("Failed to prepare pending by block height query")?;

        let rows = stmt
            .query_map(params![network], map_broadcast_row)
            .context("Failed to query pending by block height txs")?;

        let mut txs = Vec::new();
        for row in rows {
            txs.push(row.context("Failed to read row")?);
        }
        Ok(txs)
    }

    pub fn get_pending_by_scheduled_time(&self, network: &str) -> Result<Vec<BroadcastTx>> {
        let conn = self.lock_conn()?;
        let now = Utc::now();
        let sql = format!(
            "SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE status = 'pending' AND broadcast_mode IN ('scheduled', 'manual') AND scheduled_time IS NOT NULL AND network = ?1"
        );
        let mut stmt = conn
            .prepare(&sql)
            .context("Failed to prepare pending by scheduled time query")?;

        let rows = stmt
            .query_map(params![network], map_broadcast_row)
            .context("Failed to query pending by scheduled time txs")?;

        let mut txs = Vec::new();
        for row in rows {
            let tx = row.context("Failed to read row")?;
            if tx
                .scheduled_time
                .as_ref()
                .is_some_and(|t| *t <= now)
            {
                txs.push(tx);
            }
        }
        Ok(txs)
    }

    pub fn get_pending_rebroadcast(&self, interval_minutes: i32, network: &str) -> Result<Vec<BroadcastTx>> {
        let conn = self.lock_conn()?;
        let cutoff = Utc::now()
            .checked_sub_signed(chrono::Duration::minutes(interval_minutes as i64))
            .ok_or_else(|| anyhow::anyhow!("Invalid rebroadcast interval"))?
            .to_rfc3339();

        let sql = format!(
            "SELECT {BROADCAST_SELECT} FROM broadcast_pool WHERE status = 'broadcast' AND confirmed_at IS NULL AND (broadcast_at IS NULL OR broadcast_at < ?1) AND network = ?2"
        );
        let mut stmt = conn
            .prepare(&sql)
            .context("Failed to prepare rebroadcast query")?;

        let rows = stmt
            .query_map(params![cutoff, network], map_broadcast_row)
            .context("Failed to query rebroadcast txs")?;

        let mut txs = Vec::new();
        for row in rows {
            txs.push(row.context("Failed to read row")?);
        }
        Ok(txs)
    }

    pub fn mark_due(&self, id: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = 'scheduled', scheduled_time = ?1, updated_at = ?1 WHERE id = ?2",
            params![now, id],
        )
        .context("Failed to mark tx as due")?;
        Ok(())
    }

    /// Price trigger fired: ready for broadcast loop (clears price-only waiting state).
    pub fn mark_due_from_price_trigger(&self, id: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = 'scheduled', scheduled_time = ?1, schedule_trigger = 'datetime', updated_at = ?2 WHERE id = ?3",
            params![now, now, id],
        )
        .context("Failed to mark price-triggered tx as due")?;
        Ok(())
    }

    pub fn mark_due_with_schedule(&self, id: &str, scheduled_time: &DateTime<Utc>) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        let scheduled = scheduled_time.to_rfc3339();
        conn.execute(
            "UPDATE broadcast_pool SET status = 'scheduled', scheduled_time = ?1, updated_at = ?2 WHERE id = ?3",
            params![scheduled, now, id],
        )
        .context("Failed to mark tx as due with schedule")?;
        Ok(())
    }

    pub fn remove_broadcast_tx(&self, id: &str) -> Result<usize> {
        let conn = self.lock_conn()?;
        let n = conn
            .execute("DELETE FROM broadcast_pool WHERE id = ?1", params![id])
            .context("Failed to remove broadcast tx")?;
        Ok(n)
    }

    pub fn get_pool_stats(&self, network: &str) -> Result<PoolStats> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT status, COUNT(*), COALESCE(SUM(total_value_btc), 0.0)
                 FROM broadcast_pool WHERE network = ?1 GROUP BY status",
            )
            .context("Failed to prepare stats query")?;

        let mut stats = PoolStats {
            total_transactions: 0,
            pending: 0,
            scheduled: 0,
            broadcast: 0,
            confirmed: 0,
            failed: 0,
            total_value_btc: 0.0,
        };

        let rows = stmt
            .query_map(params![network], |row| {
                let status: String = row.get(0)?;
                let count: i32 = row.get(1)?;
                let value: f64 = row.get(2)?;
                Ok((status, count, value))
            })
            .context("Failed to query stats")?;

        for row in rows {
            let (status, count, value) = row?;
            stats.total_transactions += count as usize;
            stats.total_value_btc += value;
            match status.as_str() {
                "pending" => stats.pending = count as usize,
                "scheduled" => stats.scheduled = count as usize,
                "broadcast" => stats.broadcast = count as usize,
                "confirmed" => stats.confirmed = count as usize,
                "failed" => stats.failed = count as usize,
                _ => {}
            }
        }

        Ok(stats)
    }

    // ── Migration Plans ─────────────────────────────────────────────

    pub fn insert_migration_plan(&self, plan: &NewMigrationPlan) -> Result<MigrationPlan> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let status = PlanStatus::Draft.as_str();

        {
            let conn = self.lock_conn()?;
            conn.execute(
                "INSERT INTO migration_plans (id, name, source_wallet, destination_wallet, network, status, min_delay_hours, max_delay_hours, min_fee_rate, max_fee_rate, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    id,
                    plan.name,
                    plan.source_wallet,
                    plan.destination_wallet,
                    plan.network,
                    status,
                    plan.min_delay_hours.unwrap_or(2),
                    plan.max_delay_hours.unwrap_or(72),
                    plan.min_fee_rate.unwrap_or(1.0),
                    plan.max_fee_rate.unwrap_or(50.0),
                    now,
                    now,
                ],
            )
            .context("Failed to insert migration plan")?;
        }

        self.get_migration_plan_by_id(&id)
    }

    pub fn get_migration_plan_by_id(&self, id: &str) -> Result<MigrationPlan> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT id, name, source_wallet, destination_wallet, network, status, min_delay_hours, max_delay_hours, min_fee_rate, max_fee_rate, total_transactions, completed_transactions, total_value_migrated_btc, created_at, updated_at
             FROM migration_plans WHERE id = ?1",
            params![id],
            |row| {
                Ok(MigrationPlan {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    source_wallet: row.get(2)?,
                    destination_wallet: row.get(3)?,
                    network: row.get(4)?,
                    status: PlanStatus::from_str(&row.get::<_, String>(5)?),
                    min_delay_hours: row.get(6)?,
                    max_delay_hours: row.get(7)?,
                    min_fee_rate: row.get(8)?,
                    max_fee_rate: row.get(9)?,
                    total_transactions: row.get(10)?,
                    completed_transactions: row.get(11)?,
                    total_value_migrated_btc: row.get(12)?,
                    created_at: parse_datetime(&row.get::<_, String>(13)?),
                    updated_at: parse_datetime(&row.get::<_, String>(14)?),
                })
            },
        )
        .context("Failed to get migration plan")
    }

    pub fn list_migration_plans(&self, network: &str) -> Result<Vec<MigrationPlan>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, name, source_wallet, destination_wallet, network, status, min_delay_hours, max_delay_hours, min_fee_rate, max_fee_rate, total_transactions, completed_transactions, total_value_migrated_btc, created_at, updated_at
                 FROM migration_plans WHERE network = ?1 ORDER BY created_at DESC",
            )
            .context("Failed to prepare list plans query")?;

        let rows = stmt
            .query_map(params![network], |row| {
                Ok(MigrationPlan {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    source_wallet: row.get(2)?,
                    destination_wallet: row.get(3)?,
                    network: row.get(4)?,
                    status: PlanStatus::from_str(&row.get::<_, String>(5)?),
                    min_delay_hours: row.get(6)?,
                    max_delay_hours: row.get(7)?,
                    min_fee_rate: row.get(8)?,
                    max_fee_rate: row.get(9)?,
                    total_transactions: row.get(10)?,
                    completed_transactions: row.get(11)?,
                    total_value_migrated_btc: row.get(12)?,
                    created_at: parse_datetime(&row.get::<_, String>(13)?),
                    updated_at: parse_datetime(&row.get::<_, String>(14)?),
                })
            })
            .context("Failed to query plans")?;

        let mut plans = Vec::new();
        for row in rows {
            plans.push(row.context("Failed to read row")?);
        }
        Ok(plans)
    }

    pub fn update_plan_status(&self, id: &str, status: PlanStatus) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE migration_plans SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status.as_str(), now, id],
        )
        .context("Failed to update plan status")?;
        Ok(())
    }

    // ── Migration UTXOs ─────────────────────────────────────────────

    pub fn insert_migration_utxo(&self, plan_id: &str, utxo: &NewMigrationUtxo) -> Result<MigrationUtxo> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        {
            let conn = self.lock_conn()?;
            conn.execute(
                "INSERT INTO migration_utxos (id, plan_id, txid, vout, value_btc, address, label, source_label, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    id,
                    plan_id,
                    utxo.txid,
                    utxo.vout,
                    utxo.value_btc,
                    utxo.address,
                    utxo.label,
                    utxo.source_label,
                    now,
                ],
            )
            .context("Failed to insert migration utxo")?;
        }

        self.get_migration_utxo_by_id(&id)
    }

    pub fn get_migration_utxo_by_id(&self, id: &str) -> Result<MigrationUtxo> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT id, plan_id, txid, vout, value_btc, address, label, source_label, broadcast_pool_id, created_at
             FROM migration_utxos WHERE id = ?1",
            params![id],
            |row| {
                Ok(MigrationUtxo {
                    id: row.get(0)?,
                    plan_id: row.get(1)?,
                    txid: row.get(2)?,
                    vout: row.get(3)?,
                    value_btc: row.get(4)?,
                    address: row.get(5)?,
                    label: row.get(6)?,
                    source_label: row.get(7)?,
                    broadcast_pool_id: row.get(8)?,
                    created_at: parse_datetime(&row.get::<_, String>(9)?),
                })
            },
        )
        .context("Failed to get migration utxo")
    }

    pub fn list_migration_utxos(&self, plan_id: &str) -> Result<Vec<MigrationUtxo>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, plan_id, txid, vout, value_btc, address, label, source_label, broadcast_pool_id, created_at
                 FROM migration_utxos WHERE plan_id = ?1 ORDER BY created_at ASC",
            )
            .context("Failed to prepare list utxos query")?;

        let rows = stmt
            .query_map(params![plan_id], |row| {
                Ok(MigrationUtxo {
                    id: row.get(0)?,
                    plan_id: row.get(1)?,
                    txid: row.get(2)?,
                    vout: row.get(3)?,
                    value_btc: row.get(4)?,
                    address: row.get(5)?,
                    label: row.get(6)?,
                    source_label: row.get(7)?,
                    broadcast_pool_id: row.get(8)?,
                    created_at: parse_datetime(&row.get::<_, String>(9)?),
                })
            })
            .context("Failed to query utxos")?;

        let mut utxos = Vec::new();
        for row in rows {
            utxos.push(row.context("Failed to read row")?);
        }
        Ok(utxos)
    }

    pub fn link_utxo_to_pool(&self, utxo_id: &str, pool_id: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE migration_utxos SET broadcast_pool_id = ?1 WHERE id = ?2",
            params![pool_id, utxo_id],
        )
        .context("Failed to link utxo to pool")?;
        Ok(())
    }

    pub fn update_plan_total(&self, plan_id: &str, total: i32) -> Result<()> {
        let conn = self.lock_conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE migration_plans SET total_transactions = ?1, updated_at = ?2 WHERE id = ?3",
            params![total, now, plan_id],
        )
        .context("Failed to update plan total")?;
        Ok(())
    }

    pub fn execute_raw(&self, sql: &str, params: &[&dyn rusqlite::types::ToSql]) -> Result<usize> {
        let conn = self.lock_conn()?;
        conn.execute(sql, params).context("Failed to execute raw SQL")
    }
}

fn parse_datetime(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn parse_optional_datetime(s: Option<String>) -> Option<DateTime<Utc>> {
    s.map(|s| parse_datetime(&s))
}

fn map_broadcast_row(row: &rusqlite::Row) -> rusqlite::Result<BroadcastTx> {
    Ok(BroadcastTx {
        id: row.get(0)?,
        tx_hex: row.get(1)?,
        txid: row.get(2)?,
        status: TxStatus::from_str(&row.get::<_, String>(3)?),
        network: row.get(4)?,
        nlocktime: row.get(5)?,
        broadcast_mode: row.get(6)?,
        scheduled_time: parse_optional_datetime(row.get::<_, Option<String>>(7)?),
        broadcast_at: parse_optional_datetime(row.get::<_, Option<String>>(8)?),
        confirmed_at: parse_optional_datetime(row.get::<_, Option<String>>(9)?),
        block_height: row.get(10)?,
        target_fee_rate: row.get(11)?,
        actual_fee_rate: row.get(12)?,
        source_label: row.get(13)?,
        destination_address: row.get(14)?,
        utxo_count: row.get(15)?,
        total_value_btc: row.get(16)?,
        replacement_of: row.get(17)?,
        error_message: row.get(18)?,
        retry_count: row.get(19)?,
        broadcast_missed_at: parse_optional_datetime(row.get::<_, Option<String>>(20)?),
        original_scheduled_time: parse_optional_datetime(row.get::<_, Option<String>>(21)?),
        defer_until: parse_optional_datetime(row.get::<_, Option<String>>(22)?),
        schedule_trigger: row.get(23)?,
        target_price: row.get(24)?,
        price_currency: row.get(25)?,
        price_condition: row.get(26)?,
        created_at: parse_datetime(&row.get::<_, String>(27)?),
        updated_at: parse_datetime(&row.get::<_, String>(28)?),
        locktime_waiting: None,
        locktime_deferred: None,
        can_reschedule: None,
        chain_mtp: None,
        locktime_target: None,
        locktime_remaining_secs: None,
        locktime_satisfied: None,
        current_btc_price: None,
    })
}

use chrono::DateTime;
