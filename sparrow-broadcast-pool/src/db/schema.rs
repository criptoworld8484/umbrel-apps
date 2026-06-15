pub const MIGRATION_001: &str = r#"
CREATE TABLE IF NOT EXISTS broadcast_pool (
    id TEXT PRIMARY KEY,
    tx_hex TEXT NOT NULL,
    txid TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    network TEXT NOT NULL,
    nlocktime INTEGER DEFAULT 0,
    broadcast_mode TEXT DEFAULT 'immediate',
    scheduled_time TEXT,
    broadcast_at TEXT,
    confirmed_at TEXT,
    block_height INTEGER,
    target_fee_rate REAL,
    actual_fee_rate REAL,
    source_label TEXT,
    destination_address TEXT,
    utxo_count INTEGER DEFAULT 1,
    total_value_btc REAL DEFAULT 0.0,
    replacement_of TEXT,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pool_status ON broadcast_pool(status);
CREATE INDEX IF NOT EXISTS idx_pool_scheduled ON broadcast_pool(scheduled_time);
CREATE INDEX IF NOT EXISTS idx_pool_network ON broadcast_pool(network);
CREATE INDEX IF NOT EXISTS idx_pool_txid ON broadcast_pool(txid);

CREATE TABLE IF NOT EXISTS migration_plans (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_wallet TEXT,
    destination_wallet TEXT,
    network TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    min_delay_hours INTEGER DEFAULT 2,
    max_delay_hours INTEGER DEFAULT 72,
    min_fee_rate REAL DEFAULT 1.0,
    max_fee_rate REAL DEFAULT 50.0,
    total_transactions INTEGER DEFAULT 0,
    completed_transactions INTEGER DEFAULT 0,
    total_value_migrated_btc REAL DEFAULT 0.0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_plans_status ON migration_plans(status);
CREATE INDEX IF NOT EXISTS idx_plans_network ON migration_plans(network);

CREATE TABLE IF NOT EXISTS migration_utxos (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES migration_plans(id) ON DELETE CASCADE,
    txid TEXT NOT NULL,
    vout INTEGER NOT NULL,
    value_btc REAL NOT NULL,
    address TEXT,
    label TEXT,
    source_label TEXT,
    broadcast_pool_id TEXT REFERENCES broadcast_pool(id),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_utxos_plan ON migration_utxos(plan_id);
CREATE INDEX IF NOT EXISTS idx_utxos_pool_id ON migration_utxos(broadcast_pool_id);

CREATE TABLE IF NOT EXISTS config_store (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#;

pub const MIGRATION_002: &str = r#"
ALTER TABLE broadcast_pool ADD COLUMN nlocktime INTEGER DEFAULT 0;
"#;

pub const MIGRATION_003: &str = r#"
CREATE INDEX IF NOT EXISTS idx_pool_nlocktime ON broadcast_pool(nlocktime);
"#;

pub const MIGRATION_004: &str = r#"
ALTER TABLE broadcast_pool ADD COLUMN broadcast_mode TEXT DEFAULT 'immediate';
"#;

pub const MIGRATION_005: &str = r#"
ALTER TABLE broadcast_pool ADD COLUMN broadcast_missed_at TEXT;
ALTER TABLE broadcast_pool ADD COLUMN original_scheduled_time TEXT;
ALTER TABLE broadcast_pool ADD COLUMN defer_until TEXT;
"#;

pub const MIGRATION_006: &str = r#"
ALTER TABLE broadcast_pool ADD COLUMN schedule_trigger TEXT DEFAULT 'datetime';
ALTER TABLE broadcast_pool ADD COLUMN target_price REAL;
ALTER TABLE broadcast_pool ADD COLUMN price_currency TEXT;
ALTER TABLE broadcast_pool ADD COLUMN price_condition TEXT;
"#;
