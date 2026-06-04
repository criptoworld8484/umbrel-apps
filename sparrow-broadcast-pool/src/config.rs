use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub network: NetworkConfig,
    pub bitcoin_rpc: Option<BitcoinRpcConfig>,
    pub indexer: Option<IndexerConfig>,
    pub pool: PoolConfig,
    #[serde(default)]
    pub schedule: ScheduleConfig,
    pub privacy: PrivacyConfig,
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default)]
    pub electrum_server: ElectrumServerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BroadcastMode {
    Immediate,
    Scheduled,
    ByBlock,
}

impl Default for BroadcastMode {
    fn default() -> Self {
        BroadcastMode::Immediate
    }
}

impl std::fmt::Display for BroadcastMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BroadcastMode::Immediate => write!(f, "immediate"),
            BroadcastMode::Scheduled => write!(f, "scheduled"),
            BroadcastMode::ByBlock => write!(f, "by_block"),
        }
    }
}

impl std::str::FromStr for BroadcastMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "immediate" => Ok(BroadcastMode::Immediate),
            "scheduled" => Ok(BroadcastMode::Scheduled),
            "by_block" => Ok(BroadcastMode::ByBlock),
            _ => Err(format!("Unknown broadcast mode: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerConfig {
    pub url: String,
}

impl Config {
    pub fn load(config_path: Option<&Path>) -> Result<Self> {
        let path = match config_path {
            Some(p) => p.to_path_buf(),
            None => Self::default_config_path()?,
        };

        let mut config = if path.exists() {
            let content = std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read config file: {}", path.display()))?;
            toml::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {}", path.display()))?
        } else {
            tracing::warn!("Config file not found at {}, using defaults", path.display());
            Self::default_config()
        };

        // Apply environment variable overrides (for Docker / Umbrel)
        if let Ok(url) = std::env::var("BROADCAST_POOL_RPC_URL") {
            if config.bitcoin_rpc.is_none() {
                config.bitcoin_rpc = Some(BitcoinRpcConfig {
                    url: String::new(),
                    user: String::new(),
                    password: String::new(),
                });
            }
            config.bitcoin_rpc.as_mut().unwrap().url = url;
        }
        if let Ok(user) = std::env::var("BROADCAST_POOL_RPC_USER") {
            if let Some(ref mut rpc) = config.bitcoin_rpc {
                rpc.user = user;
            }
        }
        if let Ok(pass) = std::env::var("BROADCAST_POOL_RPC_PASS") {
            if let Some(ref mut rpc) = config.bitcoin_rpc {
                rpc.password = pass;
            }
        }
        if let Ok(url) = std::env::var("BROADCAST_POOL_INDEXER_URL") {
            config.indexer = Some(IndexerConfig { url });
        } else if let (Ok(host), Ok(port)) = (
            std::env::var("APP_ELECTRS_NODE_IP"),
            std::env::var("APP_ELECTRS_NODE_PORT"),
        ) {
            config.indexer = Some(IndexerConfig {
                url: format!("tcp://{}:{}", host, port),
            });
        }
        if let Ok(network) = std::env::var("BROADCAST_POOL_NETWORK") {
            config.network.network_type = match network.to_lowercase().as_str() {
                "mainnet" => NetworkType::Mainnet,
                "signet" => NetworkType::Signet,
                _ => NetworkType::Testnet4,
            };
        } else if let Ok(network) = std::env::var("APP_BITCOIN_NETWORK") {
            config.network.network_type = match network.to_lowercase().as_str() {
                "mainnet" => NetworkType::Mainnet,
                "signet" => NetworkType::Signet,
                "testnet" | "testnet3" | "testnet4" => NetworkType::Testnet4,
                _ => config.network.network_type.clone(),
            };
        }
        if let Ok(host) = std::env::var("BROADCAST_POOL_WEB_HOST") {
            config.web.host = host;
        }
        if let Ok(port) = std::env::var("BROADCAST_POOL_WEB_PORT") {
            if let Ok(p) = port.parse() {
                config.web.port = p;
            }
        }
        if let Ok(host) = std::env::var("BROADCAST_POOL_ELECTRUM_HOST") {
            config.electrum_server.host = host;
        }
        if let Ok(port) = std::env::var("BROADCAST_POOL_ELECTRUM_PORT") {
            if let Ok(p) = port.parse() {
                config.electrum_server.port = p;
            }
        }

        Ok(config)
    }

    fn default_config_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".config")))
            .unwrap_or_else(|| PathBuf::from("."));
        Ok(config_dir.join("broadcast-pool").join("config.toml"))
    }

    pub fn default_config() -> Self {
        let network_type = std::env::var("BROADCAST_POOL_NETWORK")
            .map(|n| match n.to_lowercase().as_str() {
                "mainnet" => NetworkType::Mainnet,
                "signet" => NetworkType::Signet,
                _ => NetworkType::Testnet4,
            })
            .unwrap_or(NetworkType::Testnet4);

        let default_indexer = match network_type {
            NetworkType::Mainnet => "ssl://192.168.50.97:50002",
            NetworkType::Testnet4 | NetworkType::Signet => "192.168.50.26:50001",
        };

        let indexer_url = std::env::var("BROADCAST_POOL_INDEXER_URL")
            .unwrap_or_else(|_| default_indexer.to_string());

        Self {
            network: NetworkConfig {
                network_type,
            },
            bitcoin_rpc: std::env::var("BROADCAST_POOL_RPC_URL").ok().map(|url| {
                BitcoinRpcConfig {
                    url,
                    user: std::env::var("BROADCAST_POOL_RPC_USER").unwrap_or_default(),
                    password: std::env::var("BROADCAST_POOL_RPC_PASS").unwrap_or_default(),
                }
            }),
            indexer: Some(IndexerConfig { url: indexer_url }),
            pool: PoolConfig {
                max_size_kb: 300,
                rebroadcast_interval_minutes: 30,
                expiry_days: 14,
            },
            schedule: ScheduleConfig {
                broadcast_mode: BroadcastMode::Immediate,
                default_delay_hours: 24,
                scheduled_datetime: None,
                min_delay_hours: 2,
                max_delay_hours: 72,
                min_fee_rate: 1.0,
                max_fee_rate: 50.0,
            },
            privacy: PrivacyConfig {
                use_tor: false,
                tor_socks_port: 9050,
                rotate_identity_per_tx: true,
            },
            web: WebConfig {
                host: std::env::var("BROADCAST_POOL_WEB_HOST")
                    .unwrap_or_else(|_| "127.0.0.1".to_string()),
                port: std::env::var("BROADCAST_POOL_WEB_PORT")
                    .unwrap_or_else(|_| "8080".to_string())
                    .parse()
                    .unwrap_or(8080),
            },
            electrum_server: ElectrumServerConfig {
                host: std::env::var("BROADCAST_POOL_ELECTRUM_HOST")
                    .unwrap_or_else(|_| "0.0.0.0".to_string()),
                port: std::env::var("BROADCAST_POOL_ELECTRUM_PORT")
                    .unwrap_or_else(|_| "50050".to_string())
                    .parse()
                    .unwrap_or(50050),
            },
        }
    }

    pub fn db_path(&self, data_dir: &Path) -> PathBuf {
        data_dir.join(format!("broadcast-pool-{}.db", self.network.network_type.data_dir_name()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    #[serde(rename = "type")]
    pub network_type: NetworkType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum NetworkType {
    Mainnet,
    #[serde(alias = "testnet", alias = "testnet3")]
    Testnet4,
    Signet,
}

impl NetworkType {
    pub fn default_port(&self) -> u16 {
        match self {
            NetworkType::Mainnet => 8332,
            NetworkType::Testnet4 => 48332,
            NetworkType::Signet => 38332,
        }
    }

    pub fn data_dir_name(&self) -> &str {
        match self {
            NetworkType::Mainnet => "mainnet",
            NetworkType::Testnet4 => "testnet4",
            NetworkType::Signet => "signet",
        }
    }

    pub fn supported_networks() -> &'static [&'static str] {
        &["mainnet", "testnet4", "signet"]
    }

    pub fn genesis_hash(&self) -> &'static str {
        match self {
            NetworkType::Mainnet => {
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
            }
            NetworkType::Testnet4 => {
                "000000000933ea01ad0ee984209779baaec3ced90fa537f92f5ac0adcf472867"
            }
            NetworkType::Signet => {
                "00000008819873e925632181568121be59ecd5df7a9c348375d874564ae96f681"
            }
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            NetworkType::Mainnet => "Mainnet",
            NetworkType::Testnet4 => "Testnet 4",
            NetworkType::Signet => "Signet",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinRpcConfig {
    pub url: String,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub max_size_kb: u64,
    pub rebroadcast_interval_minutes: u64,
    pub expiry_days: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleConfig {
    #[serde(default)]
    pub broadcast_mode: BroadcastMode,
    #[serde(default = "default_delay_hours")]
    pub default_delay_hours: u64,
    #[serde(default)]
    pub scheduled_datetime: Option<String>,
    pub min_delay_hours: u64,
    pub max_delay_hours: u64,
    pub min_fee_rate: f64,
    pub max_fee_rate: f64,
}

fn default_delay_hours() -> u64 {
    24
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            broadcast_mode: BroadcastMode::Immediate,
            default_delay_hours: 24,
            scheduled_datetime: None,
            min_delay_hours: 2,
            max_delay_hours: 72,
            min_fee_rate: 1.0,
            max_fee_rate: 50.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectrumServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ElectrumServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 50050,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyConfig {
    pub use_tor: bool,
    pub tor_socks_port: u16,
    pub rotate_identity_per_tx: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebConfig {
    pub host: String,
    pub port: u16,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}