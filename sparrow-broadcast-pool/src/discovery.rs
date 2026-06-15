//! Auto-discovery: Bitcoin network from RPC, Electrs/Fulcrum on ports 50001/50002, LAN IP for wallets.

use crate::config::{Config, NetworkType};
use crate::rpc::{BitcoinRpc, ElectrumClient};

pub const INDEXER_PORTS: [u16; 2] = [50001, 50002];

/// Map Bitcoin Core `getblockchaininfo().chain` to our network type.
pub fn network_from_bitcoin_chain(chain: &str) -> NetworkType {
    match chain.to_lowercase().as_str() {
        "main" => NetworkType::Mainnet,
        "signet" => NetworkType::Signet,
        "test" | "testnet" | "testnet3" | "testnet4" => NetworkType::Testnet4,
        _ => NetworkType::Testnet4,
    }
}

/// Best-effort LAN IP (same heuristic as Umbrel DEVICE_IP).
pub fn detect_lan_ip() -> Option<String> {
    if let Ok(ip) = std::env::var("BROADCAST_POOL_LAN_IP") {
        let ip = ip.trim().to_string();
        if !ip.is_empty() && is_plausible_lan_ip(&ip) {
            return Some(ip);
        }
    }

    if let Ok(out) = std::process::Command::new("sh")
        .arg("-c")
        .arg("ip -o route get to 8.8.8.8 2>/dev/null | sed -n 's/.*src \\([0-9.]\\+\\).*/\\1/p'")
        .output()
    {
        if out.status.success() {
            let ip = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if is_plausible_lan_ip(&ip) {
                return Some(ip);
            }
        }
    }

    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    for target in ["192.168.1.1:1", "192.168.50.1:1", "10.0.0.1:1", "8.8.8.8:80"] {
        if socket.connect(target).is_ok() {
            if let Ok(addr) = socket.local_addr() {
                let ip = addr.ip().to_string();
                if is_plausible_lan_ip(&ip) && !is_likely_docker_bridge(&ip) {
                    return Some(ip);
                }
            }
        }
    }
    None
}

fn is_plausible_lan_ip(ip: &str) -> bool {
    !ip.starts_with("127.") && !ip.starts_with("0.") && ip.contains('.')
}

/// Umbrel / Docker internal subnets — not useful for Sparrow on the LAN.
fn is_likely_docker_bridge(ip: &str) -> bool {
    ip.starts_with("10.21.") || ip.starts_with("172.17.") || ip.starts_with("172.18.")
}

pub fn resolve_lan_host(config: &Config) -> Option<String> {
    if let Some(ref h) = config.electrum_server.lan_connect_host {
        let h = h.trim();
        if !h.is_empty() {
            return Some(h.to_string());
        }
    }
    detect_lan_ip()
}

pub fn wallet_connect_url(config: &Config, port: u16) -> String {
    match resolve_lan_host(config) {
        Some(host) => format!("{}:{}", host, port),
        None => format!("<LAN_IP>:{}", port),
    }
}

pub fn strip_indexer_scheme(url: &str) -> &str {
    url.strip_prefix("tcp://")
        .or_else(|| url.strip_prefix("ssl://"))
        .unwrap_or(url)
}

pub fn extract_indexer_host(url: &str) -> Option<String> {
    let bare = strip_indexer_scheme(url).trim();
    if bare.is_empty() {
        return None;
    }
    let host = bare.split(':').next()?.trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

fn indexer_host_candidates(config: &Config) -> Vec<String> {
    let mut hosts = Vec::new();
    if let Ok(h) = std::env::var("APP_ELECTRS_NODE_IP") {
        let h = h.trim().to_string();
        if !h.is_empty() {
            hosts.push(h);
        }
    }
    if let Some(ref idx) = config.indexer {
        if let Some(h) = extract_indexer_host(&idx.url) {
            if !hosts.contains(&h) {
                hosts.push(h);
            }
        }
    }
    if hosts.is_empty() {
        hosts.push("127.0.0.1".to_string());
    }
    hosts
}

fn indexer_ports_to_try() -> Vec<u16> {
    let mut ports = Vec::new();
    if let Ok(p) = std::env::var("APP_ELECTRS_NODE_PORT") {
        if let Ok(n) = p.parse::<u16>() {
            ports.push(n);
        }
    }
    for p in INDEXER_PORTS {
        if !ports.contains(&p) {
            ports.push(p);
        }
    }
    ports
}

/// Probe TCP ports 50001/50002; keep indexers whose genesis matches `network`.
pub fn discover_indexer_url(network: &NetworkType, config: &Config) -> Option<String> {
    let ports = indexer_ports_to_try();
    for host in indexer_host_candidates(config) {
        for port in &ports {
            let url = format!("tcp://{}:{}", host, port);
            if indexer_matches_network(&url, network) {
                tracing::info!(
                    "Auto-detected indexer at {} (genesis matches {})",
                    url,
                    network.data_dir_name()
                );
                return Some(url);
            }
        }
    }
    None
}

fn indexer_matches_network(url: &str, network: &NetworkType) -> bool {
    let client = match ElectrumClient::new(url) {
        Ok(c) => c,
        Err(_) => return false,
    };
    if !client.test_connection().unwrap_or(false) {
        return false;
    }
    client.genesis_matches_network(network).unwrap_or(false)
}

/// Sync network from Bitcoin Core when RPC is available.
pub fn apply_network_from_rpc(config: &mut Config, rpc: Option<&BitcoinRpc>) {
    let Some(rpc) = rpc else {
        return;
    };
    let chain = match rpc.get_bitcoin_chain() {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!("Could not read Bitcoin chain for network detection: {}", e);
            return;
        }
    };
    let detected = network_from_bitcoin_chain(&chain);
    if config.network.network_type != detected {
        tracing::info!(
            "Network from Bitcoin Core chain '{}' → {} (config had {})",
            chain,
            detected.data_dir_name(),
            config.network.network_type.data_dir_name()
        );
        config.network.network_type = detected;
    } else {
        tracing::debug!(
            "Network confirmed from Bitcoin Core: {}",
            detected.data_dir_name()
        );
    }
}

/// Discover indexer unless explicitly pinned via env or saved manual URL.
pub fn apply_indexer_discovery(config: &mut Config) {
    if std::env::var("BROADCAST_POOL_INDEXER_URL").is_ok() {
        tracing::debug!("Indexer URL pinned by BROADCAST_POOL_INDEXER_URL");
        return;
    }

    let network = config.network.network_type.clone();
    if let Some(url) = discover_indexer_url(&network, config) {
        config.indexer = Some(crate::config::IndexerConfig { url });
        return;
    }

    if config.indexer.is_some() {
        let url = config.indexer.as_ref().unwrap().url.clone();
        if indexer_matches_network(&url, &network) {
            tracing::info!("Existing indexer URL validated: {}", url);
            return;
        }
        tracing::warn!(
            "Configured indexer {} does not match Bitcoin network {} — trying auto-discovery",
            url,
            network.data_dir_name()
        );
        if let Some(found) = discover_indexer_url(&network, config) {
            config.indexer = Some(crate::config::IndexerConfig { url: found });
        }
    } else if let Some(found) = discover_indexer_url(&network, config) {
        config.indexer = Some(crate::config::IndexerConfig { url: found });
    } else {
        tracing::warn!(
            "Could not auto-detect Electrs/Fulcrum on ports {:?} — configure indexer manually",
            indexer_ports_to_try()
        );
    }
}

pub fn apply_lan_ip(config: &mut Config) {
    if config
        .electrum_server
        .lan_connect_host
        .as_ref()
        .is_some_and(|h| !h.trim().is_empty())
    {
        return;
    }
    if let Some(ip) = detect_lan_ip() {
        tracing::info!("LAN IP for wallet connections: {}", ip);
        config.electrum_server.lan_connect_host = Some(ip);
    } else {
        tracing::warn!(
            "Could not detect LAN IP — set BROADCAST_POOL_LAN_IP or configure in Settings"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_bitcoin_chains() {
        assert_eq!(network_from_bitcoin_chain("main"), NetworkType::Mainnet);
        assert_eq!(network_from_bitcoin_chain("test"), NetworkType::Testnet4);
        assert_eq!(network_from_bitcoin_chain("signet"), NetworkType::Signet);
    }

    #[test]
    fn extracts_indexer_host() {
        assert_eq!(
            extract_indexer_host("tcp://10.21.22.5:50002").as_deref(),
            Some("10.21.22.5")
        );
        assert_eq!(
            extract_indexer_host("192.168.1.10:50001").as_deref(),
            Some("192.168.1.10")
        );
    }
}
