//! Auto-discovery: Bitcoin network from RPC, Electrs/Fulcrum on ports 50001/50002, LAN IP for wallets.

use crate::config::{Config, NetworkType};
use crate::rpc::{BitcoinRpc, ElectrumClient};
use serde::Serialize;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const INDEXER_PORTS: [u16; 2] = [50001, 50002];
const BROADER_PORT_RANGE: std::ops::RangeInclusive<u16> = 50000..=50010;
const TCP_PROBE_MS: u64 = 150;
const UMBREL_TCP_CONNECT_SECS: u64 = 2;
const UMBREL_DISCOVERY_COOLDOWN: Duration = Duration::from_secs(45);

static LAST_UMBREL_DISCOVERY: Mutex<Option<Instant>> = Mutex::new(None);

/// Map Bitcoin Core `getblockchaininfo().chain` to our network type.
pub fn network_from_bitcoin_chain(chain: &str) -> NetworkType {
    match chain.to_lowercase().as_str() {
        "main" => NetworkType::Mainnet,
        "signet" => NetworkType::Signet,
        "test" | "testnet" | "testnet3" | "testnet4" => NetworkType::Testnet4,
        _ => NetworkType::Testnet4,
    }
}

/// Expected genesis hash: from Bitcoin Core on Umbrel (supports custom signets), else built-in.
pub fn expected_genesis_hash(network: &NetworkType) -> String {
    if let Some(gh) = bitcoin_rpc_genesis_hash() {
        return gh;
    }
    network.genesis_hash().to_lowercase()
}

fn bitcoin_rpc_genesis_hash() -> Option<String> {
    let url = std::env::var("BROADCAST_POOL_RPC_URL").ok()?;
    let user = std::env::var("BROADCAST_POOL_RPC_USER").ok()?;
    let pass = std::env::var("BROADCAST_POOL_RPC_PASS").ok()?;
    let config = crate::config::BitcoinRpcConfig {
        url,
        user,
        password: pass,
    };
    BitcoinRpc::new(&config)
        .ok()?
        .get_genesis_block_hash()
        .ok()
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

/// Electrs/Fulcrum convention: 50001 = plain TCP, 50002 = TLS.
pub fn default_scheme_for_port(port: u16) -> &'static str {
    if port == 50002 {
        "ssl"
    } else {
        "tcp"
    }
}

/// True when running on Umbrel (node indexer available via Docker env).
pub fn is_umbrel_mode() -> bool {
    std::env::var("BROADCAST_POOL_UMBREL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
        || std::env::var("APP_ELECTRS_NODE_IP")
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
}

pub fn indexer_url_uses_ssl(url: &str) -> bool {
    url.trim().starts_with("ssl://")
}

/// Build canonical indexer URL; `use_ssl` overrides port-based scheme when set.
pub fn normalize_indexer_url_with_scheme(raw: &str, use_ssl: Option<bool>) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with("tcp://") || trimmed.starts_with("ssl://") {
        return trimmed.to_string();
    }
    if let Some(ssl) = use_ssl {
        if let Some((host, port_str)) = trimmed.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                let host = host.trim();
                if !host.is_empty() {
                    let scheme = if ssl { "ssl" } else { "tcp" };
                    return format!("{}://{}:{}", scheme, host, port);
                }
            }
        }
    }
    normalize_indexer_url(trimmed)
}

/// Normalize user input to a canonical indexer URL with scheme.
pub fn normalize_indexer_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with("tcp://") || trimmed.starts_with("ssl://") {
        return trimmed.to_string();
    }
    if let Some((host, port_str)) = trimmed.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            let host = host.trim();
            if !host.is_empty() {
                return format!(
                    "{}://{}:{}",
                    default_scheme_for_port(port),
                    host,
                    port
                );
            }
        }
    }
    format!("tcp://{}", trimmed)
}

/// Host:port for Settings UI (scheme omitted; port implies TCP vs SSL).
pub fn display_indexer_url(url: &str) -> String {
    strip_indexer_scheme(url).trim().to_string()
}

fn umbrel_electrs_ssl_configured() -> bool {
    env_nonempty("APP_ELECTRS_NODE_SSL_PORT").is_some()
}

/// All TCP/SSL combinations for standard Electrs/Fulcrum ports on one host.
/// Order per IP: 50001/tcp, 50001/ssl, 50002/tcp, 50002/ssl.
/// Umbrel electrs serves plain TCP only unless APP_ELECTRS_NODE_SSL_PORT is set.
pub fn candidate_urls_for_host(host: &str, ports: &[u16]) -> Vec<String> {
    let mut urls = Vec::new();
    let umbrel_tcp_only = is_umbrel_mode() && !umbrel_electrs_ssl_configured();
    for &port in ports {
        urls.push(format!("tcp://{}:{}", host, port));
        if !umbrel_tcp_only {
            urls.push(format!("ssl://{}:{}", host, port));
        }
    }
    urls
}

/// Connection attempts for a host/port (both TCP and SSL).
pub fn candidate_urls_for_host_port(host: &str, port: u16) -> Vec<String> {
    if is_umbrel_mode() && !umbrel_electrs_ssl_configured() {
        return vec![format!("tcp://{}:{}", host, port)];
    }
    if port == 50002 {
        vec![
            format!("ssl://{}:{}", host, port),
            format!("tcp://{}:{}", host, port),
        ]
    } else {
        vec![
            format!("tcp://{}:{}", host, port),
            format!("ssl://{}:{}", host, port),
        ]
    }
}

/// Ordered URLs to try when connecting — always both TCP and SSL for host:port.
pub fn connection_url_candidates(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let bare = strip_indexer_scheme(trimmed).trim();
    if let Some((host, port_str)) = bare.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            return candidate_urls_for_host_port(host.trim(), port);
        }
    }
    if trimmed.starts_with("tcp://") || trimmed.starts_with("ssl://") {
        return vec![trimmed.to_string()];
    }
    vec![format!("tcp://{}", trimmed)]
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

fn push_unique(hosts: &mut Vec<String>, host: String) {
    if !host.is_empty() && !hosts.contains(&host) {
        hosts.push(host);
    }
}

/// Known hosts in priority order: Umbrel docker IP, saved indexer host, localhost, LAN IP.
fn indexer_host_candidates(config: &Config) -> Vec<String> {
    if is_umbrel_mode() {
        return umbrel_indexer_hosts();
    }
    let mut hosts = Vec::new();
    if let Ok(h) = std::env::var("APP_ELECTRS_NODE_IP") {
        push_unique(&mut hosts, h.trim().to_string());
    }
    if let Some(ref idx) = config.indexer {
        if let Some(h) = extract_indexer_host(&idx.url) {
            push_unique(&mut hosts, h);
        }
    }
    push_unique(&mut hosts, "127.0.0.1".to_string());
    if let Some(lan) = detect_lan_ip() {
        push_unique(&mut hosts, lan);
    }
    hosts
}

fn umbrel_indexer_hosts() -> Vec<String> {
    let mut hosts = Vec::new();
    if let Ok(h) = std::env::var("APP_ELECTRS_NODE_IP") {
        push_unique(&mut hosts, h.trim().to_string());
    }
    push_unique(&mut hosts, "electrs".to_string());
    hosts
}

fn umbrel_indexer_ports() -> Vec<u16> {
    let mut ports = Vec::new();
    if let Ok(p) = std::env::var("APP_ELECTRS_NODE_PORT") {
        if let Ok(n) = p.parse::<u16>() {
            ports.push(n);
        }
    }
    if let Ok(p) = std::env::var("APP_ELECTRS_NODE_SSL_PORT") {
        if let Ok(n) = p.parse::<u16>() {
            ports.push(n);
        }
    }
    if ports.is_empty() {
        ports.push(50001);
    }
    if !is_umbrel_mode() {
        for p in INDEXER_PORTS {
            if !ports.contains(&p) {
                ports.push(p);
            }
        }
    }
    ports
}

/// Connect only to the Umbrel electrs container (never LAN scan).
pub fn discover_umbrel_node_indexer(network: &NetworkType) -> Option<String> {
    let mut env_keys = vec![
        "BROADCAST_POOL_UMBREL_ELECTRS_TCP",
        "BROADCAST_POOL_INDEXER_URL",
    ];
    if umbrel_electrs_ssl_configured()
        || env_nonempty("BROADCAST_POOL_UMBREL_ELECTRS_SSL").is_some()
    {
        env_keys.insert(1, "BROADCAST_POOL_UMBREL_ELECTRS_SSL");
    }
    for key in env_keys {
        if let Ok(raw) = std::env::var(key) {
            let raw = raw.trim();
            if raw.is_empty() || raw.contains("${") {
                continue;
            }
            if let Some(url) = resolve_working_indexer_url(raw, network) {
                tracing::info!(
                    "Umbrel indexer from {} → {}",
                    key,
                    display_indexer_url(&url)
                );
                return Some(url);
            }
            tracing::warn!(
                "Umbrel indexer candidate from {} unreachable: {}",
                key,
                display_indexer_url(raw)
            );
        }
    }

    let hosts = umbrel_indexer_hosts();
    let ports = umbrel_indexer_ports();
    if let Ok(ip) = std::env::var("APP_ELECTRS_NODE_IP") {
        tracing::info!(
            "Umbrel electrs discovery at {} (ports {:?}, tcp+ssl)",
            ip.trim(),
            ports
        );
    } else {
        tracing::warn!(
            "APP_ELECTRS_NODE_IP is not set — ensure Electrs is installed and listed as a dependency"
        );
    }
    if hosts.is_empty() {
        return None;
    }
    try_hosts_ports(network, &hosts, &ports)
}

/// Manual override to the node's wallet LAN IP cannot work from inside the Umbrel container.
pub fn is_mistaken_umbrel_lan_override(host: &str) -> bool {
    if !is_umbrel_mode() {
        return false;
    }
    let host = host.trim();
    if host.is_empty() {
        return false;
    }
    if host == "electrs" || host == "localhost" || host == "127.0.0.1" {
        return false;
    }
    let node_ip = std::env::var("APP_ELECTRS_NODE_IP")
        .unwrap_or_default()
        .trim()
        .to_string();
    if !node_ip.is_empty() && host == node_ip {
        return false;
    }
    // Umbrel inter-app network (10.21.x.x) — valid electrs target.
    if host.starts_with("10.21.") {
        return false;
    }
    if let Ok(lan) = std::env::var("BROADCAST_POOL_LAN_IP") {
        if host == lan.trim() {
            return true;
        }
    }
    // Wallet/home LAN IPs are not reachable as electrs from the app container.
    is_private_lan_host(host)
}

fn is_private_lan_host(host: &str) -> bool {
    if host.starts_with("192.168.") {
        return true;
    }
    if let Some(rest) = host.strip_prefix("172.") {
        if let Some(oct) = rest.split('.').next().and_then(|s| s.parse::<u8>().ok()) {
            return (16..=31).contains(&oct);
        }
    }
    false
}

fn clear_mistaken_umbrel_lan_override(config: &mut Config) -> bool {
    let mistaken = config.indexer.as_ref().and_then(|idx| {
        extract_indexer_host(&idx.url).filter(|h| is_mistaken_umbrel_lan_override(h))
    });
    if let Some(host) = mistaken {
        tracing::info!(
            "Removing Umbrel indexer URL at {} (wallet/home LAN — electrs uses APP_ELECTRS_NODE_IP)",
            host
        );
        config.indexer = None;
        return true;
    }
    false
}

/// Fast Umbrel heal: remove mistaken wallet-LAN indexer URLs and manual overrides only.
/// Does not run electrs discovery (safe for /api/status polling).
pub fn sanitize_umbrel_indexer_config(config: &mut Config) -> bool {
    if !is_umbrel_mode() {
        return false;
    }
    let mut changed = clear_mistaken_umbrel_lan_override(config);
    if config
        .indexer
        .as_ref()
        .is_some_and(|i| i.manual_override)
    {
        config.indexer = None;
        changed = true;
    }
    changed
}

/// Discover Umbrel electrs when missing; `force` skips cooldown (startup / manual discover).
pub fn discover_umbrel_if_needed(config: &mut Config, force: bool) -> bool {
    if !is_umbrel_mode() {
        return false;
    }
    let mut changed = sanitize_umbrel_indexer_config(config);
    if config.indexer.is_some() {
        return changed;
    }
    if !force {
        if let Ok(guard) = LAST_UMBREL_DISCOVERY.lock() {
            if let Some(t) = *guard {
                if t.elapsed() < UMBREL_DISCOVERY_COOLDOWN {
                    tracing::debug!("Skipping Umbrel electrs discovery (cooldown)");
                    return changed;
                }
            }
        }
    }
    if let Some(url) = discover_umbrel_node_indexer(&config.network.network_type) {
        config.indexer = Some(crate::config::IndexerConfig {
            url,
            manual_override: false,
        });
        changed = true;
    }
    if let Ok(mut guard) = LAST_UMBREL_DISCOVERY.lock() {
        *guard = Some(Instant::now());
    }
    changed
}

/// Strip mistaken LAN indexer config and reconnect to the node electrs on Umbrel.
pub fn heal_umbrel_indexer_config(config: &mut Config) -> bool {
    discover_umbrel_if_needed(config, true)
}

#[derive(Debug, Clone, Serialize)]
pub struct UmbrelIndexerDiagnostics {
    pub umbrel_mode: bool,
    pub app_electrs_node_ip: Option<String>,
    pub app_electrs_node_port: Option<String>,
    pub app_electrs_node_ssl_port: Option<String>,
    pub umbrel_electrs_tcp_env: Option<String>,
    pub umbrel_electrs_ssl_env: Option<String>,
    pub configured_indexer_url: Option<String>,
    pub configured_indexer_reachable: Option<bool>,
    pub network: String,
    pub status_hint: String,
    pub boot_log_tail: Option<String>,
}

fn umbrel_electrs_probe_failure(network: &NetworkType) -> Option<HostProbeFailure> {
    let hosts = umbrel_indexer_hosts();
    let ports = umbrel_indexer_ports();
    if hosts.is_empty() {
        return None;
    }
    let mut saw_genesis_mismatch = false;
    for host in &hosts {
        match try_host(network, host, &ports) {
            Ok(_) => return None,
            Err(HostProbeFailure::GenesisMismatch) => saw_genesis_mismatch = true,
            Err(HostProbeFailure::TcpRefused) => {}
        }
    }
    if saw_genesis_mismatch {
        Some(HostProbeFailure::GenesisMismatch)
    } else {
        Some(HostProbeFailure::TcpRefused)
    }
}

fn network_mismatch_hint(network: &NetworkType) -> &'static str {
    match network {
        NetworkType::Signet => {
            "Electrs responds but is not on signet — reinstall or reconfigure Electrs for signet (must match Bitcoin Core chain=signet)"
        }
        NetworkType::Mainnet => {
            "Electrs responds but is not on mainnet — verify Electrs matches Bitcoin Core"
        }
        NetworkType::Testnet4 => {
            "Electrs responds but is not on testnet — verify Electrs matches Bitcoin Core"
        }
    }
}

pub fn umbrel_indexer_status_hint(config: &Config, connected: bool) -> String {
    if !is_umbrel_mode() {
        return String::new();
    }
    if connected {
        return String::new();
    }
    let ip = std::env::var("APP_ELECTRS_NODE_IP")
        .unwrap_or_default()
        .trim()
        .to_string();
    if ip.is_empty() || ip.contains("${") {
        return "APP_ELECTRS_NODE_IP is not set — install Electrs and list it as a dependency"
            .to_string();
    }
    let network = &config.network.network_type;
    if let Some(failure) = umbrel_electrs_probe_failure(network) {
        return match failure {
            HostProbeFailure::GenesisMismatch => network_mismatch_hint(network).to_string(),
            HostProbeFailure::TcpRefused => {
                let ports = umbrel_indexer_ports();
                let port_list: Vec<String> = ports.iter().map(|p| p.to_string()).collect();
                format!(
                    "Cannot reach electrs at {} (TCP refused on port{}) — ensure the Electrs container is running and fully started",
                    ip,
                    if port_list.len() == 1 {
                        format!(" {}", port_list[0])
                    } else {
                        format!("s {}", port_list.join("/"))
                    }
                )
            }
        };
    }
    if let Some(ref idx) = config.indexer {
        return format!(
            "Indexer {} not responding — check Electrs is running",
            display_indexer_url(&idx.url)
        );
    }
    format!(
        "Cannot reach electrs at {} (ports 50001/50002) — ensure Electrs is synced and running",
        ip
    )
}

fn read_boot_log_tail(max_lines: usize) -> Option<String> {
    let dir = std::env::var("BROADCAST_POOL_DATA_DIR").ok()?;
    let path = std::path::PathBuf::from(dir).join("umbrel-boot.log");
    let content = std::fs::read_to_string(path).ok()?;
    let lines: Vec<&str> = content.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    Some(lines[start..].join("\n"))
}

pub fn umbrel_indexer_diagnostics(config: &Config, connected: bool) -> UmbrelIndexerDiagnostics {
    let configured = config.indexer.as_ref().map(|i| i.url.clone());
    let reachable = configured.as_ref().map(|url| {
        resolve_working_indexer_url(url, &config.network.network_type).is_some()
    });
    UmbrelIndexerDiagnostics {
        umbrel_mode: is_umbrel_mode(),
        app_electrs_node_ip: env_nonempty("APP_ELECTRS_NODE_IP"),
        app_electrs_node_port: env_nonempty("APP_ELECTRS_NODE_PORT"),
        app_electrs_node_ssl_port: env_nonempty("APP_ELECTRS_NODE_SSL_PORT"),
        umbrel_electrs_tcp_env: env_nonempty("BROADCAST_POOL_UMBREL_ELECTRS_TCP"),
        umbrel_electrs_ssl_env: env_nonempty("BROADCAST_POOL_UMBREL_ELECTRS_SSL"),
        configured_indexer_url: configured
            .as_ref()
            .map(|u| display_indexer_url(u)),
        configured_indexer_reachable: reachable,
        network: config.network.network_type.data_dir_name().to_string(),
        status_hint: umbrel_indexer_status_hint(config, connected),
        boot_log_tail: read_boot_log_tail(20),
    }
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().and_then(|v| {
        let t = v.trim().to_string();
        if t.is_empty() || t.contains("${") {
            None
        } else {
            Some(t)
        }
    })
}

fn indexer_ports_to_try() -> Vec<u16> {
    if is_umbrel_mode() {
        return umbrel_indexer_ports();
    }
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

fn tcp_connect_timeout() -> Duration {
    if is_umbrel_mode() {
        Duration::from_secs(UMBREL_TCP_CONNECT_SECS)
    } else {
        Duration::from_millis(TCP_PROBE_MS)
    }
}

fn tcp_port_open(host: &str, port: u16) -> bool {
    let addr = format!("{}:{}", host, port);
    let Ok(addrs) = addr.to_socket_addrs() else {
        return false;
    };
    let timeout = tcp_connect_timeout();
    for socket_addr in addrs {
        if TcpStream::connect_timeout(&socket_addr, timeout).is_ok() {
            return true;
        }
    }
    false
}

fn lan_subnet_prefix(lan_ip: &str) -> Option<String> {
    let parts: Vec<&str> = lan_ip.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    if parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        Some(format!("{}.{}.{}", parts[0], parts[1], parts[2]))
    } else {
        None
    }
}

fn lan_subnet_hosts(lan_ip: &str) -> Vec<String> {
    let Some(prefix) = lan_subnet_prefix(lan_ip) else {
        return Vec::new();
    };
    (1..=254)
        .map(|n| format!("{}.{}", prefix, n))
        .filter(|h| h != lan_ip)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexerProbeOutcome {
    TcpRefused,
    RpcFailed,
    GenesisMismatch,
    Ok,
}

fn probe_indexer_url(url: &str, network: &NetworkType) -> IndexerProbeOutcome {
    if !tcp_reachable_from_url(url) {
        return IndexerProbeOutcome::TcpRefused;
    }
    if crate::rpc::indexer_transport::probe_working_url(&[url.to_string()]).is_none() {
        return IndexerProbeOutcome::RpcFailed;
    }
    if ElectrumClient::genesis_matches_network_at_url(url, network) {
        IndexerProbeOutcome::Ok
    } else {
        IndexerProbeOutcome::GenesisMismatch
    }
}

fn log_indexer_probe(url: &str, network: &NetworkType, outcome: IndexerProbeOutcome) {
    let display_url = display_indexer_url(url);
    let expected = network.data_dir_name();
    match outcome {
        IndexerProbeOutcome::Ok => {
            tracing::info!(
                "Indexer OK at {} (genesis matches {})",
                display_url,
                expected
            );
        }
        IndexerProbeOutcome::TcpRefused => {
            if is_umbrel_mode() {
                tracing::warn!(
                    "Umbrel indexer candidate unreachable (TCP refused): {}",
                    display_url
                );
            } else {
                tracing::debug!("Indexer probe skip (port closed): {}", display_url);
            }
        }
        IndexerProbeOutcome::RpcFailed => {
            tracing::debug!("Indexer probe skip (no RPC): {}", display_url);
        }
        IndexerProbeOutcome::GenesisMismatch => {
            tracing::warn!(
                "Umbrel indexer candidate reachable but genesis mismatch (expected {}): {}",
                expected,
                display_url
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostProbeFailure {
    TcpRefused,
    GenesisMismatch,
}

fn try_host(network: &NetworkType, host: &str, ports: &[u16]) -> Result<String, HostProbeFailure> {
    let mut saw_tcp_open = false;
    let mut saw_genesis_mismatch = false;
    for url in candidate_urls_for_host(host, ports) {
        let outcome = probe_indexer_url(&url, network);
        log_indexer_probe(&url, network, outcome);
        match outcome {
            IndexerProbeOutcome::Ok => return Ok(url),
            IndexerProbeOutcome::TcpRefused => {}
            IndexerProbeOutcome::RpcFailed => saw_tcp_open = true,
            IndexerProbeOutcome::GenesisMismatch => {
                saw_tcp_open = true;
                saw_genesis_mismatch = true;
            }
        }
    }
    if saw_genesis_mismatch {
        tracing::warn!(
            "Umbrel electrs at {} responds but genesis does not match {} — check Bitcoin Core and Electrs use the same network",
            host,
            network.data_dir_name()
        );
        Err(HostProbeFailure::GenesisMismatch)
    } else if saw_tcp_open {
        Err(HostProbeFailure::TcpRefused)
    } else {
        Err(HostProbeFailure::TcpRefused)
    }
}

fn try_hosts_ports(network: &NetworkType, hosts: &[String], ports: &[u16]) -> Option<String> {
    let mut saw_genesis_mismatch = false;
    for host in hosts {
        match try_host(network, host, ports) {
            Ok(url) => {
                tracing::info!(
                    "Auto-detected indexer at {} (genesis matches {})",
                    display_indexer_url(&url),
                    network.data_dir_name()
                );
                return Some(url);
            }
            Err(HostProbeFailure::GenesisMismatch) => saw_genesis_mismatch = true,
            Err(HostProbeFailure::TcpRefused) => {}
        }
    }
    if is_umbrel_mode() {
        if saw_genesis_mismatch {
            tracing::warn!(
                "Umbrel electrs reachable but wrong network — expected {}, verify Electrs matches Bitcoin Core",
                network.data_dir_name()
            );
        } else {
            tracing::warn!(
                "Umbrel electrs unreachable (TCP refused on ports {:?}) — ensure Electrs container is running and APP_ELECTRS_NODE_IP is correct",
                ports
            );
        }
    }
    None
}

fn lan_subnets_to_scan(config: &Config) -> Vec<String> {
    let mut prefixes = Vec::new();
    if let Ok(ip) = std::env::var("BROADCAST_POOL_LAN_IP") {
        if let Some(p) = lan_subnet_prefix(ip.trim()) {
            push_unique(&mut prefixes, p);
        }
    }
    if let Some(ref idx) = config.indexer {
        if let Some(h) = extract_indexer_host(&idx.url) {
            if let Some(p) = lan_subnet_prefix(&h) {
                push_unique(&mut prefixes, p);
            }
        }
    }
    if let Some(ip) = detect_lan_ip() {
        if !is_likely_docker_bridge(&ip) {
            if let Some(p) = lan_subnet_prefix(&ip) {
                push_unique(&mut prefixes, p);
            }
        }
    }
    prefixes
}

fn hosts_with_open_ports_on(prefix: &str, ports: &[u16]) -> Vec<String> {
    (1..=254)
        .filter_map(|n| {
            let host = format!("{}.{}", prefix, n);
            if ports.iter().any(|p| tcp_port_open(&host, *p)) {
                Some(host)
            } else {
                None
            }
        })
        .collect()
}

fn scan_subnet_for_indexer(
    network: &NetworkType,
    prefix: &str,
    ports: &[u16],
) -> Option<String> {
    tracing::info!(
        "Scanning LAN subnet {}.x for indexer (ports {:?}, tcp+ssl)",
        prefix,
        ports
    );
    let responsive = hosts_with_open_ports_on(prefix, ports);
    tracing::info!(
        "Subnet {}.x: {} host(s) with open indexer port(s)",
        prefix,
        responsive.len()
    );
    try_hosts_ports(network, &responsive, ports)
}

fn hosts_with_broader_ports_subnet(prefix: &str) -> Vec<String> {
    let mut found = Vec::new();
    'host: for n in 1..=254 {
        let host = format!("{}.{}", prefix, n);
        for port in BROADER_PORT_RANGE {
            if tcp_port_open(&host, port) {
                push_unique(&mut found, host);
                continue 'host;
            }
        }
    }
    found
}

/// Probe TCP+SSL on ports 50001/50002: known hosts, then each LAN subnet, then broader ports.
pub fn discover_indexer_url(network: &NetworkType, config: &Config) -> Option<String> {
    if is_umbrel_mode() {
        return discover_umbrel_node_indexer(network);
    }

    let standard_ports = indexer_ports_to_try();

    tracing::info!("Indexer discovery phase 1: known hosts (tcp+ssl on {:?})", standard_ports);
    if let Some(url) = try_hosts_ports(network, &indexer_host_candidates(config), &standard_ports) {
        return Some(url);
    }

    let subnets = lan_subnets_to_scan(config);
    if subnets.is_empty() {
        tracing::warn!("No LAN subnet available for indexer scan — set BROADCAST_POOL_LAN_IP");
    }
    for prefix in &subnets {
        if let Some(url) = scan_subnet_for_indexer(network, prefix, &standard_ports) {
            return Some(url);
        }
    }

    tracing::info!("Indexer discovery phase 3: broader ports 50000–50010 (tcp+ssl)");
    for prefix in &subnets {
        let responsive = hosts_with_broader_ports_subnet(prefix);
        let broader_ports: Vec<u16> = BROADER_PORT_RANGE.collect();
        if let Some(url) = try_hosts_ports(network, &responsive, &broader_ports) {
            return Some(url);
        }
    }

    None
}

/// Find a working indexer URL trying TCP and SSL on the given host:port.
pub fn resolve_working_indexer_url(raw: &str, network: &NetworkType) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with("tcp://") || trimmed.starts_with("ssl://") {
        if indexer_matches_network(trimmed, network) {
            return Some(trimmed.to_string());
        }
        let bare = strip_indexer_scheme(trimmed);
        if let Some((host, port_str)) = bare.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                return try_host(network, host.trim(), &[port]).ok();
            }
        }
        return None;
    }

    if let Some((host, port_str)) = trimmed.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            return try_host(network, host.trim(), &[port]).ok();
        }
    }

    None
}

fn indexer_matches_network(url: &str, network: &NetworkType) -> bool {
    if !tcp_reachable_from_url(url) {
        return false;
    }
    if !crate::rpc::indexer_transport::probe_working_url(&[url.to_string()]).is_some() {
        return false;
    }
    ElectrumClient::genesis_matches_network_at_url(url, network)
}

fn tcp_reachable_from_url(url: &str) -> bool {
    let bare = strip_indexer_scheme(url).trim();
    let Some((host, port_str)) = bare.rsplit_once(':') else {
        return false;
    };
    let Ok(port) = port_str.parse::<u16>() else {
        return false;
    };
    tcp_port_open(host, port)
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
pub fn apply_indexer_discovery(config: &mut Config) -> bool {
    if std::env::var("BROADCAST_POOL_INDEXER_URL").is_ok() {
        tracing::debug!("Indexer URL pinned by BROADCAST_POOL_INDEXER_URL");
        return config.indexer.is_some();
    }

    let network = config.network.network_type.clone();

    if is_umbrel_mode() {
        let cleared = clear_mistaken_umbrel_lan_override(config);
        if !config
            .indexer
            .as_ref()
            .is_some_and(|i| i.manual_override)
        {
            if let Some(url) = discover_umbrel_node_indexer(&network) {
                config.indexer = Some(crate::config::IndexerConfig {
                    url,
                    manual_override: false,
                });
                return true;
            }
            tracing::warn!(
                "Could not reach Umbrel electrs — APP_ELECTRS_NODE_IP={:?}, APP_ELECTRS_NODE_PORT={:?}",
                std::env::var("APP_ELECTRS_NODE_IP").ok(),
                std::env::var("APP_ELECTRS_NODE_PORT").ok(),
            );
        }
        if cleared {
            return false;
        }
    }

    if config
        .indexer
        .as_ref()
        .is_some_and(|i| i.manual_override)
    {
        let raw = config.indexer.as_ref().unwrap().url.clone();
        if let Some(working) = resolve_working_indexer_url(&raw, &network) {
            config.indexer = Some(crate::config::IndexerConfig {
                url: working.clone(),
                manual_override: true,
            });
            tracing::info!(
                "Manual indexer override validated: {}",
                display_indexer_url(&working)
            );
            return true;
        }
        tracing::warn!(
            "Manual indexer {} unreachable — trying node auto-discovery",
            display_indexer_url(&raw)
        );
    }

    if let Some(url) = discover_indexer_url(&network, config) {
        config.indexer = Some(crate::config::IndexerConfig {
            url,
            manual_override: false,
        });
        return true;
    }

    if config.indexer.is_some() && !config.indexer.as_ref().unwrap().manual_override {
        let raw = config.indexer.as_ref().unwrap().url.clone();
        if let Some(working) = resolve_working_indexer_url(&raw, &network) {
            config.indexer = Some(crate::config::IndexerConfig {
                url: working.clone(),
                manual_override: false,
            });
            tracing::info!(
                "Existing indexer URL validated: {}",
                display_indexer_url(&working)
            );
            return true;
        }
        tracing::warn!(
            "Configured indexer {} does not match Bitcoin network {} — trying auto-discovery",
            display_indexer_url(&raw),
            network.data_dir_name()
        );
        if let Some(found) = discover_indexer_url(&network, config) {
            config.indexer = Some(crate::config::IndexerConfig {
                url: found,
                manual_override: false,
            });
            return true;
        }
    } else if config.indexer.is_none() {
        tracing::warn!(
            "Could not auto-detect Electrs/Fulcrum on ports {:?} — configure indexer manually",
            indexer_ports_to_try()
        );
    }
    false
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
            "Could not detect LAN IP — set BROADCAST_POOL_LAN_IP env var"
        );
    }
}

pub fn save_config_to_disk(config: &Config) -> anyhow::Result<()> {
    let config_path = Config::resolved_config_path();

    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let toml_str = toml::to_string_pretty(config)?;
    std::fs::write(&config_path, toml_str)?;
    tracing::info!("Config saved to {:?}", config_path);
    Ok(())
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

    #[test]
    fn lan_subnet_hosts_excludes_self() {
        let hosts = lan_subnet_hosts("192.168.1.50");
        assert!(!hosts.contains(&"192.168.1.50".to_string()));
        assert!(hosts.contains(&"192.168.1.1".to_string()));
    }

    #[test]
    fn normalize_indexer_url_with_explicit_ssl() {
        assert_eq!(
            normalize_indexer_url_with_scheme("192.168.1.10:50001", Some(true)),
            "ssl://192.168.1.10:50001"
        );
        assert_eq!(
            normalize_indexer_url_with_scheme("192.168.1.10:50002", Some(false)),
            "tcp://192.168.1.10:50002"
        );
    }

    #[test]
    fn mistaken_umbrel_lan_override_detects_wallet_ip() {
        std::env::set_var("BROADCAST_POOL_UMBREL", "1");
        std::env::set_var("BROADCAST_POOL_LAN_IP", "192.168.50.26");
        std::env::set_var("APP_ELECTRS_NODE_IP", "10.21.21.9");
        assert!(is_mistaken_umbrel_lan_override("192.168.50.26"));
        assert!(!is_mistaken_umbrel_lan_override("10.21.21.9"));
        assert!(!is_mistaken_umbrel_lan_override("10.21.22.5"));
        assert!(!is_mistaken_umbrel_lan_override("electrs"));
        std::env::remove_var("BROADCAST_POOL_UMBREL");
        std::env::remove_var("BROADCAST_POOL_LAN_IP");
        std::env::remove_var("APP_ELECTRS_NODE_IP");
    }

    #[test]
    fn normalizes_indexer_url_by_port() {
        assert_eq!(
            normalize_indexer_url("192.168.50.97:50002"),
            "ssl://192.168.50.97:50002"
        );
        assert_eq!(
            normalize_indexer_url("192.168.50.97:50001"),
            "tcp://192.168.50.97:50001"
        );
        assert_eq!(
            display_indexer_url("ssl://192.168.50.97:50002"),
            "192.168.50.97:50002"
        );
    }

    #[test]
    fn candidate_urls_try_ssl_first_on_50002() {
        let urls = candidate_urls_for_host_port("192.168.50.97", 50002);
        assert_eq!(urls[0], "ssl://192.168.50.97:50002");
        assert_eq!(urls[1], "tcp://192.168.50.97:50002");
    }

    #[test]
    fn connection_candidates_expand_stored_tcp_url() {
        let urls = connection_url_candidates("tcp://192.168.50.97:50002");
        assert_eq!(urls.len(), 2);
        assert!(urls.contains(&"ssl://192.168.50.97:50002".to_string()));
        assert!(urls.contains(&"tcp://192.168.50.97:50002".to_string()));
    }

    #[test]
    fn candidate_urls_try_tcp_and_ssl_on_both_ports() {
        let urls = candidate_urls_for_host("192.168.50.97", &INDEXER_PORTS);
        assert_eq!(urls.len(), 4);
        assert_eq!(urls[0], "tcp://192.168.50.97:50001");
        assert_eq!(urls[1], "ssl://192.168.50.97:50001");
        assert_eq!(urls[2], "tcp://192.168.50.97:50002");
        assert_eq!(urls[3], "ssl://192.168.50.97:50002");
    }

    #[test]
    fn sanitize_clears_mistaken_umbrel_lan_override() {
        std::env::set_var("BROADCAST_POOL_UMBREL", "1");
        std::env::set_var("BROADCAST_POOL_LAN_IP", "192.168.50.26");
        std::env::set_var("APP_ELECTRS_NODE_IP", "10.21.21.9");
        let mut cfg = crate::config::Config::default_config();
        cfg.indexer = Some(crate::config::IndexerConfig {
            url: "tcp://192.168.50.26:50001".to_string(),
            manual_override: true,
        });
        assert!(sanitize_umbrel_indexer_config(&mut cfg));
        assert!(cfg.indexer.is_none());
        std::env::remove_var("BROADCAST_POOL_UMBREL");
        std::env::remove_var("BROADCAST_POOL_LAN_IP");
        std::env::remove_var("APP_ELECTRS_NODE_IP");
    }
}
