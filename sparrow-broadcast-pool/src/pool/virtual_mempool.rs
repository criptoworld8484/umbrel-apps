use anyhow::{Context, Result};
use bitcoin::consensus::Decodable;
use bitcoin::hashes::{sha256, Hash, HashEngine};
use bitcoin::Transaction;
use std::collections::HashSet;
use std::io::Read;

/// Compute Electrum scripthash from a bitcoin Script
pub fn compute_scripthash(script: &bitcoin::Script) -> String {
    use electrum_client::ToElectrumScriptHash;
    let hash = script.to_electrum_scripthash();
    let bytes: [u8; 32] = *hash;
    hex::encode(bytes)
}

/// Electrum wire format: txid bytes reversed vs internal Bitcoin order
pub fn electrum_txid(txid: &bitcoin::Txid) -> String {
    let mut bytes = txid.to_byte_array();
    bytes.reverse();
    hex::encode(bytes)
}

/// Alternate txid representation (toggle byte order) for lookup fallbacks
pub fn alternate_txid_format(txid: &str) -> Option<String> {
    let bytes = hex::decode(txid.trim()).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut reversed = bytes;
    reversed.reverse();
    Some(hex::encode(reversed))
}

/// Compute txid from raw hex in Electrum wire format
pub fn compute_txid(tx_hex: &str) -> Result<String> {
    let tx = decode_tx(tx_hex)?;
    Ok(electrum_txid(&tx.compute_txid()))
}

/// Electrum status_hash: SHA256 of sorted "tx_hash:height:" entries
pub fn compute_status_hash(history: &[serde_json::Value]) -> Option<String> {
    if history.is_empty() {
        return None;
    }

    let mut sorted: Vec<&serde_json::Value> = history.iter().collect();
    sorted.sort_by(|a, b| {
        let height_a = a.get("height").and_then(|v| v.as_i64()).unwrap_or(0);
        let height_b = b.get("height").and_then(|v| v.as_i64()).unwrap_or(0);
        let tx_a = a.get("tx_hash").and_then(|v| v.as_str()).unwrap_or("");
        let tx_b = b.get("tx_hash").and_then(|v| v.as_str()).unwrap_or("");
        (height_a, tx_a).cmp(&(height_b, tx_b))
    });

    let mut status = String::new();
    for entry in sorted {
        let tx_hash = entry.get("tx_hash").and_then(|v| v.as_str()).unwrap_or("");
        let height = entry.get("height").and_then(|v| v.as_i64()).unwrap_or(0);
        status.push_str(&format!("{}:{}:", tx_hash, height));
    }

    let mut engine = sha256::Hash::engine();
    engine.input(status.as_bytes());
    let hash = sha256::Hash::from_engine(engine);
    Some(hex::encode(hash.as_byte_array()))
}

/// Inject pending txids into a get_history response (height 0 = mempool)
/// Add pending output value to Electrum get_balance unconfirmed field
pub fn inject_balance_unconfirmed(
    balance: serde_json::Value,
    extra_sat: u64,
) -> serde_json::Value {
    if extra_sat == 0 {
        return balance;
    }
    let confirmed = balance
        .get("confirmed")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let unconfirmed = balance
        .get("unconfirmed")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    serde_json::json!({
        "confirmed": confirmed,
        "unconfirmed": unconfirmed + extra_sat as i64,
    })
}

/// Inject pending UTXOs into listunspent (height 0 = mempool)
pub fn inject_listunspent(
    utxos: Vec<serde_json::Value>,
    pending: &[(String, u32, u64)],
) -> Vec<serde_json::Value> {
    let existing: HashSet<(String, u32)> = utxos
        .iter()
        .filter_map(|u| {
            let tx = u.get("tx_hash")?.as_str()?.to_string();
            let pos = u.get("tx_pos")?.as_u64()? as u32;
            Some((tx, pos))
        })
        .collect();

    let mut result = utxos;
    for (txid, tx_pos, value) in pending {
        if !existing.contains(&(txid.clone(), *tx_pos)) {
            result.push(serde_json::json!({
                "tx_hash": txid,
                "tx_pos": tx_pos,
                "height": 0,
                "value": value,
            }));
        }
    }
    result
}

/// Inject pending txs into get_mempool response: [fee_total, [[tx_hash, fee, height], ...]]
pub fn inject_get_mempool(
    result: serde_json::Value,
    pending_txids: &[String],
) -> serde_json::Value {
    if pending_txids.is_empty() {
        return result;
    }

    let mut parts = result.as_array().cloned().unwrap_or_else(|| {
        vec![serde_json::json!(0), serde_json::json!([])]
    });
    if parts.len() < 2 {
        parts = vec![serde_json::json!(0), serde_json::json!([])];
    }

    let fee_total = parts[0].as_i64().unwrap_or(0);
    let mut tx_list = parts[1].as_array().cloned().unwrap_or_default();
    let existing: HashSet<String> = tx_list
        .iter()
        .filter_map(|e| e.get(0).and_then(|v| v.as_str()).map(String::from))
        .collect();

    for txid in pending_txids {
        if !existing.contains(txid) {
            tx_list.push(serde_json::json!([txid, 0, 0]));
        }
    }

    serde_json::json!([fee_total, tx_list])
}

pub fn inject_in_history(
    history: Vec<serde_json::Value>,
    scripthash: &str,
    pending_txids: &[String],
) -> Vec<serde_json::Value> {
    let existing: HashSet<String> = history
        .iter()
        .filter_map(|h| h.get("tx_hash").and_then(|v| v.as_str()).map(String::from))
        .collect();

    let mut result = history;
    for txid in pending_txids {
        if !existing.contains(txid) {
            tracing::debug!(
                "Injecting tx {} into get_history for scripthash {}",
                txid,
                &scripthash[..scripthash.len().min(16)]
            );
            result.push(serde_json::json!({
                "tx_hash": txid,
                "height": 0
            }));
        }
    }
    result
}

pub fn compute_modified_status_hash(
    real_history: Vec<serde_json::Value>,
    scripthash: &str,
    pending_txids: &[String],
) -> Option<String> {
    let combined = inject_in_history(real_history, scripthash, pending_txids);
    compute_status_hash(&combined)
}

pub fn decode_tx(tx_hex: &str) -> Result<Transaction> {
    let raw = hex::decode(tx_hex.trim()).context("Invalid tx hex")?;
    let mut cursor = std::io::Cursor::new(&raw);
    Transaction::consensus_decode(&mut cursor).context("Failed to decode transaction")
}

pub fn extract_outputs(tx_hex: &str) -> Result<Vec<(u32, u64, String)>> {
    let tx = decode_tx(tx_hex)?;
    Ok(tx
        .output
        .iter()
        .enumerate()
        .map(|(i, output)| {
            (
                i as u32,
                output.value.to_sat(),
                compute_scripthash(&output.script_pubkey),
            )
        })
        .collect())
}

pub fn strip_indexer_host(url: &str) -> String {
    url.strip_prefix("tcp://")
        .or_else(|| url.strip_prefix("ssl://"))
        .unwrap_or(url)
        .to_string()
}

fn fetch_prev_tx(txid_str: &str, indexer_addr: &str) -> Result<Transaction> {
    let addr = strip_indexer_host(indexer_addr);

    let mut stream = std::net::TcpStream::connect_timeout(
        &addr.parse().context("Invalid indexer address")?,
        std::time::Duration::from_secs(5),
    )
    .context("Failed to connect to indexer")?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .ok();
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(5)))
        .ok();

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "blockchain.transaction.get",
        "params": [txid_str]
    });

    use std::io::Write;
    let mut req_bytes = serde_json::to_vec(&request)?;
    req_bytes.push(b'\n');
    stream.write_all(&req_bytes)?;

    let mut response = Vec::new();
    let mut buf = [0u8; 65536];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                response.extend_from_slice(&buf[..n]);
                if response.iter().any(|&b| b == b'\n') {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    let resp_str = String::from_utf8(response).context("Invalid UTF-8 in indexer response")?;
    let resp: serde_json::Value =
        serde_json::from_str(&resp_str).context("Failed to parse indexer response")?;

    let hex_str = resp["result"]
        .as_str()
        .context("No result in indexer response")?;

    let raw_tx = hex::decode(hex_str).context("Invalid tx hex from indexer")?;
    let mut cursor = std::io::Cursor::new(&raw_tx);
    Transaction::consensus_decode(&mut cursor).context("Failed to decode prev tx from indexer")
}

pub fn extract_output_scripthashes(tx_hex: &str) -> Result<Vec<String>> {
    let tx = decode_tx(tx_hex)?;
    Ok(tx
        .output
        .iter()
        .map(|o| compute_scripthash(&o.script_pubkey))
        .collect())
}

pub fn extract_affected_scripthashes(tx_hex: &str, indexer_addr: &str) -> Result<Vec<String>> {
    extract_affected_scripthashes_opts(tx_hex, indexer_addr, false)
}

/// When `fast` is true, only output scripthashes (no blocking prev-tx fetches to the indexer).
pub fn extract_affected_scripthashes_opts(
    tx_hex: &str,
    indexer_addr: &str,
    fast: bool,
) -> Result<Vec<String>> {
    let tx = decode_tx(tx_hex)?;
    let mut scripthashes = HashSet::new();

    for output in &tx.output {
        scripthashes.insert(compute_scripthash(&output.script_pubkey));
    }

    if fast {
        return Ok(scripthashes.into_iter().collect());
    }

    for input in &tx.input {
        let prev_txid = electrum_txid(&input.previous_output.txid);
        let vout = input.previous_output.vout as usize;

        match fetch_prev_tx(&prev_txid, indexer_addr) {
            Ok(prev_tx) => {
                if vout < prev_tx.output.len() {
                    scripthashes.insert(compute_scripthash(&prev_tx.output[vout].script_pubkey));
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Could not fetch prev tx {} for scripthash computation: {}",
                    prev_txid,
                    e
                );
            }
        }
    }

    Ok(scripthashes.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_scripthash() {
        use bitcoin::ScriptBuf;
        let script = ScriptBuf::from_hex(
            "0014d952c2c0d09d4ef2e1e3f0a1e3e5c8f2d4b6a7c8",
        )
        .unwrap();
        let sh = compute_scripthash(&script);
        assert_eq!(sh.len(), 64);
    }

    #[test]
    fn test_alternate_txid_format() {
        let a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let b = alternate_txid_format(a).unwrap();
        assert_eq!(alternate_txid_format(&b).unwrap(), a);
    }

    #[test]
    fn test_inject_get_mempool() {
        let result = serde_json::json!([0, []]);
        let injected = inject_get_mempool(result, &["abc".to_string()]);
        assert_eq!(injected[1].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_inject_in_history() {
        let history = vec![serde_json::json!({"tx_hash": "abc", "height": 100})];
        let injected = inject_in_history(history, "sh", &["def".to_string()]);
        assert_eq!(injected.len(), 2);
        assert_eq!(injected[1]["height"], 0);
    }
}
