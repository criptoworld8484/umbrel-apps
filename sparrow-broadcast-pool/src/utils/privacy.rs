use anyhow::Result;

pub fn validate_tx_hex(hex: &str) -> Result<bool> {
    let decoded = hex::decode(hex)?;
    // Basic validation: a Bitcoin transaction should be at least 10 bytes
    if decoded.len() < 10 {
        anyhow::bail!("Transaction too short to be valid");
    }
    // Check version (first 4 bytes)
    if decoded.len() >= 4 {
        let version = u32::from_le_bytes([decoded[0], decoded[1], decoded[2], decoded[3]]);
        if version == 0 {
            // Version 0 is used for specific cases but generally invalid for standard txs
            tracing::warn!("Transaction has version 0, which is unusual");
        }
    }
    Ok(true)
}

pub fn estimate_tx_size(vin_count: usize, vout_count: usize) -> usize {
    // Rough estimation: base size + inputs + outputs
    // Each input: ~148 bytes (legacy) or ~68 bytes (segwit)
    // Each output: ~34 bytes
    // Base: ~10 bytes
    let base = 10;
    let input_size = vin_count * 148; // Conservative estimate
    let output_size = vout_count * 34;
    base + input_size + output_size
}

pub fn calculate_fee_rate(fee_sat: u64, tx_size_bytes: usize) -> f64 {
    if tx_size_bytes == 0 {
        return 0.0;
    }
    fee_sat as f64 / tx_size_bytes as f64
}
