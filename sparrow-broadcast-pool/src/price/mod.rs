use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const KRAKEN_URL: &str = "https://api.kraken.com/0/public/Ticker?pair=XXBTZEUR,XXBTZUSD";
const COINGECKO_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=eur,usd";
const BITSTAMP_EUR_URL: &str = "https://www.bitstamp.net/api/v2/ticker/btceur/";
const BITSTAMP_USD_URL: &str = "https://www.bitstamp.net/api/v2/ticker/btcusd/";
const CMC_URL: &str =
    "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=BTC&convert=EUR,USD";

const CACHE_TTL: Duration = Duration::from_secs(60);
const LKG_MAX_AGE: Duration = Duration::from_secs(600);
const PROVIDER_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone, Serialize)]
pub struct PriceSnapshot {
    pub prices: HashMap<String, f64>,
    pub source: String,
    pub stale: bool,
    pub fetched_at: DateTime<Utc>,
}

struct CachedEntry {
    snapshot: PriceSnapshot,
    cached_at: Instant,
}

#[derive(Clone)]
pub struct PriceFeed {
    cmc_api_key: Option<String>,
    cache: Arc<Mutex<Option<CachedEntry>>>,
    lkg: Arc<Mutex<Option<PriceSnapshot>>>,
}

impl Default for PriceFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceFeed {
    pub fn new() -> Self {
        let cmc_api_key = std::env::var("BROADCAST_POOL_CMC_API_KEY")
            .ok()
            .filter(|k| !k.is_empty());
        Self {
            cmc_api_key,
            cache: Arc::new(Mutex::new(None)),
            lkg: Arc::new(Mutex::new(None)),
        }
    }

    pub fn provider_name(&self) -> &str {
        "fallback-chain"
    }

    pub async fn fetch_btc_prices(&self) -> Result<HashMap<String, f64>> {
        Ok(self.fetch_snapshot().await?.prices)
    }

    pub async fn fetch_snapshot(&self) -> Result<PriceSnapshot> {
        if let Ok(guard) = self.cache.lock() {
            if let Some(entry) = guard.as_ref() {
                if entry.cached_at.elapsed() < CACHE_TTL {
                    return Ok(entry.snapshot.clone());
                }
            }
        }

        match self.fetch_fresh().await {
            Ok(snapshot) => {
                if let Ok(mut guard) = self.cache.lock() {
                    *guard = Some(CachedEntry {
                        snapshot: snapshot.clone(),
                        cached_at: Instant::now(),
                    });
                }
                if let Ok(mut lkg) = self.lkg.lock() {
                    *lkg = Some(snapshot.clone());
                }
                Ok(snapshot)
            }
            Err(e) => {
                tracing::warn!("All BTC price providers failed: {}", e);
                self.lkg_snapshot()
            }
        }
    }

    fn lkg_snapshot(&self) -> Result<PriceSnapshot> {
        let lkg = self
            .lkg
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .context("No cached BTC price available")?;

        let age_secs = Utc::now()
            .signed_duration_since(lkg.fetched_at)
            .num_seconds();
        if age_secs > LKG_MAX_AGE.as_secs() as i64 {
            anyhow::bail!("Cached BTC price too old ({age_secs}s)");
        }

        Ok(PriceSnapshot {
            prices: lkg.prices,
            source: lkg.source,
            stale: true,
            fetched_at: lkg.fetched_at,
        })
    }

    async fn fetch_fresh(&self) -> Result<PriceSnapshot> {
        let client = reqwest::Client::builder()
            .timeout(PROVIDER_TIMEOUT)
            .build()
            .context("Failed to build HTTP client")?;

        let mut errors = Vec::new();

        match self.fetch_kraken(&client).await {
            Ok(prices) if prices.len() >= 2 => {
                return Ok(fresh_snapshot(prices, "kraken"));
            }
            Ok(_) => errors.push("kraken: incomplete prices".to_string()),
            Err(e) => errors.push(format!("kraken: {e}")),
        }

        match self.fetch_coingecko(&client).await {
            Ok(prices) if prices.len() >= 2 => {
                return Ok(fresh_snapshot(prices, "coingecko"));
            }
            Ok(_) => errors.push("coingecko: incomplete prices".to_string()),
            Err(e) => errors.push(format!("coingecko: {e}")),
        }

        match self.fetch_bitstamp(&client).await {
            Ok(prices) if prices.len() >= 2 => {
                return Ok(fresh_snapshot(prices, "bitstamp"));
            }
            Ok(_) => errors.push("bitstamp: incomplete prices".to_string()),
            Err(e) => errors.push(format!("bitstamp: {e}")),
        }

        if let Some(ref key) = self.cmc_api_key {
            match self.fetch_coinmarketcap(&client, key).await {
                Ok(prices) if prices.len() >= 2 => {
                    return Ok(fresh_snapshot(prices, "coinmarketcap"));
                }
                Ok(_) => errors.push("coinmarketcap: incomplete prices".to_string()),
                Err(e) => errors.push(format!("coinmarketcap: {e}")),
            }
        }

        anyhow::bail!("{}", errors.join("; "))
    }

    async fn fetch_kraken(&self, client: &reqwest::Client) -> Result<HashMap<String, f64>> {
        let resp: serde_json::Value = client
            .get(KRAKEN_URL)
            .send()
            .await
            .context("Kraken request failed")?
            .error_for_status()
            .context("Kraken returned error status")?
            .json()
            .await
            .context("Kraken response parse failed")?;

        if let Some(errs) = resp.get("error").and_then(|v| v.as_array()) {
            if !errs.is_empty() {
                anyhow::bail!("Kraken API error: {:?}", errs);
            }
        }

        let result = resp
            .get("result")
            .and_then(|v| v.as_object())
            .context("Kraken missing result")?;

        let mut prices = HashMap::new();
        for (key, currency) in [("XXBTZEUR", "eur"), ("XXBTZUSD", "usd")] {
            if let Some(ticker) = result.get(key) {
                if let Some(last) = ticker
                    .get("c")
                    .and_then(|c| c.get(0))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    prices.insert(currency.to_string(), last);
                }
            }
        }
        Ok(prices)
    }

    async fn fetch_coingecko(&self, client: &reqwest::Client) -> Result<HashMap<String, f64>> {
        let resp: serde_json::Value = client
            .get(COINGECKO_URL)
            .send()
            .await
            .context("CoinGecko request failed")?
            .error_for_status()
            .context("CoinGecko returned error status")?
            .json()
            .await
            .context("CoinGecko response parse failed")?;

        let mut prices = HashMap::new();
        if let Some(btc) = resp.get("bitcoin").and_then(|v| v.as_object()) {
            for currency in ["eur", "usd"] {
                if let Some(price) = btc.get(currency).and_then(|v| v.as_f64()) {
                    prices.insert(currency.to_string(), price);
                }
            }
        }
        Ok(prices)
    }

    async fn fetch_bitstamp(&self, client: &reqwest::Client) -> Result<HashMap<String, f64>> {
        let mut prices = HashMap::new();
        for (url, currency) in [(BITSTAMP_EUR_URL, "eur"), (BITSTAMP_USD_URL, "usd")] {
            let resp: serde_json::Value = client
                .get(url)
                .send()
                .await
                .with_context(|| format!("Bitstamp {currency} request failed"))?
                .error_for_status()
                .with_context(|| format!("Bitstamp {currency} error status"))?
                .json()
                .await
                .with_context(|| format!("Bitstamp {currency} parse failed"))?;

            if let Some(last) = resp.get("last").and_then(parse_price_value) {
                prices.insert(currency.to_string(), last);
            }
        }
        Ok(prices)
    }

    async fn fetch_coinmarketcap(
        &self,
        client: &reqwest::Client,
        api_key: &str,
    ) -> Result<HashMap<String, f64>> {
        let resp: serde_json::Value = client
            .get(CMC_URL)
            .header("X-CMC_PRO_API_KEY", api_key)
            .send()
            .await
            .context("CoinMarketCap request failed")?
            .error_for_status()
            .context("CoinMarketCap returned error status")?
            .json()
            .await
            .context("CoinMarketCap response parse failed")?;

        let mut prices = HashMap::new();
        if let Some(quote) = resp.pointer("/data/BTC/quote").and_then(|v| v.as_object()) {
            for (currency, key) in [("EUR", "eur"), ("USD", "usd")] {
                if let Some(price) = quote
                    .get(currency)
                    .and_then(|v| v.get("price"))
                    .and_then(|v| v.as_f64())
                {
                    prices.insert(key.to_string(), price);
                }
            }
        }
        Ok(prices)
    }

    pub fn cached_prices(&self) -> Option<HashMap<String, f64>> {
        let guard = self.cache.lock().ok()?;
        guard.as_ref().map(|e| e.snapshot.prices.clone())
    }

    pub fn price_condition_met(current: f64, target: f64, condition: &str) -> bool {
        match condition {
            "below" => current <= target,
            _ => current >= target,
        }
    }
}

fn fresh_snapshot(prices: HashMap<String, f64>, source: &str) -> PriceSnapshot {
    PriceSnapshot {
        prices,
        source: source.to_string(),
        stale: false,
        fetched_at: Utc::now(),
    }
}

fn parse_price_value(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}
