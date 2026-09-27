mod gate;

use crate::config::Config;
use ccxt_pro::{
    types::{Market, OrderBook},
    Binance, Bitget, Bybit, Okx, Params,
};
use futures_util::FutureExt;
use rust_decimal::Decimal;
use std::{any::Any, collections::HashMap, panic::AssertUnwindSafe, str::FromStr, time::Instant};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct MarketInfo {
    pub symbol: String,
    pub base: String,
    pub display_base: String,
    pub quote: String,
    pub pair_label: String,
}

#[derive(Clone)]
pub struct BookUpdate {
    pub venue: String,
    pub market: MarketInfo,
    pub quote: Result<(Decimal, Decimal), String>,
    pub received_at: Instant,
}

pub enum FeedEvent {
    Markets {
        venue: String,
        markets: Vec<MarketInfo>,
    },
    Book(BookUpdate),
    Offline {
        venue: String,
        reason: String,
    },
}

enum Client {
    Binance(Binance),
    Okx(Okx),
    Bitget(Bitget),
    Bybit(Bybit),
    Gate(Box<gate::GateFeed>),
}

fn public_spot_config() -> Option<ccxt_pro::Value> {
    Some(ccxt_pro::Value::from_json(&serde_json::json!({
        "options": {
            "defaultType": "spot",
            "fetchCurrencies": false,
            "fetchMarkets": {"types": ["spot"]}
        }
    })))
}

pub struct VenueFeed {
    pub venue: String,
    client: Client,
    markets: HashMap<String, MarketInfo>,
    symbols: Vec<String>,
    config: Config,
}

pub fn supported(id: &str) -> bool {
    matches!(id, "binance" | "okx" | "bitget" | "bybit" | "gate")
}

fn market_info(markets: Vec<Market>, config: &Config) -> Vec<MarketInfo> {
    let stablecoins: HashMap<_, _> = config
        .stablecoins
        .iter()
        .map(|name| (name.to_ascii_uppercase(), name.as_str()))
        .collect();
    markets
        .into_iter()
        .filter_map(|market| {
            let base = market.base.to_ascii_uppercase();
            let quote = market.quote.to_ascii_uppercase();
            if !market.spot
                || !market.active
                || market.contract
                || base == quote
                || base == "USDT"
                || !stablecoins.contains_key(&base)
                || (quote != "USDT" && quote != "USDC")
            {
                return None;
            }
            let display_base = stablecoins[&base].to_string();
            Some(MarketInfo {
                symbol: market.symbol,
                pair_label: format!("{display_base} / {quote}"),
                display_base,
                base,
                quote,
            })
        })
        .collect()
}

impl VenueFeed {
    pub async fn connect(venue: &str, config: &Config) -> Result<Self, String> {
        let (client, markets) = match venue {
            "binance" => {
                let mut api = Binance::new(public_spot_config());
                api.set_timeout_ms(15_000);
                let markets = api
                    .try_load_markets(false)
                    .await
                    .map_err(|e| e.to_string())?;
                (Client::Binance(api), markets)
            }
            "okx" => {
                let mut api = Okx::new(public_spot_config());
                api.set_timeout_ms(15_000);
                let markets = api
                    .try_load_markets(false)
                    .await
                    .map_err(|e| e.to_string())?;
                (Client::Okx(api), markets)
            }
            "bitget" => {
                let mut api = Bitget::new(public_spot_config());
                api.set_timeout_ms(15_000);
                let markets = api
                    .try_load_markets(false)
                    .await
                    .map_err(|e| e.to_string())?;
                (Client::Bitget(api), markets)
            }
            "bybit" => {
                let mut api = Bybit::new(public_spot_config());
                api.set_timeout_ms(15_000);
                let markets = api
                    .try_load_markets(false)
                    .await
                    .map_err(|e| e.to_string())?;
                (Client::Bybit(api), markets)
            }
            "gate" => {
                let (core, markets) = gate::GateFeed::load_markets().await?;
                let markets = market_info(markets, config);
                if markets.is_empty() {
                    return Err("no matching active spot markets".into());
                }
                let symbols: Vec<_> = markets.iter().map(|market| market.symbol.clone()).collect();
                let client = gate::GateFeed::new(core, &symbols)?;
                return Ok(Self::from_parts(
                    venue,
                    Client::Gate(Box::new(client)),
                    markets,
                    config,
                ));
            }
            _ => return Err(format!("unsupported exchange id: {venue}")),
        };
        let markets = market_info(markets, config);
        if markets.is_empty() {
            return Err("no matching active spot markets".into());
        }
        Ok(Self::from_parts(venue, client, markets, config))
    }

    fn from_parts(venue: &str, client: Client, markets: Vec<MarketInfo>, config: &Config) -> Self {
        let symbols: Vec<_> = markets.iter().map(|market| market.symbol.clone()).collect();
        let markets = markets
            .into_iter()
            .map(|market| (market.symbol.clone(), market))
            .collect();
        Self {
            venue: venue.into(),
            client,
            markets,
            symbols,
            config: config.clone(),
        }
    }

    pub fn markets(&self) -> Vec<MarketInfo> {
        self.markets.values().cloned().collect()
    }

    pub async fn next(&mut self) -> Result<BookUpdate, String> {
        if self.symbols.is_empty() {
            return Err("no matching active spot markets".into());
        }
        loop {
            let book = match &mut self.client {
                Client::Binance(api) => api
                    .watch_order_book_for_symbols(self.symbols.clone(), None, Params::none())
                    .await
                    .map_err(|e| e.to_string())?,
                Client::Okx(api) => api
                    .watch_order_book_for_symbols(self.symbols.clone(), None, Params::none())
                    .await
                    .map_err(|e| e.to_string())?,
                Client::Bitget(api) => api
                    .watch_order_book_for_symbols(self.symbols.clone(), None, Params::none())
                    .await
                    .map_err(|e| e.to_string())?,
                Client::Bybit(api) => api
                    .watch_order_book_for_symbols(self.symbols.clone(), None, Params::none())
                    .await
                    .map_err(|e| e.to_string())?,
                Client::Gate(feed) => feed.next_book().await?,
            };
            let symbol = book
                .symbol
                .as_deref()
                .ok_or("CCXT returned a book without a symbol")?;
            let Some(market) = self.markets.get(symbol).cloned() else {
                continue;
            };
            return Ok(BookUpdate {
                venue: self.venue.clone(),
                market,
                quote: bbo(&book),
                received_at: Instant::now(),
            });
        }
    }

    pub async fn refresh_markets(&mut self) -> Result<bool, String> {
        let all = match &mut self.client {
            Client::Binance(api) => api
                .try_load_markets(true)
                .await
                .map_err(|e| e.to_string())?,
            Client::Okx(api) => api
                .try_load_markets(true)
                .await
                .map_err(|e| e.to_string())?,
            Client::Bitget(api) => api
                .try_load_markets(true)
                .await
                .map_err(|e| e.to_string())?,
            Client::Bybit(api) => api
                .try_load_markets(true)
                .await
                .map_err(|e| e.to_string())?,
            Client::Gate(feed) => feed.reload_markets().await?,
        };
        let selected = market_info(all, &self.config);
        let symbols: Vec<_> = selected
            .iter()
            .map(|market| market.symbol.clone())
            .collect();
        let changed = symbols != self.symbols;
        if changed {
            if let Client::Gate(feed) = &mut self.client {
                feed.set_symbols(&symbols)?;
            }
        }
        self.symbols = symbols;
        self.markets = selected
            .into_iter()
            .map(|market| (market.symbol.clone(), market))
            .collect();
        if changed {
            eprintln!("[market] {} market list refreshed", self.venue);
        }
        Ok(changed)
    }

    pub fn reset_after_disconnect(&mut self) -> Result<(), String> {
        if let Client::Gate(feed) = &mut self.client {
            feed.reset_subscriptions(&self.symbols)?;
        }
        Ok(())
    }
}

fn bbo(book: &OrderBook) -> Result<(Decimal, Decimal), String> {
    let bid = book.bids.first().ok_or("empty bids")?[0];
    let ask = book.asks.first().ok_or("empty asks")?[0];
    let bid = Decimal::from_str(&bid.to_string()).map_err(|e| e.to_string())?;
    let ask = Decimal::from_str(&ask.to_string()).map_err(|e| e.to_string())?;
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO || bid > ask {
        return Err(format!("invalid best bid/ask: {bid}/{ask}"));
    }
    Ok((bid, ask))
}

pub async fn run_venue(venue: String, config: Config, tx: mpsc::Sender<FeedEvent>) {
    let mut backoff = 1_u64;
    let mut feed = loop {
        let result = match AssertUnwindSafe(VenueFeed::connect(&venue, &config))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(payload) => Err(panic_reason(payload)),
        };
        match result {
            Ok(feed) => break feed,
            Err(reason) => {
                let _ = tx
                    .send(FeedEvent::Offline {
                        venue: venue.clone(),
                        reason,
                    })
                    .await;
                tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
            }
        }
    };
    let _ = tx
        .send(FeedEvent::Markets {
            venue: venue.clone(),
            markets: feed.markets(),
        })
        .await;
    eprintln!(
        "[market] {venue}: discovered {} eligible spot markets; connecting",
        feed.symbols.len()
    );
    let mut live = false;
    let mut queue_full_logged = false;
    let mut refresh_at =
        tokio::time::Instant::now() + std::time::Duration::from_secs(config.market_refresh_seconds);
    loop {
        let next = tokio::select! {
            result = AssertUnwindSafe(feed.next()).catch_unwind() => Some(result),
            _ = tokio::time::sleep_until(refresh_at) => None,
        };
        match next {
            Some(Ok(Ok(book))) => {
                if !live {
                    eprintln!("[market] {venue}: live L2 updates received");
                    live = true;
                }
                match tx.try_send(FeedEvent::Book(book)) {
                    Ok(()) => queue_full_logged = false,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // ponytail: drop intermediate snapshots under pressure; CCXT retains the full book. Increase buffering only if this log appears.
                        if !queue_full_logged {
                            eprintln!("[market] {venue} update queue full; dropping snapshots");
                            queue_full_logged = true;
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                }
                backoff = 1;
            }
            Some(Ok(Err(reason))) => {
                live = false;
                let _ = tx
                    .send(FeedEvent::Offline {
                        venue: venue.clone(),
                        reason,
                    })
                    .await;
                tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
                if let Err(error) = feed.reset_after_disconnect() {
                    eprintln!("[market] {venue} subscription reset failed: {error}");
                }
            }
            Some(Err(payload)) => {
                live = false;
                let _ = tx
                    .send(FeedEvent::Offline {
                        venue: venue.clone(),
                        reason: panic_reason(payload),
                    })
                    .await;
                tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
                if let Err(error) = feed.reset_after_disconnect() {
                    eprintln!("[market] {venue} subscription reset failed: {error}");
                }
            }
            None => {
                let result = match AssertUnwindSafe(feed.refresh_markets())
                    .catch_unwind()
                    .await
                {
                    Ok(result) => result,
                    Err(payload) => Err(panic_reason(payload)),
                };
                match result {
                    Ok(changed) => {
                        let _ = tx
                            .send(FeedEvent::Markets {
                                venue: venue.clone(),
                                markets: feed.markets(),
                            })
                            .await;
                        if changed {
                            backoff = 1;
                        }
                    }
                    Err(reason) => {
                        eprintln!("[market] {venue} market refresh failed: {reason}");
                        backoff = (backoff * 2).min(60);
                    }
                }
                refresh_at = tokio::time::Instant::now()
                    + std::time::Duration::from_secs(if backoff > 1 {
                        backoff
                    } else {
                        config.market_refresh_seconds
                    });
            }
        }
    }
}

fn panic_reason(payload: Box<dyn Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_else(|| "CCXT operation panicked".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn only_active_stablecoin_spot_pairs_are_selected() {
        let market = |base: &str, quote: &str, spot: bool, contract: bool, active: bool| Market {
            base: base.into(),
            quote: quote.into(),
            symbol: format!("{base}/{quote}"),
            spot,
            contract,
            active,
            ..Market::default()
        };
        let config = Config::default();
        let selected = market_info(
            vec![
                market("USDC", "USDT", true, false, true),
                market("USDe", "USDC", true, false, true),
                market("USDT", "USDC", true, false, true),
                market("USDe", "USDT", false, true, true),
                market("DAI", "USDT", true, false, false),
                market("BTC", "USDT", true, false, true),
            ],
            &config,
        );
        assert_eq!(
            selected
                .iter()
                .map(|m| m.symbol.as_str())
                .collect::<Vec<_>>(),
            ["USDC/USDT", "USDe/USDC"]
        );
    }

    #[test]
    fn crossed_or_empty_books_are_rejected() {
        let crossed = OrderBook {
            bids: vec![[1.001, 1.0]],
            asks: vec![[0.999, 1.0]],
            ..OrderBook::default()
        };
        assert!(bbo(&crossed).is_err());
        assert!(bbo(&OrderBook::default()).is_err());
        let non_finite = OrderBook {
            bids: vec![[f64::NAN, 1.0]],
            asks: vec![[1.0, 1.0]],
            ..OrderBook::default()
        };
        assert!(bbo(&non_finite).is_err());
    }

    async fn live_smoke(venue: &str, config: Config) -> Result<String, String> {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            AssertUnwindSafe(VenueFeed::connect(venue, &config)).catch_unwind(),
        )
        .await
        .map_err(|_| format!("{venue}: market discovery timed out"))?;
        let mut feed = match result {
            Ok(Ok(feed)) => feed,
            Ok(Err(reason)) => return Err(format!("{venue}: {reason}")),
            Err(payload) => return Err(format!("{venue}: {}", panic_reason(payload))),
        };
        if feed.symbols.len() < 2 {
            return Err(format!(
                "{venue}: only {} matching markets",
                feed.symbols.len()
            ));
        }
        let markets = feed.markets();
        let has_anchor = markets
            .iter()
            .any(|market| market.base == "USDC" && market.quote == "USDT");
        let has_usdc_quote = markets
            .iter()
            .any(|market| market.quote == "USDC" && market.base != "USDC");
        let mut required = Vec::new();
        if let Some(anchor) = markets
            .iter()
            .find(|market| market.base == "USDC" && market.quote == "USDT")
        {
            required.push(anchor.symbol.clone());
        }
        if let Some(quoted) = markets
            .iter()
            .find(|market| market.quote == "USDC" && market.base != "USDC")
        {
            if !required.contains(&quoted.symbol) {
                required.push(quoted.symbol.clone());
            }
        }
        for market in &markets {
            if required.len() == 2 {
                break;
            }
            if !required.contains(&market.symbol) {
                required.push(market.symbol.clone());
            }
        }
        let required: HashSet<_> = required.into_iter().collect();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(25);
        let mut seen = HashSet::new();
        while seen.len() < required.len() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let update = tokio::time::timeout(remaining, feed.next())
                .await
                .map_err(|_| format!("{venue}: two-market websocket smoke timed out"))??;
            if required.contains(&update.market.symbol) {
                seen.insert(update.market.symbol);
            }
        }
        Ok(format!(
            "{venue}: {} markets, two L2 streams live, USDC/USDT anchor={has_anchor}, USDC quote={has_usdc_quote}",
            feed.symbols.len()
        ))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "one-shot smoke test using the configured local network"]
    async fn live_five_exchange_two_market_smoke() {
        let config = Config::default();
        let mut failures = Vec::new();
        let venues = std::env::var("DEPEG_SMOKE_EXCHANGES")
            .unwrap_or_else(|_| "binance,okx,bitget,bybit,gate".into());
        for venue in venues.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let venue = venue.to_string();
            let label = venue.clone();
            let config = config.clone();
            match tokio::spawn(async move { live_smoke(&venue, config).await }).await {
                Ok(Ok(summary)) => println!("{summary}"),
                Ok(Err(reason)) => {
                    println!("{reason}");
                    failures.push(reason);
                }
                Err(error) => failures.push(format!("{label}: smoke task failed: {error}")),
            }
        }
        assert!(failures.is_empty(), "live smoke failures: {failures:?}");
    }
}
