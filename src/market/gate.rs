use ccxt_base::{exchange::ExchangeRuntime, exchange_generated::ExchangeBase, runtime, Value};
use ccxt_pro::{pro::gate::GateCore, types::OrderBook};
use std::collections::HashSet;

/// One Gate core owns all Gate subscriptions because CCXT shares websocket
/// clients process-wide by URL while each core keeps its own local books.
pub struct GateFeed {
    core: GateCore,
    url: Value,
    hashes: Value,
    topics: Value,
    subscribed: HashSet<String>,
}

impl Drop for GateFeed {
    fn drop(&mut self) {
        if let Value::Str(url) = &self.url {
            // CCXT keeps websocket clients in a process-wide registry.
            ccxt_base::pro::ws_client::drop_client(url);
        }
    }
}

impl GateFeed {
    pub async fn load_markets() -> Result<(GateCore, Vec<ccxt_pro::types::Market>), String> {
        let mut core = GateCore::new(Some(Value::from_json(&serde_json::json!({
            "options": {
                "defaultType": "spot",
                "fetchCurrencies": false,
                "fetchMarkets": {"types": ["spot"]}
            }
        }))));
        runtime::call_typed(core.load_markets(&[Value::Bool(false), Value::Null]))
            .await
            .map_err(|e| e.to_string())?;
        let markets = Self::catalogue(&core)?;
        Ok((core, markets))
    }

    pub async fn reload_markets(&mut self) -> Result<Vec<ccxt_pro::types::Market>, String> {
        runtime::call_typed(self.core.load_markets(&[Value::Bool(true), Value::Null]))
            .await
            .map_err(|e| e.to_string())?;
        Self::catalogue(&self.core)
    }

    fn catalogue(core: &GateCore) -> Result<Vec<ccxt_pro::types::Market>, String> {
        let values: Vec<Value> = match &core.markets {
            Value::Dict(markets) => markets.values().cloned().collect(),
            _ => return Err("Gate returned no market catalogue".into()),
        };
        Ok(values
            .into_iter()
            .map(ccxt_pro::types::Market::from_value)
            .collect())
    }

    pub fn new(core: GateCore, symbols: &[String]) -> Result<Self, String> {
        let mut feed = Self {
            core,
            url: Value::Null,
            hashes: Value::Null,
            topics: Value::Null,
            subscribed: HashSet::new(),
        };
        feed.set_symbols(symbols)?;
        Ok(feed)
    }

    pub fn set_symbols(&mut self, symbols: &[String]) -> Result<(), String> {
        let mut url = Value::Null;
        let mut hashes = Vec::with_capacity(symbols.len());
        let mut new_topics = Vec::new();
        let active: HashSet<_> = symbols.iter().cloned().collect();
        self.subscribed.retain(|symbol| active.contains(symbol));
        for symbol in symbols {
            let market = ExchangeBase::market(&self.core, Value::from(symbol.as_str()));
            let market_id = runtime::get_value(&market, &Value::from("id"));
            let market_id = match market_id {
                Value::Str(id) => id.to_string(),
                _ => return Err(format!("Gate market {symbol} is unavailable")),
            };
            let market_url = self.core.get_url_by_market(market);
            if url != Value::Null && url != market_url {
                return Err("Gate markets resolved to different websocket endpoints".into());
            }
            url = market_url;
            hashes.push(Value::from(format!("orderbook:{symbol}")));
            if self.subscribed.insert(symbol.clone()) {
                new_topics.push(Value::from(format!("ob.{market_id}.50")));
            }
        }

        self.url = url;
        self.hashes = Value::from(hashes);
        self.topics = Value::from(new_topics);
        Ok(())
    }

    pub fn reset_subscriptions(&mut self, symbols: &[String]) -> Result<(), String> {
        if let Value::Str(url) = &self.url {
            ccxt_base::pro::ws_client::drop_client(url);
        }
        self.subscribed.clear();
        self.set_symbols(symbols)
    }

    pub async fn next_book(&mut self) -> Result<OrderBook, String> {
        let value = runtime::call_typed(self.core.subscribe_public_multiple(
            self.url.clone(),
            self.hashes.clone(),
            self.topics.clone(),
            Value::from("spot.obu"),
            &[Value::from_json(&serde_json::json!({}))],
        ))
        .await
        .map_err(|e| e.to_string())?;
        let book = OrderBook::from_value(value);
        if book.symbol.is_none() {
            return Err("Gate returned an order book without a symbol".into());
        }
        Ok(book)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ccxt_base::pro::ws_client::{mock_inject, mock_setup};
    use serde_json::json;

    const URL: &str = "wss://api.gateio.ws/ws/v4/";

    fn core() -> GateCore {
        let mut core = GateCore::new(None);
        let btc = json!({"id":"BTC_USDT","symbol":"BTC/USDT","base":"BTC","quote":"USDT","type":"spot","spot":true,"contract":false,"active":true});
        let eth = json!({"id":"ETH_USDT","symbol":"ETH/USDT","base":"ETH","quote":"USDT","type":"spot","spot":true,"contract":false,"active":true});
        core.markets = Value::from_json(&json!({"BTC/USDT":btc.clone(),"ETH/USDT":eth.clone()}));
        core.markets_by_id = Value::from_json(&json!({"BTC_USDT":[btc],"ETH_USDT":[eth]}));
        core
    }

    fn inject(
        symbol: &str,
        full: bool,
        sequence: i64,
        bids: serde_json::Value,
        asks: serde_json::Value,
    ) {
        mock_inject(
            URL,
            Value::from_json(&json!({
                "channel":"spot.obu","event":"update",
                "result":{"full":full,"s":format!("ob.{symbol}.50"),"u":sequence,"t":1700000000000i64+sequence,"b":bids,"a":asks}
            })),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_gate_core_keeps_both_books_synchronized() {
        mock_setup(URL);
        let mut feed = GateFeed::new(core(), &["BTC/USDT".into(), "ETH/USDT".into()]).unwrap();

        inject(
            "BTC_USDT",
            true,
            1,
            json!([["99", "1"]]),
            json!([["101", "1"]]),
        );
        assert_eq!(feed.next_book().await.unwrap().bids[0][0], 99.0);
        inject("BTC_USDT", false, 2, json!([["100", "2"]]), json!([]));
        inject(
            "ETH_USDT",
            true,
            1,
            json!([["199", "1"]]),
            json!([["201", "1"]]),
        );
        let _ = feed.next_book().await.unwrap();
        inject("BTC_USDT", false, 3, json!([]), json!([["102", "1"]]));

        let mut btc = feed.next_book().await.unwrap();
        for _ in 0..2 {
            if btc.symbol.as_deref() == Some("BTC/USDT") && btc.nonce == Some(3) {
                break;
            }
            btc = feed.next_book().await.unwrap();
        }
        assert_eq!(btc.symbol.as_deref(), Some("BTC/USDT"));
        assert_eq!(btc.bids[0][0], 100.0);
        assert_eq!(btc.nonce, Some(3));
    }

    #[tokio::test]
    #[ignore = "live Gate public websocket smoke test"]
    async fn live_gate_stream_updates_two_books_on_one_connection() {
        let (core, markets) = GateFeed::load_markets().await.unwrap();
        let selected: Vec<_> = ["BTC/USDT", "ETH/USDT"]
            .into_iter()
            .map(|symbol| {
                markets
                    .iter()
                    .find(|market| market.symbol == symbol && market.spot && market.active)
                    .unwrap_or_else(|| panic!("Gate spot market {symbol} is unavailable"))
                    .symbol
                    .clone()
            })
            .collect();
        let mut feed = GateFeed::new(core, &selected).unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut seen = std::collections::HashSet::new();
        while seen.len() < selected.len() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let book = tokio::time::timeout(remaining, feed.next_book())
                .await
                .expect("Gate book update timed out")
                .unwrap();
            assert!(book.bids[0][0] > 0.0 && book.bids[0][0] <= book.asks[0][0]);
            seen.insert(book.symbol.unwrap());
        }
        assert_eq!(seen.len(), 2);
    }
}
