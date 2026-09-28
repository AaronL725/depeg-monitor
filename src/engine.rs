use crate::{
    alert::{escape_html, Delivery, DeliveryAck, Notification},
    config::{Config, MonitorSettings},
    market::{BookUpdate, FeedEvent, MarketInfo},
    state::{IncidentState, StateFile},
};
use chrono::Utc;
use rust_decimal::Decimal;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

#[derive(Default)]
struct Incident {
    confirmation_since: Option<Instant>,
    recovery_since: Option<Instant>,
    notified: bool,
    last_sent_at: Option<chrono::DateTime<Utc>>,
    next_retry_at: Option<Instant>,
    pending: bool,
    permanently_failed: bool,
}

#[derive(Clone, Copy)]
struct EvaluationTime {
    now: Instant,
    sampled_at: Instant,
}

impl From<IncidentState> for Incident {
    fn from(value: IncidentState) -> Self {
        Self {
            notified: value.notified,
            last_sent_at: value.last_sent_at,
            ..Self::default()
        }
    }
}

pub struct Engine {
    config: Config,
    markets: HashMap<String, MarketInfo>,
    books: HashMap<String, BookUpdate>,
    catalogued: HashSet<String>,
    offline: HashSet<String>,
    stale: HashSet<String>,
    incidents: HashMap<String, Incident>,
    state: StateFile,
    queue: mpsc::Sender<Notification>,
    status: watch::Sender<String>,
    authorized_chats: watch::Receiver<Vec<i64>>,
    queue_full_logged: bool,
}

impl Engine {
    pub fn new(
        config: Config,
        state: StateFile,
        queue: mpsc::Sender<Notification>,
        status: watch::Sender<String>,
        authorized_chats: watch::Receiver<Vec<i64>>,
    ) -> Self {
        let engine = Self {
            config,
            markets: HashMap::new(),
            books: HashMap::new(),
            catalogued: HashSet::new(),
            offline: HashSet::new(),
            stale: HashSet::new(),
            incidents: HashMap::new(),
            state,
            queue,
            status,
            authorized_chats,
            queue_full_logged: false,
        };
        engine.publish_status();
        engine
    }

    pub async fn run(
        mut self,
        mut feeds: mpsc::Receiver<FeedEvent>,
        mut acknowledgements: mpsc::Receiver<DeliveryAck>,
        mut shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(self.config.evaluation_interval_ms));
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                event = feeds.recv() => match event {
                    Some(FeedEvent::Settings(settings)) => self.apply_monitor_settings(settings),
                    Some(FeedEvent::Markets { venue, markets }) => self.set_markets(&venue, markets),
                    Some(FeedEvent::Book(book)) => self.update_book(book),
                    Some(FeedEvent::Offline { venue, reason }) => {
                        self.offline.insert(venue.clone());
                        self.pause_venue(&venue);
                        eprintln!("[market] {venue} offline: {reason}");
                    }
                    None => return,
                },
                ack = acknowledgements.recv() => match ack {
                    Some(ack) => self.delivery_ack(ack),
                    None => return,
                },
                _ = interval.tick() => self.evaluate(),
            }
            self.publish_status();
        }
    }

    fn status_text(&self) -> String {
        let now = Instant::now();
        let max_age = Duration::from_secs(self.config.max_quote_age_seconds);
        let mut lines = vec!["📊 <b>MONITOR STATUS</b>".to_string()];
        for venue in &self.config.exchanges {
            let prefix = format!("{venue}|");
            let mut markets: Vec<_> = self
                .markets
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .collect();
            markets.sort_by_key(|(_, market)| market.display_base.as_str());
            let total = markets.len();
            let mut fresh = 0;
            let mut received = false;
            let mut market_status = Vec::with_capacity(total);
            for (key, market) in markets {
                let detail = if self.offline.contains(venue) {
                    "offline".to_string()
                } else if let Some(book) = self.books.get(key) {
                    received = true;
                    let age = now.duration_since(book.received_at);
                    if book.quote.is_err() {
                        format!("invalid {}s", age.as_secs())
                    } else if age > max_age {
                        format!("no update {}s", age.as_secs())
                    } else {
                        fresh += 1;
                        format!("{}s", age.as_secs())
                    }
                } else {
                    "waiting for L2".to_string()
                };
                market_status.push(format!(
                    "{} / USDT: {detail}",
                    escape_html(&market.display_base)
                ));
            }
            let state = if self.offline.contains(venue) {
                "🔴 reconnecting"
            } else if total == 0 {
                "⚪ waiting for markets"
            } else if fresh == total {
                "🟢 live"
            } else if fresh > 0 {
                "🟡 partial"
            } else if received {
                "🟠 no fresh L2"
            } else {
                "🟡 waiting for L2"
            };
            let coverage = if total == 0 {
                "0 spot pairs".to_string()
            } else {
                format!("{fresh}/{total} L2 fresh")
            };
            lines.push(format!(
                "{state} <b>{}</b> — {coverage}",
                exchange_name(venue)
            ));
            if total > 0 {
                lines.push(format!("  {}", market_status.join(" · ")));
            }
        }
        lines.join("\n")
    }

    fn publish_status(&self) {
        self.status.send_replace(self.status_text());
    }

    fn set_markets(&mut self, venue: &str, markets: Vec<MarketInfo>) {
        self.catalogued.insert(venue.into());
        if markets.is_empty() {
            self.offline.remove(venue);
        }
        let count = markets.len();
        let symbols = markets
            .iter()
            .map(|market| market.symbol.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let venue_prefix = format!("{venue}|");
        let retained: HashSet<_> = markets
            .iter()
            .map(|market| market_key(venue, &market.symbol))
            .collect();
        let removed: Vec<_> = self
            .markets
            .keys()
            .filter(|key| key.starts_with(&venue_prefix) && !retained.contains(*key))
            .cloned()
            .collect();
        if let Err(error) = self.state.retain_markets(venue, &retained) {
            eprintln!("[state] delisted market cleanup failed: {error}");
        }
        for key in removed {
            self.books.remove(&key);
            self.stale.remove(&key);
            self.incidents.remove(&event_key(&key));
        }
        self.markets
            .retain(|key, _| !key.starts_with(&venue_prefix));
        for market in markets {
            self.markets
                .insert(market_key(venue, &market.symbol), market);
        }
        eprintln!("[market] {venue}: {count} stablecoin spot markets: {symbols}");
    }

    fn apply_monitor_settings(&mut self, settings: MonitorSettings) {
        let mut venues: HashSet<_> = self.config.exchanges.iter().cloned().collect();
        venues.extend(settings.exchanges.iter().cloned());
        self.config.exchanges = settings.exchanges;
        self.config.stablecoins = settings.stablecoins;

        for venue in venues {
            let prefix = format!("{venue}|");
            let markets = if self.config.exchanges.contains(&venue) {
                self.markets
                    .iter()
                    .filter(|(key, market)| {
                        key.starts_with(&prefix)
                            && self
                                .config
                                .stablecoins
                                .iter()
                                .any(|coin| coin.eq_ignore_ascii_case(&market.display_base))
                    })
                    .map(|(_, market)| market.clone())
                    .collect()
            } else {
                Vec::new()
            };
            self.set_markets(&venue, markets);
            if self.config.exchanges.contains(&venue) {
                self.offline.insert(venue.clone());
                self.pause_venue(&venue);
            } else {
                self.offline.remove(&venue);
            }
        }
    }

    fn pause_venue(&mut self, venue: &str) {
        let venue_prefix = format!("{venue}|");
        let keys: Vec<_> = self
            .markets
            .keys()
            .filter(|key| key.starts_with(&venue_prefix))
            .cloned()
            .collect();
        for key in keys {
            self.stale.insert(key.clone());
            self.books.remove(&key);
            self.pause_incident(&key);
        }
    }

    fn update_book(&mut self, book: BookUpdate) {
        let key = market_key(&book.venue, &book.market.symbol);
        if !self.markets.contains_key(&key) {
            return;
        }
        self.offline.remove(&book.venue);
        match &book.quote {
            Ok(_) => {
                if self.stale.remove(&key) {
                    eprintln!(
                        "[market] {} {} L2 updates resumed",
                        book.venue, book.market.symbol
                    );
                }
            }
            Err(reason) => {
                self.pause_incident(&key);
                if self.stale.insert(key.clone()) {
                    eprintln!(
                        "[market] {} {} invalid: {reason}",
                        book.venue, book.market.symbol
                    );
                }
            }
        }
        self.books.insert(key, book);
    }

    fn evaluate(&mut self) {
        if self.queue.capacity() > self.config.notification_queue_capacity / 2 {
            self.queue_full_logged = false;
        }
        let now = Instant::now();
        let fresh = Duration::from_secs(self.config.max_quote_age_seconds);
        let monitored: Vec<_> = self
            .markets
            .iter()
            .map(|(key, market)| (key.clone(), market.clone()))
            .collect();
        for (key, market) in monitored {
            let Some((venue, quote, received_at)) = self
                .books
                .get(&key)
                .map(|book| (book.venue.clone(), book.quote.clone(), book.received_at))
            else {
                self.pause_incident(&key);
                continue;
            };
            let Ok((_, sell_price)) = quote else {
                self.pause_incident(&key);
                continue;
            };
            if now.duration_since(received_at) > fresh {
                if self.stale.insert(key.clone()) {
                    eprintln!(
                        "[market] {venue} {} no L2 update for {}s; alert paused",
                        market.symbol,
                        now.duration_since(received_at).as_secs()
                    );
                }
                self.pause_incident(&key);
                continue;
            }
            self.evaluate_depeg(
                &key,
                &market,
                &venue,
                sell_price,
                EvaluationTime {
                    now,
                    sampled_at: received_at,
                },
            );
        }
    }

    fn pause_incident(&mut self, key: &str) {
        let id = event_key(key);
        if let Some(incident) = self.incidents.get_mut(&id) {
            incident.confirmation_since = None;
            incident.recovery_since = None;
        }
    }

    fn evaluate_depeg(
        &mut self,
        source_key: &str,
        market: &MarketInfo,
        venue: &str,
        price: Decimal,
        time: EvaluationTime,
    ) {
        let EvaluationTime { now, sampled_at } = time;
        let Some(depeg_bps) = Decimal::ONE
            .checked_sub(price)
            .and_then(|deviation| deviation.checked_mul(Decimal::from(10_000)))
        else {
            self.pause_incident(source_key);
            return;
        };
        let depeg_bps = depeg_bps.max(Decimal::ZERO);
        let triggered = depeg_bps >= Decimal::from(self.config.depeg_bps);
        let recovered = depeg_bps < Decimal::from(self.config.recovery_bps);
        let id = event_key(source_key);
        let incident = self
            .incidents
            .entry(id.clone())
            .or_insert_with(|| self.state.get(&id).into());

        if recovered {
            incident.confirmation_since = None;
            if !incident.notified
                && !incident.pending
                && !incident.permanently_failed
                && incident.next_retry_at.is_none()
                && incident.recovery_since.is_none()
            {
                incident.recovery_since = None;
                return;
            }
            let since = incident.recovery_since.get_or_insert(now);
            if !incident.pending
                && now.duration_since(*since) >= Duration::from_secs(self.config.recovery_seconds)
            {
                *incident = Incident::default();
                if let Err(error) = self.state.set(id, IncidentState::default()) {
                    eprintln!("[state] recovery save failed: {error}");
                }
            }
            return;
        }
        incident.recovery_since = None;
        if triggered {
            incident.confirmation_since.get_or_insert(now);
        } else {
            incident.confirmation_since = None;
        }
        let confirmed = incident.confirmation_since.is_some_and(|since| {
            now.duration_since(since) >= Duration::from_secs(self.config.confirmation_seconds)
        });
        let reminder_due = incident.notified
            && incident.last_sent_at.is_some_and(|sent| {
                (Utc::now() - sent).to_std().unwrap_or_default()
                    >= Duration::from_secs(self.config.reminder_seconds)
            });
        if !confirmed || (incident.notified && !reminder_due) {
            return;
        }
        if self.authorized_chats.borrow().is_empty() {
            return;
        }
        if incident.pending
            || incident.permanently_failed
            || incident.next_retry_at.is_some_and(|retry| now < retry)
        {
            return;
        }

        let message = Notification {
            key: id,
            base: market.display_base.clone(),
            exchange: exchange_name(venue),
            price,
            confirmed_at: Utc::now(),
            sampled_at,
        };
        match self.queue.try_send(message) {
            Ok(()) => incident.pending = true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.queue_full_logged {
                    eprintln!("[telegram] notification queue full; will retry");
                    self.queue_full_logged = true;
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                incident.permanently_failed = true;
                eprintln!("[telegram] sender stopped; notification disabled for {source_key}");
            }
        }
    }

    fn delivery_ack(&mut self, ack: DeliveryAck) {
        if let Some((market_key, _)) = ack.key.rsplit_once('|') {
            let venue = market_key.split_once('|').map(|(venue, _)| venue);
            if venue.is_some_and(|venue| self.catalogued.contains(venue))
                && !self.markets.contains_key(market_key)
            {
                return;
            }
        }
        let incident = self
            .incidents
            .entry(ack.key.clone())
            .or_insert_with(|| self.state.get(&ack.key).into());
        incident.pending = false;
        match ack.delivery {
            Delivery::Sent(sent_at) => {
                incident.notified = true;
                incident.last_sent_at = Some(sent_at);
                incident.next_retry_at = None;
                if let Err(error) = self.state.set(
                    ack.key,
                    IncidentState {
                        notified: incident.notified,
                        last_sent_at: incident.last_sent_at,
                    },
                ) {
                    eprintln!("[state] notification sent but state save failed: {error}");
                }
            }
            Delivery::Stale => {}
            Delivery::Retryable { reason, after } => {
                incident.next_retry_at = Some(Instant::now() + after);
                eprintln!("[telegram] transient send failure: {reason}");
            }
            Delivery::Permanent(reason) => {
                incident.permanently_failed = true;
                eprintln!("[telegram] permanent send failure: {reason}");
            }
        }
    }
}

fn market_key(venue: &str, symbol: &str) -> String {
    format!("{venue}|{symbol}")
}

fn event_key(market_key: &str) -> String {
    format!("{market_key}|down")
}

fn exchange_name(venue: &str) -> String {
    match venue {
        "binance" => "Binance".into(),
        "okx" => "OKX".into(),
        "bitget" => "Bitget".into(),
        "bybit" => "Bybit".into(),
        "gate" => "Gate".into(),
        _ => venue.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    fn engine(name: &str) -> (Engine, mpsc::Receiver<Notification>, PathBuf) {
        engine_with_capacity(name, 8)
    }

    fn engine_with_capacity(
        name: &str,
        capacity: usize,
    ) -> (Engine, mpsc::Receiver<Notification>, PathBuf) {
        let path =
            std::env::temp_dir().join(format!("depeg-engine-{}-{name}.json", std::process::id()));
        let _ = fs::remove_file(&path);
        let state = StateFile::load(&path).unwrap();
        let (queue, receiver) = mpsc::channel(capacity);
        let (status, _status_receiver) = watch::channel(String::new());
        let (_authorized_sender, authorized_receiver) = watch::channel(vec![123]);
        let config = Config {
            reminder_seconds: 30,
            ..Config::default()
        };
        (
            Engine::new(config, state, queue, status, authorized_receiver),
            receiver,
            path,
        )
    }

    fn market() -> MarketInfo {
        MarketInfo {
            symbol: "USDE/USDT".into(),
            display_base: "USDe".into(),
        }
    }

    fn dai_market() -> MarketInfo {
        MarketInfo {
            symbol: "DAI/USDT".into(),
            display_base: "DAI".into(),
        }
    }

    fn evaluate_at(
        engine: &mut Engine,
        market: &MarketInfo,
        price: &str,
        start: Instant,
        sec: u64,
    ) {
        let at = start + Duration::from_secs(sec);
        engine.evaluate_depeg(
            "binance|USDE/USDT",
            market,
            "binance",
            price.parse().unwrap(),
            EvaluationTime {
                now: at,
                sampled_at: at,
            },
        );
    }

    #[test]
    fn event_keys_are_scoped_to_market_and_venue() {
        assert_eq!(event_key("binance|USDE/USDT"), "binance|USDE/USDT|down");
        assert_ne!(event_key("binance|USDE/USDT"), event_key("okx|USDE/USDT"));
    }

    #[test]
    fn status_lists_all_five_and_shows_freshness() {
        let (mut engine, _queue, path) = engine("status");
        for venue in ["binance", "okx", "bitget", "bybit", "gate"] {
            engine.offline.insert(venue.into());
        }
        let market = market();
        engine.set_markets("binance", vec![market.clone()]);
        engine.update_book(BookUpdate {
            venue: "binance".into(),
            market,
            quote: Ok(("0.999".parse().unwrap(), "1.001".parse().unwrap())),
            received_at: Instant::now(),
        });
        let status = engine.status_text();
        for name in ["Binance", "OKX", "Bitget", "Bybit", "Gate"] {
            assert!(status.contains(name));
        }
        assert!(status.contains("live <b>Binance</b> — 1/1 L2 fresh"));
        assert!(status.contains("USDe / USDT: 0s"));
        assert!(status.contains("reconnecting <b>Bitget</b> — 0 spot pairs"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn status_identifies_each_stale_market_and_its_age() {
        let (mut engine, _queue, path) = engine("market-status");
        let stable = market();
        let quiet = dai_market();
        engine.set_markets("binance", vec![stable.clone(), quiet.clone()]);
        for (market, age) in [(stable, Duration::ZERO), (quiet, Duration::from_secs(20))] {
            let key = market_key("binance", &market.symbol);
            engine.books.insert(
                key,
                BookUpdate {
                    venue: "binance".into(),
                    market,
                    quote: Ok(("0.999".parse().unwrap(), "1.001".parse().unwrap())),
                    received_at: Instant::now() - age,
                },
            );
        }

        let status = engine.status_text();
        assert!(status.contains("partial <b>Binance</b> — 1/2 L2 fresh"));
        assert!(status.contains("DAI / USDT: no update 20s"));
        assert!(status.contains("USDe / USDT: 0s"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reconnect_requires_a_fresh_book_for_each_market() {
        let (mut engine, _queue, path) = engine("market-resync");
        let stable = market();
        let dai = dai_market();
        engine.set_markets("binance", vec![stable.clone(), dai.clone()]);
        for market in [stable.clone(), dai] {
            engine.update_book(BookUpdate {
                venue: "binance".into(),
                market,
                quote: Ok(("0.999".parse().unwrap(), "1.001".parse().unwrap())),
                received_at: Instant::now(),
            });
        }

        engine.offline.insert("binance".into());
        engine.pause_venue("binance");
        assert!(engine.books.is_empty());
        engine.set_markets("binance", vec![stable.clone(), dai_market()]);
        assert!(engine.offline.contains("binance"));

        engine.update_book(BookUpdate {
            venue: "binance".into(),
            market: stable,
            quote: Ok(("0.999".parse().unwrap(), "1.001".parse().unwrap())),
            received_at: Instant::now(),
        });
        assert!(!engine.stale.contains("binance|USDE/USDT"));
        assert!(engine.stale.contains("binance|DAI/USDT"));
        let status = engine.status_text();
        assert!(status.contains("partial <b>Binance</b> — 1/2 L2 fresh"));
        assert!(status.contains("DAI / USDT: waiting for L2"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn monitor_settings_remove_disabled_venues_and_unselected_coins() {
        let (mut engine, _queue, path) = engine("settings-change");
        let stable = market();
        let dai = dai_market();
        engine.set_markets("binance", vec![stable.clone(), dai]);
        engine.set_markets("okx", vec![stable.clone()]);

        engine.apply_monitor_settings(MonitorSettings {
            exchanges: vec!["binance".into()],
            stablecoins: vec!["USDe".into()],
        });

        assert_eq!(engine.config.exchanges, ["binance"]);
        assert_eq!(engine.config.stablecoins, ["USDe"]);
        assert_eq!(engine.markets.len(), 1);
        assert!(engine.markets.contains_key("binance|USDE/USDT"));
        assert!(engine.books.is_empty());
        assert!(engine.offline.contains("binance"));
        let disabled_alert = event_key("okx|USDE/USDT");
        engine.delivery_ack(DeliveryAck {
            key: disabled_alert.clone(),
            delivery: Delivery::Sent(Utc::now()),
        });
        assert!(!engine.incidents.contains_key(&disabled_alert));
        assert!(!engine.state.get(&disabled_alert).notified);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn only_the_sell_one_price_can_trigger_a_downward_alert() {
        let (mut engine, mut queue, path) = engine("sell-one");
        let market = market();
        let key = market_key("binance", &market.symbol);
        engine.set_markets("binance", vec![market.clone()]);
        engine.books.insert(
            key.clone(),
            BookUpdate {
                venue: "binance".into(),
                market,
                quote: Ok(("0.95".parse().unwrap(), "1.0".parse().unwrap())),
                received_at: Instant::now(),
            },
        );

        engine.evaluate();
        assert!(engine.incidents[&event_key(&key)]
            .confirmation_since
            .is_none());
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn one_percent_sell_one_depeg_confirms_at_five_seconds() {
        let (mut engine, mut queue, path) = engine("threshold");
        let market = market();
        let start = Instant::now();
        for sec in [0, 4] {
            evaluate_at(&mut engine, &market, "0.990", start, sec);
        }
        assert!(queue.try_recv().is_err());
        evaluate_at(&mut engine, &market, "0.990", start, 5);
        let alert = queue.try_recv().unwrap();
        assert_eq!(alert.price, "0.990".parse().unwrap());
        assert_eq!(alert.base, "USDe");
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn ask_at_the_fair_price_side_does_not_start_a_downward_alert() {
        let (mut engine, mut queue, path) = engine("fair-side");
        let market = market();
        let start = Instant::now();
        evaluate_at(&mut engine, &market, "1.005", start, 0);
        evaluate_at(&mut engine, &market, "1.005", start, 5);
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn recovery_from_invalid_or_stale_book_restarts_confirmation() {
        let (mut engine, mut queue, path) = engine("stale-book");
        let market = market();
        let key = market_key("binance", &market.symbol);
        let now = Instant::now();
        engine.set_markets("binance", vec![market.clone()]);
        engine.books.insert(
            key.clone(),
            BookUpdate {
                venue: "binance".into(),
                market,
                quote: Ok(("0.98".parse().unwrap(), "0.99".parse().unwrap())),
                received_at: now - Duration::from_secs(16),
            },
        );
        engine.incidents.insert(
            event_key(&key),
            Incident {
                confirmation_since: Some(now - Duration::from_secs(6)),
                ..Incident::default()
            },
        );

        engine.evaluate();
        assert!(engine.incidents[&event_key(&key)]
            .confirmation_since
            .is_none());
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn brief_move_above_one_percent_resets_confirmation() {
        let (mut engine, mut queue, path) = engine("threshold-reset");
        let market = market();
        let start = Instant::now();
        for (sec, price) in [(0, "0.989"), (4, "0.9900001"), (5, "0.989"), (9, "0.989")] {
            evaluate_at(&mut engine, &market, price, start, sec);
        }
        assert!(queue.try_recv().is_err());
        evaluate_at(&mut engine, &market, "0.989", start, 10);
        assert_eq!(queue.try_recv().unwrap().price, "0.989".parse().unwrap());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn decimal_overflow_pauses_confirmation_without_panicking() {
        let (mut engine, mut queue, path) = engine("decimal-overflow");
        let start = Instant::now();
        let key = event_key("binance|USDE/USDT");
        engine.incidents.insert(
            key.clone(),
            Incident {
                confirmation_since: Some(start - Duration::from_secs(6)),
                ..Incident::default()
            },
        );
        engine.evaluate_depeg(
            "binance|USDE/USDT",
            &market(),
            "binance",
            Decimal::MAX,
            EvaluationTime {
                now: start,
                sampled_at: start,
            },
        );
        assert!(engine.incidents[&key].confirmation_since.is_none());
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn full_notification_queue_retries_after_capacity_returns() {
        let (mut engine, mut queue, path) = engine_with_capacity("queue-full", 1);
        let market = market();
        let start = Instant::now();
        for source in ["first", "second"] {
            for sec in [0, 5] {
                let at = start + Duration::from_secs(sec);
                engine.evaluate_depeg(
                    &format!("binance|USDE/USDT|{source}"),
                    &market,
                    "binance",
                    "0.989".parse().unwrap(),
                    EvaluationTime {
                        now: at,
                        sampled_at: at,
                    },
                );
            }
        }
        let first = queue.try_recv().unwrap();
        let second_source = "binance|USDE/USDT|second";
        let second_key = event_key(second_source);
        assert!(!engine.incidents[&second_key].pending);
        engine.evaluate_depeg(
            second_source,
            &market,
            "binance",
            "0.989".parse().unwrap(),
            EvaluationTime {
                now: start + Duration::from_secs(6),
                sampled_at: start + Duration::from_secs(6),
            },
        );
        let second = queue.try_recv().unwrap();
        assert_ne!(first.key, second.key);
        assert_eq!(second.key, second_key);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn pending_alert_is_not_cleared_until_telegram_acknowledges_it() {
        let (mut engine, mut queue, path) = engine("pending-recovery");
        let market = market();
        let start = Instant::now();
        evaluate_at(&mut engine, &market, "0.989", start, 0);
        evaluate_at(&mut engine, &market, "0.989", start, 5);
        let alert = queue.try_recv().unwrap();
        evaluate_at(&mut engine, &market, "1.0", start, 6);
        evaluate_at(&mut engine, &market, "1.0", start, 36);
        assert!(engine.incidents[&alert.key].pending);
        engine.delivery_ack(DeliveryAck {
            key: alert.key.clone(),
            delivery: Delivery::Stale,
        });
        evaluate_at(&mut engine, &market, "1.0", start, 37);
        assert!(!engine.incidents[&alert.key].notified);
        assert!(!engine.incidents[&alert.key].pending);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reminders_use_the_latest_price_and_wait_for_the_interval() {
        let path =
            std::env::temp_dir().join(format!("depeg-engine-{}-reminder.json", std::process::id()));
        let _ = fs::remove_file(&path);
        let key = event_key("binance|USDE/USDT");
        let mut state = StateFile::load(&path).unwrap();
        state
            .set(
                key.clone(),
                IncidentState {
                    notified: true,
                    last_sent_at: Some(Utc::now()),
                },
            )
            .unwrap();
        let (sender, mut queue) = mpsc::channel(8);
        let (status, _status_receiver) = watch::channel(String::new());
        let (_authorized_sender, authorized_receiver) = watch::channel(vec![123]);
        let config = Config {
            reminder_seconds: 1800,
            ..Config::default()
        };
        let mut engine = Engine::new(config, state, sender, status, authorized_receiver);
        let market = market();
        let start = Instant::now();
        for sec in [0, 5] {
            evaluate_at(&mut engine, &market, "0.989", start, sec);
        }
        assert!(queue.try_recv().is_err());
        engine.incidents.get_mut(&key).unwrap().last_sent_at =
            Some(Utc::now() - chrono::Duration::minutes(31));
        evaluate_at(&mut engine, &market, "0.988", start, 6);
        let reminder = queue.try_recv().unwrap();
        assert_eq!(reminder.price, "0.988".parse().unwrap());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn recovery_requires_thirty_seconds_strictly_below_four_bps() {
        let (mut engine, _queue, path) = engine("recovery");
        let market = market();
        let key = event_key("binance|USDE/USDT");
        engine.delivery_ack(DeliveryAck {
            key: key.clone(),
            delivery: Delivery::Sent(Utc::now()),
        });
        let start = Instant::now();
        evaluate_at(&mut engine, &market, "0.996", start, 0);
        evaluate_at(&mut engine, &market, "0.996", start, 30);
        assert!(engine.incidents[&key].notified);
        evaluate_at(&mut engine, &market, "0.9961", start, 31);
        evaluate_at(&mut engine, &market, "0.9961", start, 60);
        assert!(engine.incidents[&key].notified);
        evaluate_at(&mut engine, &market, "0.9961", start, 61);
        assert!(!engine.incidents[&key].notified);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn failed_notification_does_not_mark_alert_as_sent() {
        let (mut engine, mut queue, path) = engine("failed-send");
        let market = market();
        let start = Instant::now();
        evaluate_at(&mut engine, &market, "0.989", start, 0);
        evaluate_at(&mut engine, &market, "0.989", start, 5);
        let alert = queue.try_recv().unwrap();
        engine.delivery_ack(DeliveryAck {
            key: alert.key.clone(),
            delivery: Delivery::Permanent("HTTP 401".into()),
        });
        assert!(!engine.incidents[&alert.key].notified);
        assert!(engine.incidents[&alert.key].permanently_failed);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn state_save_failure_does_not_undo_successful_delivery() {
        let directory = std::env::temp_dir().join(format!(
            "depeg-engine-{}-state-save-failure",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("alerts.json");
        fs::create_dir(path.with_extension("json.tmp")).unwrap();
        let state = StateFile::load(&path).unwrap();
        let (sender, _queue) = mpsc::channel(1);
        let (status, _status_receiver) = watch::channel(String::new());
        let (_authorized_sender, authorized_receiver) = watch::channel(vec![123]);
        let mut engine = Engine::new(
            Config::default(),
            state,
            sender,
            status,
            authorized_receiver,
        );
        let key = event_key("binance|USDE/USDT");
        engine.delivery_ack(DeliveryAck {
            key: key.clone(),
            delivery: Delivery::Sent(Utc::now()),
        });
        assert!(engine.incidents[&key].notified);
        fs::remove_dir_all(directory).unwrap();
    }
}
