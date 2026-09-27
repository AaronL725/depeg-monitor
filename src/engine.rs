use crate::{
    alert::{Delivery, DeliveryAck, Direction, Level, Notification},
    config::Config,
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
    warning_since: Option<Instant>,
    critical_since: Option<Instant>,
    recovery_since: Option<Instant>,
    notified_level: u8,
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
            notified_level: value.notified_level,
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
    online: HashSet<String>,
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
            online: HashSet::new(),
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
                    Some(FeedEvent::Markets { venue, markets }) => self.set_markets(&venue, markets),
                    Some(FeedEvent::Book(book)) => {
                        let key = market_key(&book.venue, &book.market.symbol);
                        if self.markets.contains_key(&key) {
                            self.online.insert(book.venue.clone());
                            self.offline.remove(&book.venue);
                            self.stale.remove(&key);
                            match &book.quote {
                                Ok(_) => { self.books.insert(key, book); }
                                Err(reason) => {
                                    self.books.remove(&key);
                                    self.pause_incident(&key, Direction::Down);
                                    self.pause_incident(&key, Direction::Up);
                                    if self.stale.insert(key) {
                                        eprintln!("[market] {} {} invalid: {reason}", book.venue, book.market.symbol);
                                    }
                                }
                            }
                        }
                    }
                    Some(FeedEvent::Offline { venue, reason }) => {
                        self.online.remove(&venue);
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
            let total = self
                .markets
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .count();
            let fresh = self
                .books
                .iter()
                .filter(|(key, book)| {
                    key.starts_with(&prefix)
                        && self.online.contains(venue)
                        && book.quote.is_ok()
                        && now.duration_since(book.received_at) <= max_age
                })
                .count();
            let needs_anchor = self
                .markets
                .iter()
                .any(|(key, market)| key.starts_with(&prefix) && market.quote == "USDC");
            let anchor_fresh = self.markets.iter().any(|(key, market)| {
                key.starts_with(&prefix)
                    && market.base == "USDC"
                    && market.quote == "USDT"
                    && self.online.contains(venue)
                    && self.books.get(key).is_some_and(|book| {
                        book.quote.is_ok() && now.duration_since(book.received_at) <= max_age
                    })
            });
            let state = if self.offline.contains(venue) {
                "🔴 reconnecting"
            } else if total == 0 {
                "⚪ waiting for markets"
            } else if needs_anchor && !anchor_fresh {
                "🟡 USDC anchor unavailable"
            } else if fresh == total {
                "🟢 live"
            } else if fresh > 0 {
                "🟡 partial"
            } else if self.online.contains(venue) {
                "🟠 no fresh L2"
            } else {
                "🟡 waiting for L2"
            };
            let mut coverage = if total == 0 {
                "0 spot pairs".to_string()
            } else {
                format!("{fresh}/{total} L2 fresh")
            };
            if needs_anchor && !anchor_fresh {
                coverage.push_str("; USDC/USDT ask unavailable");
            }
            lines.push(format!(
                "{state} <b>{}</b> — {coverage}",
                exchange_name(venue)
            ));
        }
        lines.join("\n")
    }

    fn publish_status(&self) {
        self.status.send_replace(self.status_text());
    }

    fn set_markets(&mut self, venue: &str, markets: Vec<MarketInfo>) {
        self.offline.remove(venue);
        self.catalogued.insert(venue.into());
        let count = markets.len();
        let symbols = markets
            .iter()
            .map(|market| market.symbol.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let retained: std::collections::HashSet<_> = markets
            .iter()
            .map(|market| market_key(venue, &market.symbol))
            .collect();
        let removed: Vec<_> = self
            .markets
            .keys()
            .filter(|key| key.starts_with(&format!("{venue}|")) && !retained.contains(*key))
            .cloned()
            .collect();
        if let Err(error) = self.state.retain_markets(venue, &retained) {
            eprintln!("[state] delisted market cleanup failed: {error}");
        }
        for key in removed {
            self.books.remove(&key);
            self.stale.remove(&key);
            self.incidents.remove(&event_key(&key, Direction::Down));
            self.incidents.remove(&event_key(&key, Direction::Up));
        }
        self.markets
            .retain(|key, _| !key.starts_with(&format!("{venue}|")));
        for market in markets {
            self.markets
                .insert(market_key(venue, &market.symbol), market);
        }
        eprintln!("[market] {venue}: {count} stablecoin spot markets: {symbols}");
    }

    fn pause_venue(&mut self, venue: &str) {
        let keys: Vec<_> = self
            .markets
            .keys()
            .filter(|key| key.starts_with(&format!("{venue}|")))
            .cloned()
            .collect();
        for key in keys {
            self.stale.insert(key.clone());
            self.pause_incident(&key, Direction::Down);
            self.pause_incident(&key, Direction::Up);
        }
    }

    fn evaluate(&mut self) {
        if self.queue.capacity() > self.config.notification_queue_capacity / 2 {
            self.queue_full_logged = false;
        }
        let now = Instant::now();
        let fresh = Duration::from_secs(self.config.max_quote_age_seconds);
        let mut anchor_by_venue = HashMap::new();
        for book in self.books.values() {
            if self.online.contains(&book.venue)
                && book.market.base == "USDC"
                && book.market.quote == "USDT"
                && now.duration_since(book.received_at) <= fresh
            {
                if let Ok((_, ask)) = &book.quote {
                    anchor_by_venue.insert(book.venue.clone(), (*ask, book.received_at));
                }
            }
        }

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
                self.pause_incident(&key, Direction::Down);
                self.pause_incident(&key, Direction::Up);
                continue;
            };
            if !self.online.contains(&venue) || now.duration_since(received_at) > fresh {
                if self.stale.insert(key.clone()) {
                    eprintln!("[market] {venue} {} data stale", market.symbol);
                }
                self.pause_incident(&key, Direction::Down);
                self.pause_incident(&key, Direction::Up);
                continue;
            }
            let Ok((raw_bid, raw_ask)) = quote else {
                self.pause_incident(&key, Direction::Down);
                self.pause_incident(&key, Direction::Up);
                continue;
            };
            let anchor = anchor_by_venue.get(&venue).copied();
            let Some((bid, ask)) =
                normalize_prices(&market, raw_bid, raw_ask, anchor.map(|(ask, _)| ask))
            else {
                self.pause_incident(&key, Direction::Down);
                self.pause_incident(&key, Direction::Up);
                continue;
            };
            let sampled_at = anchor
                .map(|(_, time)| time.min(received_at))
                .unwrap_or(received_at);
            let time = EvaluationTime { now, sampled_at };
            self.evaluate_direction(&key, &market, &venue, bid, Direction::Down, time);
            self.evaluate_direction(&key, &market, &venue, ask, Direction::Up, time);
        }
    }

    fn pause_incident(&mut self, key: &str, direction: Direction) {
        let id = event_key(key, direction);
        if let Some(incident) = self.incidents.get_mut(&id) {
            incident.warning_since = None;
            incident.critical_since = None;
            incident.recovery_since = None;
        }
    }

    fn evaluate_direction(
        &mut self,
        source_key: &str,
        market: &MarketInfo,
        venue: &str,
        price: Decimal,
        direction: Direction,
        time: EvaluationTime,
    ) {
        let EvaluationTime { now, sampled_at } = time;
        let Some(deviation_bps) = price
            .checked_sub(Decimal::ONE)
            .and_then(|deviation| deviation.checked_mul(Decimal::from(10_000)))
        else {
            self.pause_incident(source_key, direction);
            return;
        };
        let directional_bps = match direction {
            Direction::Down if deviation_bps < Decimal::ZERO => -deviation_bps,
            Direction::Up if deviation_bps > Decimal::ZERO => deviation_bps,
            _ => Decimal::ZERO,
        };
        let critical = directional_bps >= Decimal::from(self.config.critical_bps);
        let warning = directional_bps >= Decimal::from(self.config.warning_bps);
        let recovered = directional_bps < Decimal::from(self.config.recovery_bps);
        let id = event_key(source_key, direction);
        let incident = self
            .incidents
            .entry(id.clone())
            .or_insert_with(|| self.state.get(&id).into());

        if recovered {
            incident.warning_since = None;
            incident.critical_since = None;
            if incident.notified_level == 0
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
        if warning {
            incident.warning_since.get_or_insert(now);
        } else {
            incident.warning_since = None;
        }
        if critical {
            incident.critical_since.get_or_insert(now);
        } else {
            incident.critical_since = None;
        }

        let warning_confirmed = incident.warning_since.is_some_and(|since| {
            now.duration_since(since) >= Duration::from_secs(self.config.confirmation_seconds)
        });
        let critical_confirmed = incident.critical_since.is_some_and(|since| {
            now.duration_since(since) >= Duration::from_secs(self.config.confirmation_seconds)
        });
        let current_level = if critical_confirmed {
            Some(Level::Critical)
        } else if warning_confirmed {
            Some(Level::Warning)
        } else {
            None
        };

        let next = if critical_confirmed && incident.notified_level < 2 {
            Some(Level::Critical)
        } else if warning_confirmed && incident.notified_level == 0 {
            Some(Level::Warning)
        } else if incident.notified_level > 0
            && current_level.is_some()
            && incident.last_sent_at.is_some_and(|sent| {
                (Utc::now() - sent).to_std().unwrap_or_default()
                    >= Duration::from_secs(self.config.reminder_seconds)
            })
        {
            current_level
        } else {
            None
        };
        let Some(level) = next else { return };
        if self.authorized_chats.borrow().is_empty() {
            return;
        }
        if incident.pending
            || incident.permanently_failed
            || incident.next_retry_at.is_some_and(|retry| now < retry)
        {
            return;
        }

        let pair_label = if market.quote == "USDC" {
            format!("{} → USDT", market.pair_label)
        } else {
            market.pair_label.clone()
        };
        let message = Notification {
            key: id,
            base: market.display_base.clone(),
            pair_label,
            exchange: exchange_name(venue),
            direction,
            level,
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
                incident.notified_level = incident.notified_level.max(ack.level.value());
                incident.last_sent_at = Some(sent_at);
                incident.next_retry_at = None;
                if let Err(error) = self.state.set(
                    ack.key,
                    IncidentState {
                        notified_level: incident.notified_level,
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

fn normalize_prices(
    market: &MarketInfo,
    bid: Decimal,
    ask: Decimal,
    usdc_usdt_ask: Option<Decimal>,
) -> Option<(Decimal, Decimal)> {
    let conversion = match market.quote.as_str() {
        "USDT" => Decimal::ONE,
        "USDC" => usdc_usdt_ask?,
        _ => return None,
    };
    Some((bid.checked_mul(conversion)?, ask.checked_mul(conversion)?))
}

fn market_key(venue: &str, symbol: &str) -> String {
    format!("{venue}|{symbol}")
}

fn event_key(market_key: &str, direction: Direction) -> String {
    let direction = match direction {
        Direction::Down => "down",
        Direction::Up => "up",
    };
    format!("{market_key}|{direction}")
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
        let state = StateFile::load(&path).unwrap();
        let (queue, receiver) = mpsc::channel(capacity);
        let (status, _status_receiver) = watch::channel(String::new());
        let (_authorized_sender, authorized_receiver) = watch::channel(vec![123]);
        let mut config = Config::default();
        config.reminder_seconds = 30;
        (
            Engine::new(config, state, queue, status, authorized_receiver),
            receiver,
            path,
        )
    }

    fn market() -> MarketInfo {
        MarketInfo {
            symbol: "USDe/USDT".into(),
            base: "USDE".into(),
            display_base: "USDe".into(),
            quote: "USDT".into(),
            pair_label: "USDe / USDT".into(),
        }
    }

    #[test]
    fn event_keys_keep_direction_and_venue_independent() {
        assert_ne!(
            event_key("binance|USDe/USDT", Direction::Down),
            event_key("binance|USDe/USDT", Direction::Up)
        );
        assert_ne!(
            event_key("binance|USDe/USDT", Direction::Down),
            event_key("okx|USDe/USDT", Direction::Down)
        );
    }

    #[test]
    fn usdc_conversion_uses_the_same_venue_sell_price() {
        let mut target = market();
        target.quote = "USDC".into();
        target.pair_label = "USDe / USDC".into();
        let (bid, ask) = normalize_prices(
            &target,
            "0.997".parse().unwrap(),
            "1.001".parse().unwrap(),
            Some("1.002".parse().unwrap()),
        )
        .unwrap();
        assert_eq!(bid, "0.998994".parse().unwrap());
        assert_eq!(ask, "1.003002".parse().unwrap());
        assert!(normalize_prices(&target, bid, ask, None).is_none());
        assert!(
            normalize_prices(&target, Decimal::MAX, Decimal::MAX, Some(Decimal::from(2))).is_none()
        );
    }

    #[test]
    fn status_lists_all_five_and_shows_freshness() {
        let (mut engine, _queue, path) = engine("status");
        for venue in ["binance", "okx", "bitget", "bybit", "gate"] {
            engine.offline.insert(venue.into());
        }
        let direct = market();
        let key = market_key("binance", &direct.symbol);
        engine.set_markets("binance", vec![direct.clone()]);
        engine.online.insert("binance".into());
        engine.books.insert(
            key,
            BookUpdate {
                venue: "binance".into(),
                market: direct,
                quote: Ok(("0.999".parse().unwrap(), "1.001".parse().unwrap())),
                received_at: Instant::now(),
            },
        );
        let status = engine.status_text();
        for name in ["Binance", "OKX", "Bitget", "Bybit", "Gate"] {
            assert!(status.contains(name));
        }
        assert!(status.contains("live <b>Binance</b> — 1/1 L2 fresh"));
        assert!(status.contains("reconnecting <b>Bitget</b> — 0 spot pairs"));

        let mut cross = market();
        cross.symbol = "USDe/USDC".into();
        cross.quote = "USDC".into();
        cross.pair_label = "USDe / USDC".into();
        let cross_key = market_key("binance", &cross.symbol);
        engine.markets.insert(cross_key.clone(), cross.clone());
        engine.books.insert(
            cross_key,
            BookUpdate {
                venue: "binance".into(),
                market: cross,
                quote: Ok(("0.99".parse().unwrap(), "1.01".parse().unwrap())),
                received_at: Instant::now(),
            },
        );
        let status = engine.status_text();
        assert!(status.contains("USDC anchor unavailable <b>Binance</b>"));
        assert!(status.contains("USDC/USDT ask unavailable"));

        let anchor = MarketInfo {
            symbol: "USDC/USDT".into(),
            base: "USDC".into(),
            display_base: "USDC".into(),
            quote: "USDT".into(),
            pair_label: "USDC / USDT".into(),
        };
        let anchor_key = market_key("binance", &anchor.symbol);
        engine.markets.insert(anchor_key.clone(), anchor.clone());
        engine.books.insert(
            anchor_key,
            BookUpdate {
                venue: "binance".into(),
                market: anchor,
                quote: Ok(("1.001".parse().unwrap(), "1.002".parse().unwrap())),
                received_at: Instant::now(),
            },
        );
        assert!(engine
            .status_text()
            .contains("live <b>Binance</b> — 3/3 L2 fresh"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn active_depeg_is_sent_after_a_chat_is_authorized() {
        let path = std::env::temp_dir().join(format!(
            "depeg-engine-{}-authorization.json",
            std::process::id()
        ));
        let state = StateFile::load(&path).unwrap();
        let (sender, mut queue) = mpsc::channel(8);
        let (status, _status_receiver) = watch::channel(String::new());
        let (authorized_sender, authorized_receiver) = watch::channel(Vec::new());
        let mut engine = Engine::new(
            Config::default(),
            state,
            sender,
            status,
            authorized_receiver,
        );
        let market = market();
        let source = "binance|USDe/USDT";
        let start = Instant::now();
        for seconds in [0, 5] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "0.994".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
        }
        assert!(queue.try_recv().is_err());
        authorized_sender.send_replace(vec![123]);
        engine.evaluate_direction(
            source,
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(6),
                sampled_at: start + Duration::from_secs(6),
            },
        );
        assert_eq!(queue.try_recv().unwrap().level, Level::Warning);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn stale_usdc_anchor_and_target_books_pause_confirmation() {
        let (mut engine, mut queue, path) = engine("stale-cross");
        let mut target = market();
        target.symbol = "USDe/USDC".into();
        target.quote = "USDC".into();
        target.pair_label = "USDe / USDC".into();
        let anchor = MarketInfo {
            symbol: "USDC/USDT".into(),
            base: "USDC".into(),
            display_base: "USDC".into(),
            quote: "USDT".into(),
            pair_label: "USDC / USDT".into(),
        };
        let target_key = market_key("binance", &target.symbol);
        let anchor_key = market_key("binance", &anchor.symbol);
        let incident_key = event_key(&target_key, Direction::Down);
        let now = Instant::now();
        engine.markets.insert(target_key.clone(), target.clone());
        engine.markets.insert(anchor_key.clone(), anchor.clone());
        engine.online.insert("binance".into());
        engine.books.insert(
            target_key.clone(),
            BookUpdate {
                venue: "binance".into(),
                market: target.clone(),
                quote: Ok(("0.94".parse().unwrap(), "0.96".parse().unwrap())),
                received_at: now,
            },
        );
        engine.books.insert(
            anchor_key.clone(),
            BookUpdate {
                venue: "binance".into(),
                market: anchor,
                quote: Ok(("1.001".parse().unwrap(), "1.002".parse().unwrap())),
                received_at: now - Duration::from_secs(16),
            },
        );
        engine.incidents.insert(
            incident_key.clone(),
            Incident {
                warning_since: Some(now - Duration::from_secs(6)),
                critical_since: Some(now - Duration::from_secs(6)),
                ..Incident::default()
            },
        );

        engine.evaluate();
        assert!(engine.incidents[&incident_key].warning_since.is_none());
        assert!(queue.try_recv().is_err());

        engine.books.get_mut(&anchor_key).unwrap().received_at = Instant::now();
        engine.evaluate();
        assert!(engine.incidents[&incident_key].warning_since.is_some());
        assert!(queue.try_recv().is_err());

        engine.books.get_mut(&target_key).unwrap().received_at =
            Instant::now() - Duration::from_secs(16);
        engine.evaluate();
        assert!(engine.incidents[&incident_key].warning_since.is_none());
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn both_directions_confirm_at_the_exact_unrounded_threshold() {
        let (mut engine, mut queue, path) = engine("thresholds");
        let market = market();
        let start = Instant::now();
        for (source, price, direction) in [
            ("binance|USDe/USDT|down-boundary", "0.995", Direction::Down),
            ("binance|USDe/USDT|up-boundary", "1.005", Direction::Up),
            ("binance|USDe/USDT|wrong-down", "1.010", Direction::Down),
            ("binance|USDe/USDT|wrong-up", "0.990", Direction::Up),
        ] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                price.parse().unwrap(),
                direction,
                EvaluationTime {
                    now: start,
                    sampled_at: start,
                },
            );
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                price.parse().unwrap(),
                direction,
                EvaluationTime {
                    now: start + Duration::from_secs(4),
                    sampled_at: start,
                },
            );
        }
        assert!(queue.try_recv().is_err());
        for (source, price, direction) in [
            ("binance|USDe/USDT|down-boundary", "0.995", Direction::Down),
            ("binance|USDe/USDT|up-boundary", "1.005", Direction::Up),
        ] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                price.parse().unwrap(),
                direction,
                EvaluationTime {
                    now: start + Duration::from_secs(5),
                    sampled_at: start,
                },
            );
        }
        assert_eq!(queue.try_recv().unwrap().direction, Direction::Down);
        assert_eq!(queue.try_recv().unwrap().direction, Direction::Up);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn decimal_overflow_pauses_confirmation_without_panicking() {
        let (mut engine, mut queue, path) = engine("decimal-overflow");
        let start = Instant::now();
        let key = event_key("binance|USDe/USDT", Direction::Up);
        engine.incidents.insert(
            key.clone(),
            Incident {
                warning_since: Some(start - Duration::from_secs(6)),
                critical_since: Some(start - Duration::from_secs(6)),
                ..Incident::default()
            },
        );
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market(),
            "binance",
            Decimal::MAX,
            Direction::Up,
            EvaluationTime {
                now: start,
                sampled_at: start,
            },
        );
        assert!(engine.incidents[&key].warning_since.is_none());
        assert!(engine.incidents[&key].critical_since.is_none());
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn full_notification_queue_retries_after_capacity_returns() {
        let (mut engine, mut queue, path) = engine_with_capacity("queue-full", 1);
        let market = market();
        let start = Instant::now();
        for source in ["binance|USDe/USDT|first", "binance|USDe/USDT|second"] {
            for seconds in [0, 5] {
                engine.evaluate_direction(
                    source,
                    &market,
                    "binance",
                    "0.994".parse().unwrap(),
                    Direction::Down,
                    EvaluationTime {
                        now: start + Duration::from_secs(seconds),
                        sampled_at: start,
                    },
                );
            }
        }
        let first = queue.try_recv().unwrap();
        let second_key = event_key("binance|USDe/USDT|second", Direction::Down);
        assert!(!engine.incidents[&second_key].pending);
        engine.evaluate_direction(
            "binance|USDe/USDT|second",
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
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
    fn recovery_waits_until_a_queued_alert_has_an_acknowledgement() {
        let (mut engine, mut queue, path) = engine("pending-recovery");
        let market = market();
        let source = "binance|USDe/USDT";
        let start = Instant::now();
        for seconds in [0, 5] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "0.994".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
        }
        let alert = queue.try_recv().unwrap();
        for seconds in [6, 36] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "1.000".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start + Duration::from_secs(seconds),
                },
            );
        }
        assert!(engine.incidents[&alert.key].pending);
        engine.delivery_ack(DeliveryAck {
            key: alert.key.clone(),
            level: alert.level,
            delivery: Delivery::Stale,
        });
        engine.evaluate_direction(
            source,
            &market,
            "binance",
            "1.000".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(37),
                sampled_at: start + Duration::from_secs(37),
            },
        );
        assert_eq!(engine.incidents[&alert.key].notified_level, 0);
        assert!(!engine.incidents[&alert.key].pending);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn wide_spread_can_send_both_critical_directions_independently() {
        let (mut engine, mut queue, path) = engine("wide-spread");
        let market = market();
        let source = "binance|USDe/USDT";
        let start = Instant::now();
        for seconds in [0, 4] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "0.99".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "1.01".parse().unwrap(),
                Direction::Up,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
        }
        assert!(queue.try_recv().is_err());
        for (price, direction) in [("0.99", Direction::Down), ("1.01", Direction::Up)] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                price.parse().unwrap(),
                direction,
                EvaluationTime {
                    now: start + Duration::from_secs(5),
                    sampled_at: start,
                },
            );
        }
        let down = queue.try_recv().unwrap();
        let up = queue.try_recv().unwrap();
        assert_eq!(
            (down.direction, down.level),
            (Direction::Down, Level::Critical)
        );
        assert_eq!((up.direction, up.level), (Direction::Up, Level::Critical));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dropping_below_warning_resets_the_confirmation_timer() {
        let (mut engine, mut queue, path) = engine("threshold-reset");
        let market = market();
        let source = "binance|USDe/USDT";
        let start = Instant::now();
        for (seconds, price) in [(0, "0.994"), (4, "0.9950001"), (5, "0.994"), (9, "0.994")] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                price.parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
        }
        assert!(queue.try_recv().is_err());
        engine.evaluate_direction(
            source,
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(10),
                sampled_at: start,
            },
        );
        assert_eq!(queue.try_recv().unwrap().level, Level::Warning);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_data_pause_restarts_confirmation_time() {
        let (mut engine, mut queue, path) = engine("pause");
        let market = market();
        let start = Instant::now();
        let source = "binance|USDe/USDT";
        for seconds in [0, 4] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "0.994".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
        }
        engine.pause_incident(source, Direction::Down);
        engine.evaluate_direction(
            source,
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(9),
                sampled_at: start + Duration::from_secs(9),
            },
        );
        assert!(queue.try_recv().is_err());
        engine.evaluate_direction(
            source,
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(14),
                sampled_at: start + Duration::from_secs(14),
            },
        );
        assert_eq!(queue.try_recv().unwrap().level, Level::Warning);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn warning_confirms_after_five_seconds_and_critical_can_upgrade() {
        let (mut engine, mut queue, path) = engine("upgrade");
        let market = market();
        let start = Instant::now();
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start,
                sampled_at: start,
            },
        );
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(4),
                sampled_at: start,
            },
        );
        assert!(queue.try_recv().is_err());
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(5),
                sampled_at: start,
            },
        );
        let warning = queue.try_recv().unwrap();
        assert_eq!(warning.level, Level::Warning);
        engine.delivery_ack(DeliveryAck {
            key: warning.key,
            level: warning.level,
            delivery: Delivery::Sent(Utc::now()),
        });
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.989".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(6),
                sampled_at: start + Duration::from_secs(6),
            },
        );
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.989".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(11),
                sampled_at: start + Duration::from_secs(11),
            },
        );
        assert_eq!(queue.try_recv().unwrap().level, Level::Critical);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reminders_wait_for_the_interval_and_use_the_latest_price() {
        let path = std::env::temp_dir().join(format!(
            "depeg-engine-{}-restart-reminder.json",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let key = event_key("binance|USDe/USDT", Direction::Down);
        let mut state = StateFile::load(&path).unwrap();
        state
            .set(
                key.clone(),
                IncidentState {
                    notified_level: 1,
                    last_sent_at: Some(Utc::now()),
                },
            )
            .unwrap();
        let (sender, mut queue) = mpsc::channel(8);
        let (status, _status_receiver) = watch::channel(String::new());
        let (_authorized_sender, authorized_receiver) = watch::channel(vec![123]);
        let mut config = Config::default();
        config.reminder_seconds = 1800;
        let mut engine = Engine::new(config, state, sender, status, authorized_receiver);
        let market = market();
        let start = Instant::now();
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start,
                sampled_at: start,
            },
        );
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.994".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(5),
                sampled_at: start,
            },
        );
        assert!(queue.try_recv().is_err());
        assert_eq!(engine.incidents[&key].notified_level, 1);

        engine.incidents.get_mut(&key).unwrap().last_sent_at =
            Some(Utc::now() - chrono::Duration::minutes(31));
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.993".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(6),
                sampled_at: start + Duration::from_secs(6),
            },
        );
        let reminder = queue.try_recv().unwrap();
        assert_eq!(reminder.level, Level::Warning);
        assert_eq!(reminder.price, "0.993".parse().unwrap());
        engine.delivery_ack(DeliveryAck {
            key: reminder.key,
            level: reminder.level,
            delivery: Delivery::Sent(Utc::now()),
        });
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.992".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(7),
                sampled_at: start + Duration::from_secs(7),
            },
        );
        assert!(queue.try_recv().is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn recovery_requires_thirty_seconds_below_four_basis_points() {
        let (mut engine, _queue, path) = engine("recovery");
        let market = market();
        let key = "binance|USDe/USDT|down".to_string();
        engine.delivery_ack(DeliveryAck {
            key: key.clone(),
            level: Level::Warning,
            delivery: Delivery::Sent(Utc::now()),
        });
        let start = Instant::now();
        for seconds in [0, 31] {
            engine.evaluate_direction(
                "binance|USDe/USDT",
                &market,
                "binance",
                "0.996".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start + Duration::from_secs(seconds),
                },
            );
        }
        let event = event_key("binance|USDe/USDT", Direction::Down);
        assert_eq!(engine.incidents[&event].notified_level, 1);
        for seconds in [32, 61] {
            engine.evaluate_direction(
                "binance|USDe/USDT",
                &market,
                "binance",
                "0.9961".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start + Duration::from_secs(seconds),
                },
            );
        }
        assert_eq!(engine.incidents[&event].notified_level, 1);
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "0.9961".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(62),
                sampled_at: start + Duration::from_secs(62),
            },
        );
        assert_eq!(engine.incidents[&event].notified_level, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn opposite_side_price_recovers_the_incident_direction() {
        let (mut engine, _queue, path) = engine("opposite-recovery");
        let market = market();
        let key = "binance|USDe/USDT|down".to_string();
        engine.delivery_ack(DeliveryAck {
            key: key.clone(),
            level: Level::Warning,
            delivery: Delivery::Sent(Utc::now()),
        });
        let start = Instant::now();
        for seconds in [0, 29] {
            engine.evaluate_direction(
                "binance|USDe/USDT",
                &market,
                "binance",
                "1.005".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start + Duration::from_secs(seconds),
                },
            );
        }
        let event = event_key("binance|USDe/USDT", Direction::Down);
        assert_eq!(engine.incidents[&event].notified_level, 1);
        engine.evaluate_direction(
            "binance|USDe/USDT",
            &market,
            "binance",
            "1.005".parse().unwrap(),
            Direction::Down,
            EvaluationTime {
                now: start + Duration::from_secs(30),
                sampled_at: start + Duration::from_secs(30),
            },
        );
        assert_eq!(engine.incidents[&event].notified_level, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn failed_notification_does_not_mark_an_alert_as_sent() {
        let (mut engine, mut queue, path) = engine("failed-send");
        let market = market();
        let start = Instant::now();
        for seconds in [0, 5] {
            engine.evaluate_direction(
                "binance|USDe/USDT",
                &market,
                "binance",
                "0.994".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
        }
        let warning = queue.try_recv().unwrap();
        engine.delivery_ack(DeliveryAck {
            key: warning.key.clone(),
            level: warning.level,
            delivery: Delivery::Permanent("HTTP 401".into()),
        });
        assert_eq!(engine.incidents[&warning.key].notified_level, 0);
        assert!(engine.incidents[&warning.key].permanently_failed);
        for seconds in [6, 36] {
            engine.evaluate_direction(
                "binance|USDe/USDT",
                &market,
                "binance",
                "0.997".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start + Duration::from_secs(seconds),
                },
            );
        }
        assert!(!engine.incidents[&warning.key].permanently_failed);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn retryable_delivery_waits_for_retry_after_without_marking_sent() {
        let (mut engine, mut queue, path) = engine("retryable-send");
        let market = market();
        let source = "binance|USDe/USDT";
        let start = Instant::now();
        for seconds in [0, 5] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "0.994".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: start + Duration::from_secs(seconds),
                    sampled_at: start,
                },
            );
        }
        let warning = queue.try_recv().unwrap();
        engine.delivery_ack(DeliveryAck {
            key: warning.key.clone(),
            level: warning.level,
            delivery: Delivery::Retryable {
                reason: "HTTP 429".into(),
                after: Duration::from_secs(30),
            },
        });
        assert_eq!(engine.incidents[&warning.key].notified_level, 0);
        let retry_at = engine.incidents[&warning.key].next_retry_at.unwrap();
        for now in [retry_at - Duration::from_secs(1), retry_at] {
            engine.evaluate_direction(
                source,
                &market,
                "binance",
                "0.993".parse().unwrap(),
                Direction::Down,
                EvaluationTime {
                    now: now,
                    sampled_at: now,
                },
            );
            if now < retry_at {
                assert!(queue.try_recv().is_err());
            }
        }
        let retry = queue.try_recv().unwrap();
        assert_eq!(retry.level, Level::Warning);
        assert_eq!(retry.price, "0.993".parse().unwrap());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn state_save_failure_does_not_undo_a_successful_delivery() {
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
        let key = event_key("binance|USDe/USDT", Direction::Down);
        engine.delivery_ack(DeliveryAck {
            key: key.clone(),
            level: Level::Warning,
            delivery: Delivery::Sent(Utc::now()),
        });
        assert_eq!(engine.incidents[&key].notified_level, 1);
        fs::remove_dir_all(directory).unwrap();
    }
}
