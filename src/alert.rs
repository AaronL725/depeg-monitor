use crate::{config::TelegramConfig, state::AuthorizedChats};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde_json::json;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

#[derive(Debug)]
pub struct Notification {
    pub key: String,
    pub base: String,
    pub exchange: String,
    pub price: Decimal,
    pub confirmed_at: DateTime<Utc>,
    pub sampled_at: Instant,
}

#[derive(Debug)]
pub enum Delivery {
    Sent(DateTime<Utc>),
    Stale,
    Retryable { reason: String, after: Duration },
    Permanent(String),
}

#[derive(Debug)]
pub struct DeliveryAck {
    pub key: String,
    pub delivery: Delivery,
}

#[derive(Clone)]
pub struct Telegram {
    client: Client,
    token: String,
    access_password: String,
}

#[derive(Default)]
struct LoginAttempt {
    failures: u8,
    locked_until: Option<Instant>,
    last_attempt: Option<Instant>,
}

impl LoginAttempt {
    fn is_locked(&self, now: Instant) -> bool {
        self.locked_until.is_some_and(|until| now < until)
    }

    fn fail(&mut self, now: Instant) -> bool {
        self.failures += 1;
        self.last_attempt = Some(now);
        if self.failures >= 5 {
            self.locked_until = Some(now + Duration::from_secs(600));
            true
        } else {
            false
        }
    }

    fn can_start(&mut self, now: Instant) -> bool {
        if self.is_locked(now) {
            return false;
        }
        if self.locked_until.is_some() {
            *self = Self::default();
        }
        true
    }

    fn is_expired(&self, now: Instant) -> bool {
        !self.is_locked(now)
            && self
                .last_attempt
                .is_some_and(|last| now.duration_since(last) >= Duration::from_secs(600))
    }
}

impl Telegram {
    pub fn from_config(config: &TelegramConfig) -> Result<Self, String> {
        if config.bot_token.trim().is_empty()
            || config.access_password.trim().is_empty()
            || is_placeholder(&config.bot_token)
            || is_placeholder(&config.access_password)
        {
            return Err("Telegram token and access password must be filled in".into());
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            token: config.bot_token.clone(),
            access_password: config.access_password.clone(),
        })
    }

    pub async fn run(
        self,
        mut queue: mpsc::Receiver<Notification>,
        ack: mpsc::Sender<DeliveryAck>,
        max_age: Duration,
        authorized: watch::Receiver<Vec<i64>>,
    ) {
        while let Some(notification) = queue.recv().await {
            let chat_ids = authorized.borrow().clone();
            let delivery = self.deliver(&notification, max_age, &chat_ids).await;
            let _ = ack
                .send(DeliveryAck {
                    key: notification.key,
                    delivery,
                })
                .await;
        }
    }

    pub async fn run_status(
        self,
        status: watch::Receiver<String>,
        authorized_tx: watch::Sender<Vec<i64>>,
        mut authorized: AuthorizedChats,
    ) {
        let url = format!("https://api.telegram.org/bot{}/getUpdates", self.token);
        let mut offset = None;
        let mut backoff = 1_u64;
        let mut pending: HashMap<i64, Instant> = HashMap::new();
        let mut failed_attempts: HashMap<i64, LoginAttempt> = HashMap::new();
        loop {
            let mut request = self
                .client
                .get(&url)
                .query(&[("timeout", "25"), ("allowed_updates", r#"["message"]"#)])
                .timeout(Duration::from_secs(35));
            if let Some(offset) = offset {
                request = request.query(&[("offset", offset)]);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(_) => {
                    eprintln!("[telegram] /status polling request failed");
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                    continue;
                }
            };
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                let body = response
                    .json::<serde_json::Value>()
                    .await
                    .unwrap_or_default();
                let delay = body["parameters"]["retry_after"]
                    .as_u64()
                    .unwrap_or(backoff);
                tokio::time::sleep(Duration::from_secs(delay.clamp(1, 3600))).await;
                continue;
            }
            if !response.status().is_success() {
                let status = response.status();
                if status == StatusCode::CONFLICT {
                    eprintln!("[telegram] /status polling needs webhook disabled");
                } else {
                    eprintln!("[telegram] /status polling HTTP {status}");
                }
                if status.is_client_error() {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
                continue;
            }
            let body = match response.json::<serde_json::Value>().await {
                Ok(body) if body["ok"] == true => body,
                _ => {
                    eprintln!("[telegram] /status polling returned an invalid response");
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                    continue;
                }
            };
            backoff = 1;
            for update in body["result"].as_array().into_iter().flatten() {
                let now = Instant::now();
                pending
                    .retain(|_, started| now.duration_since(*started) < Duration::from_secs(600));
                failed_attempts.retain(|_, attempt| !attempt.is_expired(now));
                if let Some(id) = update["update_id"].as_i64() {
                    offset = Some(id + 1);
                }
                let message = &update["message"];
                if message["chat"]["type"] != "private" {
                    continue;
                }
                let Some(chat_id) = message["chat"]["id"].as_i64() else {
                    continue;
                };
                let Some(text) = message["text"].as_str() else {
                    continue;
                };

                if is_start_command(text) {
                    if authorized.contains(chat_id) {
                        self.send_reply(
                            chat_id,
                            "✅ 已授权，可以使用 /status；发送 /stop 可取消接收告警。",
                        )
                        .await;
                    } else {
                        if failed_attempts
                            .get_mut(&chat_id)
                            .is_some_and(|attempt| !attempt.can_start(now))
                        {
                            self.send_reply(chat_id, "尝试次数过多，请稍后再试。").await;
                            continue;
                        }
                        pending.insert(chat_id, now);
                        self.send_reply(chat_id, "请输入访问密码以启用监控告警。")
                            .await;
                    }
                    continue;
                }

                if is_stop_command(text) {
                    pending.remove(&chat_id);
                    failed_attempts.remove(&chat_id);
                    match authorized.remove(chat_id) {
                        Ok(true) => {
                            authorized_tx.send_replace(authorized.ids());
                            self.send_reply(chat_id, "已停用，不会再收到监控告警。")
                                .await;
                        }
                        Ok(false) => self.send_reply(chat_id, "当前聊天尚未授权。").await,
                        Err(error) => {
                            eprintln!("[telegram] authorization state save failed: {error}");
                            self.send_reply(chat_id, "停用失败，请稍后重试。").await;
                        }
                    }
                    continue;
                }

                if is_status_command(text) {
                    if authorized.contains(chat_id) {
                        let text = status.borrow().clone();
                        self.send_status(chat_id, &text).await;
                    } else {
                        self.send_reply(chat_id, "请先发送 /start 并输入访问密码。")
                            .await;
                    }
                    continue;
                }

                if !pending.contains_key(&chat_id) {
                    continue;
                }
                if failed_attempts
                    .get(&chat_id)
                    .is_some_and(|attempt| attempt.is_locked(Instant::now()))
                {
                    self.send_reply(chat_id, "尝试次数过多，请稍后再试。").await;
                    continue;
                }
                if text.trim() == self.access_password {
                    pending.remove(&chat_id);
                    failed_attempts.remove(&chat_id);
                    match authorized.add(chat_id) {
                        Ok(()) => {
                            authorized_tx.send_replace(authorized.ids());
                            self.send_reply(chat_id, "✅ 密码正确，已启用监控告警。发送 /status 查看状态，/stop 可取消。")
                                .await;
                        }
                        Err(error) => {
                            eprintln!("[telegram] authorization state save failed: {error}");
                            self.send_reply(chat_id, "授权保存失败，请稍后发送 /start 重试。")
                                .await;
                        }
                    }
                } else {
                    let attempt = failed_attempts.entry(chat_id).or_default();
                    if attempt.fail(Instant::now()) {
                        pending.remove(&chat_id);
                        self.send_reply(chat_id, "密码错误过多，已锁定 10 分钟。")
                            .await;
                    } else {
                        self.send_reply(chat_id, &format!("密码错误（{}/5）。", attempt.failures))
                            .await;
                    }
                }
            }
        }
    }

    async fn send_status(&self, chat_id: i64, text: &str) {
        self.send_text(chat_id, text, true).await;
    }

    async fn send_reply(&self, chat_id: i64, text: &str) {
        self.send_text(chat_id, text, false).await;
    }

    async fn send_text(&self, chat_id: i64, text: &str, html: bool) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        let mut body = json!({
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": true
        });
        if html {
            body["parse_mode"] = json!("HTML");
        }
        let response = self.client.post(url).json(&body).send().await;
        match response {
            Ok(response) if response.status() == StatusCode::OK => {
                if !response
                    .json::<serde_json::Value>()
                    .await
                    .is_ok_and(|body| body["ok"] == true)
                {
                    eprintln!("[telegram] /status reply rejected");
                }
            }
            Ok(response) => eprintln!("[telegram] /status reply HTTP {}", response.status()),
            Err(_) => eprintln!("[telegram] /status reply failed"),
        }
    }

    async fn deliver(
        &self,
        notification: &Notification,
        max_age: Duration,
        chat_ids: &[i64],
    ) -> Delivery {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        self.deliver_at(notification, max_age, chat_ids, &url).await
    }

    async fn deliver_at(
        &self,
        notification: &Notification,
        max_age: Duration,
        chat_ids: &[i64],
        url: &str,
    ) -> Delivery {
        if chat_ids.is_empty() {
            return Delivery::Stale;
        }
        // ponytail: cap fanout at eight concurrent sends; raise for larger subscriber lists.
        let mut pending = chat_ids.to_vec();
        let mut sent = false;
        let mut stale = false;
        let mut permanently_rejected = false;
        let mut retryable = None;
        while !pending.is_empty() && notification.sampled_at.elapsed() <= max_age {
            let batch = std::mem::take(&mut pending);
            let results = futures_util::stream::iter(batch)
                .map(|chat_id| async move {
                    (
                        chat_id,
                        self.deliver_to_at(notification, max_age, chat_id, url)
                            .await,
                    )
                })
                .buffer_unordered(8)
                .collect::<Vec<_>>()
                .await;
            for (chat_id, result) in results {
                match result {
                    Delivery::Sent(_) => sent = true,
                    Delivery::Stale => stale = true,
                    Delivery::Retryable { reason, after } => {
                        pending.push(chat_id);
                        if retryable
                            .as_ref()
                            .is_none_or(|(_, current)| after > *current)
                        {
                            retryable = Some((reason, after));
                        }
                    }
                    Delivery::Permanent(reason) => {
                        permanently_rejected = true;
                        eprintln!("[telegram] delivery rejected for one authorized chat: {reason}");
                    }
                }
            }
            if !pending.is_empty() {
                let Some((_, after)) = &retryable else { break };
                let remaining = max_age.saturating_sub(notification.sampled_at.elapsed());
                if *after >= remaining {
                    break;
                }
                tokio::time::sleep(*after).await;
            }
        }
        if !pending.is_empty() {
            let (reason, after) = retryable.unwrap_or_else(|| {
                (
                    "Telegram delivery remained pending".into(),
                    Duration::from_secs(30),
                )
            });
            Delivery::Retryable { reason, after }
        } else if stale && sent {
            Delivery::Retryable {
                reason: "some authorized chats did not receive a fresh alert".into(),
                after: Duration::from_secs(1),
            }
        } else if sent {
            Delivery::Sent(Utc::now())
        } else if stale {
            Delivery::Stale
        } else if permanently_rejected {
            Delivery::Permanent("Telegram rejected delivery for all authorized chats".into())
        } else {
            Delivery::Stale
        }
    }

    async fn deliver_to_at(
        &self,
        notification: &Notification,
        max_age: Duration,
        chat_id: i64,
        url: &str,
    ) -> Delivery {
        for attempt in 0..4 {
            if notification.sampled_at.elapsed() > max_age {
                return Delivery::Stale;
            }
            let response = self
                .client
                .post(url)
                .json(&json!({
                    "chat_id": chat_id,
                    "text": message(notification),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": true
                }))
                .send()
                .await;
            match response {
                Ok(response) if response.status() == StatusCode::OK => {
                    match response.json::<serde_json::Value>().await {
                        Ok(body) if body["ok"] == true => return Delivery::Sent(Utc::now()),
                        Ok(body) => {
                            return Delivery::Permanent(
                                body["description"]
                                    .as_str()
                                    .unwrap_or("Telegram rejected message")
                                    .into(),
                            )
                        }
                        Err(_) => {
                            return Delivery::Retryable {
                                reason: "invalid Telegram response".into(),
                                after: Duration::from_secs(30),
                            }
                        }
                    }
                }
                Ok(response) if response.status() == StatusCode::TOO_MANY_REQUESTS => {
                    let body = response
                        .json::<serde_json::Value>()
                        .await
                        .unwrap_or_default();
                    let delay = body["parameters"]["retry_after"]
                        .as_u64()
                        .unwrap_or(5)
                        .clamp(1, 3600);
                    return Delivery::Retryable {
                        reason: "Telegram rate limited".into(),
                        after: Duration::from_secs(delay),
                    };
                }
                Ok(response) if response.status().is_server_error() => {
                    let status = response.status();
                    if attempt == 3 {
                        return Delivery::Retryable {
                            reason: format!("Telegram HTTP {status}"),
                            after: Duration::from_secs(30),
                        };
                    }
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                }
                Ok(response) => {
                    return Delivery::Permanent(format!("Telegram HTTP {}", response.status()))
                }
                Err(_) => {
                    if attempt == 3 {
                        return Delivery::Retryable {
                            reason: "Telegram network request failed".into(),
                            after: Duration::from_secs(30),
                        };
                    }
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                }
            }
        }
        Delivery::Retryable {
            reason: "Telegram retry limit reached".into(),
            after: Duration::from_secs(30),
        }
    }
}

fn is_placeholder(value: &str) -> bool {
    value.starts_with('<') && value.ends_with('>')
}

pub fn message(notification: &Notification) -> String {
    let magnitude = ((Decimal::ONE - notification.price) * Decimal::from(100)).round_dp(2);
    let base = escape_html(&notification.base);
    format!(
        "🚨 <b>PRICE DEVIATION ALERT</b>\n<b>{base} / USDT</b>\nPrice: <b>{:.4}</b>\nFair Price: <b>1.0000</b>\nDeviation: <b>–{magnitude:.2}%</b>\n📍 {}\n⏱️ {}\n⚠️ {base} is trading <b>{magnitude:.2}% below</b> its estimated fair value.",
        notification.price.round_dp(4),
        escape_html(&notification.exchange),
        notification.confirmed_at.format("%H:%M:%S UTC"),
    )
}

pub(crate) fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn is_start_command(text: &str) -> bool {
    is_command(text, "/start")
}

fn is_stop_command(text: &str) -> bool {
    is_command(text, "/stop")
}

fn is_status_command(text: &str) -> bool {
    is_command(text, "/status")
}

fn is_command(text: &str, expected: &str) -> bool {
    text.split_whitespace()
        .next()
        .and_then(|command| command.split('@').next())
        == Some(expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
    };

    fn example(price: &str) -> Notification {
        Notification {
            key: "binance|USDe/USDT|down".into(),
            base: "USDe".into(),
            exchange: "Binance".into(),
            price: price.parse().unwrap(),
            confirmed_at: Utc.with_ymd_and_hms(2025, 1, 2, 5, 3, 29).unwrap(),
            sampled_at: Instant::now(),
        }
    }

    fn request_body(stream: &mut TcpStream) -> serde_json::Value {
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let count = stream.read(&mut buffer).unwrap();
            request.extend_from_slice(&buffer[..count]);
            let header_end = request.windows(4).position(|part| part == b"\r\n\r\n");
            if let Some(header_end) = header_end {
                let header = String::from_utf8_lossy(&request[..header_end]);
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|length| length.parse::<usize>().ok())
                    })
                    .unwrap();
                if request.len() >= header_end + 4 + length {
                    return serde_json::from_slice(
                        &request[header_end + 4..header_end + 4 + length],
                    )
                    .unwrap();
                }
            }
        }
    }

    #[test]
    fn screenshot_example_has_the_required_eight_lines() {
        let rendered = message(&example("0.9498"));
        assert_eq!(
            rendered,
            "🚨 <b>PRICE DEVIATION ALERT</b>\n<b>USDe / USDT</b>\nPrice: <b>0.9498</b>\nFair Price: <b>1.0000</b>\nDeviation: <b>–5.02%</b>\n📍 Binance\n⏱️ 05:03:29 UTC\n⚠️ USDe is trading <b>5.02% below</b> its estimated fair value."
        );
    }

    #[test]
    fn downward_alert_uses_en_dash_and_below() {
        let rendered = message(&example("0.994"));
        assert!(rendered.contains("🚨 <b>PRICE DEVIATION ALERT</b>"));
        assert!(rendered.contains("Deviation: <b>–0.60%</b>"));
        assert!(rendered.ends_with("USDe is trading <b>0.60% below</b> its estimated fair value."));
    }

    #[test]
    fn status_command_matches_private_and_bot_addressed_forms() {
        assert!(is_status_command("/status"));
        assert!(is_status_command("/status@DepegBot"));
        assert!(is_start_command("/start@DepegBot"));
        assert!(is_stop_command("/stop"));
        assert!(!is_status_command("/start"));
        assert!(!is_status_command("status"));
    }

    #[test]
    fn example_telegram_placeholders_are_rejected_at_startup() {
        assert!(Telegram::from_config(&crate::config::Config::default().telegram).is_err());
    }

    #[test]
    fn password_attempts_lock_for_ten_minutes_after_five_failures() {
        let start = Instant::now();
        let mut attempt = LoginAttempt::default();
        for _ in 0..4 {
            assert!(!attempt.fail(start));
        }
        assert!(attempt.can_start(start));
        assert_eq!(attempt.failures, 4);
        assert!(attempt.fail(start));
        assert!(!attempt.can_start(start + Duration::from_secs(599)));
        assert!(attempt.is_locked(start + Duration::from_secs(599)));
        assert!(attempt.can_start(start + Duration::from_secs(600)));
        assert_eq!(attempt.failures, 0);
        assert!(!attempt.is_locked(start + Duration::from_secs(600)));
    }

    #[tokio::test]
    async fn telegram_429_uses_retry_after() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 4096];
            let _ = stream.read(&mut request).unwrap();
            let body = r#"{"ok":false,"parameters":{"retry_after":7}}"#;
            write!(
                stream,
                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let telegram = Telegram {
            client: Client::new(),
            token: "unused".into(),
            access_password: "test-secret".into(),
        };
        let notification = example("0.994");
        let result = telegram
            .deliver_to_at(
                &notification,
                Duration::from_secs(10),
                123,
                &format!("http://{address}/sendMessage"),
            )
            .await;
        server.join().unwrap();
        assert!(matches!(
            result,
            Delivery::Retryable { after, .. } if after == Duration::from_secs(7)
        ));
    }

    #[tokio::test]
    async fn each_authorized_chat_receives_the_alert() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut chat_ids = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                chat_ids.push(request_body(&mut stream)["chat_id"].as_i64().unwrap());
                let body = r#"{"ok":true}"#;
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            chat_ids.sort();
            chat_ids
        });
        let telegram = Telegram {
            client: Client::new(),
            token: "unused".into(),
            access_password: "test-secret".into(),
        };
        let result = telegram
            .deliver_at(
                &example("0.994"),
                Duration::from_secs(10),
                &[123, 456],
                &format!("http://{address}/sendMessage"),
            )
            .await;
        assert!(matches!(result, Delivery::Sent(_)));
        assert_eq!(server.join().unwrap(), [123, 456]);
    }

    #[tokio::test]
    async fn alerts_are_not_delivered_before_a_chat_is_authorized() {
        let telegram = Telegram {
            client: Client::new(),
            token: "unused".into(),
            access_password: "test-secret".into(),
        };
        assert!(matches!(
            telegram
                .deliver(&example("0.994"), Duration::from_secs(10), &[])
                .await,
            Delivery::Stale
        ));
    }
}
