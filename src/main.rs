mod alert;
mod config;
mod engine;
mod market;
mod state;

use crate::{
    alert::Telegram,
    config::Config,
    engine::Engine,
    state::{AuthorizedChats, StateFile},
};
use std::collections::HashSet;
use std::{env, path::PathBuf, time::Duration};
#[cfg(unix)]
use std::{future::Future, pin::Pin};
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("[fatal] {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .or_else(|| env::var_os("DEPEG_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let config = Config::load(&config_path)?;
    if config.exchanges.iter().collect::<HashSet<_>>().len() != config.exchanges.len() {
        return Err("exchange ids must be unique; duplicate clients share websocket state".into());
    }
    for venue in &config.exchanges {
        if !market::supported(venue) {
            return Err(format!("unsupported exchange id in config: {venue}"));
        }
    }
    let telegram = Telegram::from_config(&config.telegram)?;
    let state = StateFile::load(&config.state_path)?;
    let authorized = AuthorizedChats::load(&config.telegram.authorized_chats_path)?;
    let (feeds_tx, feeds_rx) = mpsc::channel(256);
    let (notifications_tx, notifications_rx) = mpsc::channel(config.notification_queue_capacity);
    let (ack_tx, ack_rx) = mpsc::channel(config.notification_queue_capacity);
    let (status_tx, status_rx) = watch::channel(String::new());
    let (authorized_tx, authorized_rx) = watch::channel(authorized.ids());

    tokio::spawn(
        telegram
            .clone()
            .run_status(status_rx, authorized_tx, authorized),
    );
    tokio::spawn(telegram.run(
        notifications_rx,
        ack_tx,
        Duration::from_secs(config.max_quote_age_seconds),
        authorized_rx.clone(),
    ));
    for venue in config.exchanges.iter().cloned() {
        tokio::spawn(market::run_venue(venue, config.clone(), feeds_tx.clone()));
    }
    drop(feeds_tx);

    #[cfg(unix)]
    let shutdown: Pin<Box<dyn Future<Output = ()> + Send>> = {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|e| e.to_string())?;
        Box::pin(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
        })
    };
    #[cfg(not(unix))]
    let shutdown: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async {
            let _ = tokio::signal::ctrl_c().await;
        });

    Engine::new(config, state, notifications_tx, status_tx, authorized_rx)
        .run(feeds_rx, ack_rx, shutdown)
        .await;
    Ok(())
}
