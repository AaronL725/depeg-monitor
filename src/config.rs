use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

pub const SUPPORTED_EXCHANGES: [&str; 5] = ["binance", "okx", "bitget", "bybit", "gate"];
pub const MONITOR_SETTINGS_PATH: &str = "state/monitor_settings.json";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MonitorSettings {
    pub exchanges: Vec<String>,
    pub stablecoins: Vec<String>,
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub exchanges: Vec<String>,
    pub stablecoins: Vec<String>,
    pub depeg_bps: i64,
    pub confirmation_seconds: u64,
    pub reminder_seconds: u64,
    pub recovery_bps: i64,
    pub recovery_seconds: u64,
    pub max_quote_age_seconds: u64,
    pub evaluation_interval_ms: u64,
    pub market_refresh_seconds: u64,
    pub notification_queue_capacity: usize,
    pub state_path: String,
    pub telegram: TelegramConfig,
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub access_password: String,
    pub authorized_chats_path: String,
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self {
            bot_token: "<YOUR_TELEGRAM_BOT_TOKEN>".into(),
            access_password: "<YOUR_ACCESS_PASSWORD>".into(),
            authorized_chats_path: "state/authorized_chats.json".into(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            exchanges: SUPPORTED_EXCHANGES.map(str::to_owned).into(),
            stablecoins: ["USDC", "USDe", "DAI", "FDUSD", "PYUSD"]
                .map(str::to_owned)
                .into(),
            depeg_bps: 100,
            confirmation_seconds: 5,
            reminder_seconds: 1800,
            recovery_bps: 40,
            recovery_seconds: 30,
            max_quote_age_seconds: 15,
            evaluation_interval_ms: 250,
            market_refresh_seconds: 21600,
            notification_queue_capacity: 128,
            state_path: "state/alerts.json".into(),
            telegram: TelegramConfig::default(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let config: Self = toml::from_str(&raw).map_err(|e| e.to_string())?;
        if config.depeg_bps <= 0 {
            return Err("depeg_bps must be positive".into());
        }
        if config.recovery_bps <= 0 || config.recovery_bps >= config.depeg_bps {
            return Err("recovery_bps must satisfy 0 < recovery_bps < depeg_bps".into());
        }
        if config.confirmation_seconds == 0
            || config.reminder_seconds == 0
            || config.recovery_seconds == 0
            || config.max_quote_age_seconds == 0
            || config.evaluation_interval_ms == 0
            || config.market_refresh_seconds == 0
        {
            return Err("configured durations must be positive".into());
        }
        if config.exchanges.is_empty() || config.stablecoins.is_empty() {
            return Err("exchanges and stablecoins must not be empty".into());
        }
        if config.notification_queue_capacity == 0 {
            return Err("notification_queue_capacity must be positive".into());
        }
        if config.notification_queue_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("notification_queue_capacity exceeds Tokio's channel limit".into());
        }
        if config.state_path == config.telegram.authorized_chats_path {
            return Err("state_path and telegram.authorized_chats_path must differ".into());
        }
        if [
            config.state_path.as_str(),
            config.telegram.authorized_chats_path.as_str(),
        ]
        .contains(&MONITOR_SETTINGS_PATH)
        {
            return Err("monitor settings path must differ from other state paths".into());
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_confirmed_thresholds() {
        let config = Config::default();
        assert_eq!(config.depeg_bps, 100);
        assert_eq!(
            (config.confirmation_seconds, config.reminder_seconds),
            (5, 1800)
        );
    }

    #[test]
    fn example_config_loads_the_five_exchanges_and_white_list() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
        let config = Config::load(&path).unwrap();
        assert_eq!(
            config.exchanges,
            ["binance", "okx", "bitget", "bybit", "gate"]
        );
        assert!(config.stablecoins.iter().any(|coin| coin == "USDe"));
        assert_eq!(config.telegram.bot_token, "<YOUR_TELEGRAM_BOT_TOKEN>");
        assert_eq!(
            config.telegram.authorized_chats_path,
            "state/authorized_chats.json"
        );
    }

    #[test]
    fn queue_capacity_above_tokios_limit_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "depeg-config-{}-queue-limit.toml",
            std::process::id()
        ));
        let capacity = tokio::sync::Semaphore::MAX_PERMITS + 1;
        fs::write(&path, format!("notification_queue_capacity = {capacity}\n")).unwrap();
        assert!(Config::load(&path).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn alert_and_authorization_state_paths_must_differ() {
        let path = std::env::temp_dir().join(format!(
            "depeg-config-{}-state-paths.toml",
            std::process::id()
        ));
        fs::write(
            &path,
            "state_path = \"state/same.json\"\n[telegram]\nauthorized_chats_path = \"state/same.json\"\n",
        )
        .unwrap();
        assert!(Config::load(&path).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn monitor_settings_path_must_not_overlap_other_state_files() {
        let path = std::env::temp_dir().join(format!(
            "depeg-config-{}-monitor-settings-path.toml",
            std::process::id()
        ));
        fs::write(&path, "state_path = \"state/monitor_settings.json\"\n").unwrap();
        assert!(Config::load(&path).is_err());
        fs::remove_file(path).unwrap();
    }
}
