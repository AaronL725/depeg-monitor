use crate::config::MonitorSettings;
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IncidentState {
    #[serde(
        default,
        alias = "notified_level",
        deserialize_with = "deserialize_notified"
    )]
    pub notified: bool,
    pub last_sent_at: Option<DateTime<Utc>>,
}

fn deserialize_notified<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Bool(bool),
        LegacyLevel(u8),
    }

    match Value::deserialize(deserializer)? {
        Value::Bool(notified) => Ok(notified),
        Value::LegacyLevel(level) if level <= 2 => Ok(level == 2),
        Value::LegacyLevel(_) => Err(D::Error::custom("invalid legacy notification level")),
    }
}

pub struct StateFile {
    path: PathBuf,
    entries: HashMap<String, IncidentState>,
}

pub fn load_monitor_settings(
    path: &Path,
    defaults: MonitorSettings,
) -> Result<MonitorSettings, String> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            serde_json::from_str(&contents).map_err(|e| format!("{}: {e}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(defaults),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

pub fn save_monitor_settings(path: &Path, settings: &MonitorSettings) -> Result<(), String> {
    save_json(path, settings)
}

impl StateFile {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let mut entries: HashMap<String, IncidentState> = match fs::read_to_string(&path) {
            Ok(contents) => {
                serde_json::from_str(&contents).map_err(|e| format!("{}: {e}", path.display()))?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => return Err(format!("{}: {error}", path.display())),
        };
        entries.retain(|_, state| state.notified);
        Ok(Self { path, entries })
    }

    pub fn get(&self, key: &str) -> IncidentState {
        self.entries.get(key).cloned().unwrap_or_default()
    }

    pub fn set(&mut self, key: String, state: IncidentState) -> Result<(), String> {
        if !state.notified {
            self.entries.remove(&key);
        } else {
            self.entries.insert(key, state);
        }
        save_json(&self.path, &self.entries)
    }

    pub fn retain_markets(
        &mut self,
        venue: &str,
        active_market_keys: &HashSet<String>,
    ) -> Result<(), String> {
        let venue_prefix = format!("{venue}|");
        let old_len = self.entries.len();
        self.entries.retain(|key, _| {
            let Some((market_key, direction)) = key.rsplit_once('|') else {
                return true;
            };
            !market_key.starts_with(&venue_prefix)
                || (direction == "down" && active_market_keys.contains(market_key))
        });
        if self.entries.len() == old_len {
            return Ok(());
        }
        save_json(&self.path, &self.entries)
    }
}

pub struct AuthorizedChats {
    path: PathBuf,
    ids: Vec<i64>,
}

impl AuthorizedChats {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let ids = match fs::read_to_string(&path) {
            Ok(contents) => {
                serde_json::from_str(&contents).map_err(|e| format!("{}: {e}", path.display()))?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(format!("{}: {error}", path.display())),
        };
        Ok(Self { path, ids })
    }

    pub fn ids(&self) -> Vec<i64> {
        self.ids.clone()
    }

    pub fn contains(&self, id: i64) -> bool {
        self.ids.contains(&id)
    }

    pub fn add(&mut self, id: i64) -> Result<(), String> {
        if self.contains(id) {
            return Ok(());
        }
        let mut ids = self.ids.clone();
        ids.push(id);
        save_json(&self.path, &ids)?;
        self.ids = ids;
        Ok(())
    }

    pub fn remove(&mut self, id: i64) -> Result<bool, String> {
        if !self.contains(id) {
            return Ok(false);
        }
        let ids: Vec<_> = self
            .ids
            .iter()
            .copied()
            .filter(|saved| *saved != id)
            .collect();
        save_json(&self.path, &ids)?;
        self.ids = ids;
        Ok(true)
    }
}

fn save_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    fs::write(&temp, serde_json::to_vec(value).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    fs::rename(temp, path).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_state_survives_reopen_and_recovery_removes_it() {
        let path = std::env::temp_dir().join(format!("depeg-state-{}.json", std::process::id()));
        let mut state = StateFile::load(&path).unwrap();
        state
            .set(
                "binance|USDe/USDT|down".into(),
                IncidentState {
                    notified: true,
                    last_sent_at: Some(Utc::now()),
                },
            )
            .unwrap();
        let loaded = StateFile::load(&path).unwrap();
        assert!(loaded.get("binance|USDe/USDT|down").notified);
        let mut loaded = loaded;
        loaded
            .set("binance|USDe/USDT|down".into(), IncidentState::default())
            .unwrap();
        assert!(
            !StateFile::load(&path)
                .unwrap()
                .get("binance|USDe/USDT|down")
                .notified
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn monitor_settings_load_defaults_then_persist_changes() {
        let path = std::env::temp_dir().join(format!("depeg-settings-{}.json", std::process::id()));
        let defaults = MonitorSettings {
            exchanges: vec!["binance".into()],
            stablecoins: vec!["USDe".into()],
        };
        assert_eq!(
            load_monitor_settings(&path, defaults.clone()).unwrap(),
            defaults
        );
        let selected = MonitorSettings {
            exchanges: vec!["binance".into(), "gate".into()],
            stablecoins: vec!["USDe".into(), "DAI".into()],
        };
        save_monitor_settings(&path, &selected).unwrap();
        assert_eq!(load_monitor_settings(&path, defaults).unwrap(), selected);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn old_notification_state_migrates_and_invalid_level_fails_loading() {
        let path = std::env::temp_dir().join(format!(
            "depeg-state-{}-invalid-level.json",
            std::process::id()
        ));
        fs::write(
            &path,
            r#"{"binance|USDe/USDT|down":{"notified_level":1,"last_sent_at":null}}"#,
        )
        .unwrap();
        assert!(
            !StateFile::load(&path)
                .unwrap()
                .get("binance|USDe/USDT|down")
                .notified
        );
        fs::write(
            &path,
            r#"{"binance|USDe/USDT|down":{"notified_level":2,"last_sent_at":null}}"#,
        )
        .unwrap();
        assert!(
            StateFile::load(&path)
                .unwrap()
                .get("binance|USDe/USDT|down")
                .notified
        );
        fs::write(
            &path,
            r#"{"binance|USDe/USDT|down":{"notified_level":255,"last_sent_at":null}}"#,
        )
        .unwrap();
        assert!(StateFile::load(&path).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn authorized_chats_survive_restart_and_can_unsubscribe() {
        let path =
            std::env::temp_dir().join(format!("depeg-authorized-{}.json", std::process::id()));
        let _ = fs::remove_file(&path);
        let mut chats = AuthorizedChats::load(&path).unwrap();
        chats.add(123).unwrap();
        chats.add(456).unwrap();
        chats.add(123).unwrap();
        assert_eq!(chats.ids(), [123, 456]);

        let mut chats = AuthorizedChats::load(&path).unwrap();
        assert!(chats.contains(123));
        assert!(chats.remove(123).unwrap());
        assert!(!chats.remove(123).unwrap());
        assert_eq!(AuthorizedChats::load(&path).unwrap().ids(), [456]);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn delisted_market_alerts_are_removed_without_touching_other_markets() {
        let path =
            std::env::temp_dir().join(format!("depeg-state-{}-delisted.json", std::process::id()));
        let _ = fs::remove_file(&path);
        let mut state = StateFile::load(&path).unwrap();
        for key in [
            "binance|USDe/USDT|down",
            "binance|USDe/USDT|up",
            "binance|USDC/USDT|down",
        ] {
            state
                .set(
                    key.into(),
                    IncidentState {
                        notified: true,
                        last_sent_at: Some(Utc::now()),
                    },
                )
                .unwrap();
        }
        state
            .retain_markets("binance", &["binance|USDC/USDT".into()].into())
            .unwrap();
        let state = StateFile::load(&path).unwrap();
        assert!(!state.get("binance|USDe/USDT|down").notified);
        assert!(!state.get("binance|USDe/USDT|up").notified);
        assert!(state.get("binance|USDC/USDT|down").notified);
        fs::remove_file(path).unwrap();
    }
}
