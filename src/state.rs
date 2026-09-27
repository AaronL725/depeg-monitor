use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IncidentState {
    pub notified_level: u8,
    pub last_sent_at: Option<DateTime<Utc>>,
}

pub struct StateFile {
    path: PathBuf,
    entries: HashMap<String, IncidentState>,
}

impl StateFile {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let entries: HashMap<String, IncidentState> = match fs::read_to_string(&path) {
            Ok(contents) => {
                serde_json::from_str(&contents).map_err(|e| format!("{}: {e}", path.display()))?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => return Err(format!("{}: {error}", path.display())),
        };
        if let Some((key, _)) = entries.iter().find(|(_, state)| state.notified_level > 2) {
            return Err(format!(
                "{}: invalid notification level for {key}",
                path.display()
            ));
        }
        Ok(Self { path, entries })
    }

    pub fn get(&self, key: &str) -> IncidentState {
        self.entries.get(key).cloned().unwrap_or_default()
    }

    pub fn set(&mut self, key: String, state: IncidentState) -> Result<(), String> {
        if state.notified_level == 0 {
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
            let Some((market_key, _)) = key.rsplit_once('|') else {
                return true;
            };
            !market_key.starts_with(&venue_prefix) || active_market_keys.contains(market_key)
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
    fn persisted_level_survives_reopen_and_recovery_removes_it() {
        let path = std::env::temp_dir().join(format!("depeg-state-{}.json", std::process::id()));
        let mut state = StateFile::load(&path).unwrap();
        state
            .set(
                "binance|USDe/USDT|down".into(),
                IncidentState {
                    notified_level: 2,
                    last_sent_at: Some(Utc::now()),
                },
            )
            .unwrap();
        let loaded = StateFile::load(&path).unwrap();
        assert_eq!(loaded.get("binance|USDe/USDT|down").notified_level, 2);
        let mut loaded = loaded;
        loaded
            .set("binance|USDe/USDT|down".into(), IncidentState::default())
            .unwrap();
        assert_eq!(
            StateFile::load(&path)
                .unwrap()
                .get("binance|USDe/USDT|down")
                .notified_level,
            0
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_persisted_level_fails_loading_instead_of_suppressing_alerts() {
        let path = std::env::temp_dir().join(format!(
            "depeg-state-{}-invalid-level.json",
            std::process::id()
        ));
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
                        notified_level: 1,
                        last_sent_at: Some(Utc::now()),
                    },
                )
                .unwrap();
        }
        state
            .retain_markets("binance", &["binance|USDC/USDT".into()].into())
            .unwrap();
        let state = StateFile::load(&path).unwrap();
        assert_eq!(state.get("binance|USDe/USDT|down").notified_level, 0);
        assert_eq!(state.get("binance|USDe/USDT|up").notified_level, 0);
        assert_eq!(state.get("binance|USDC/USDT|down").notified_level, 1);
        fs::remove_file(path).unwrap();
    }
}
