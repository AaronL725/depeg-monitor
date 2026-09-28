use crate::config::{MonitorSettings, SUPPORTED_EXCHANGES};

pub(super) fn is_command(text: &str, expected: &str) -> bool {
    text.split_whitespace()
        .next()
        .and_then(|command| command.split('@').next())
        == Some(expected)
}

pub(super) fn command_args<'a>(text: &'a str, expected: &str) -> Option<Vec<&'a str>> {
    let mut words = text.split_whitespace();
    let command = words.next()?.split('@').next()?;
    (command == expected).then(|| words.collect())
}

pub(super) fn set_exchange(
    settings: &mut MonitorSettings,
    exchange: &str,
    action: &str,
) -> Result<(), &'static str> {
    let Some(exchange) = SUPPORTED_EXCHANGES
        .iter()
        .find(|id| exchange.eq_ignore_ascii_case(id))
    else {
        return Err("请使用支持的交易所 ID。");
    };
    if action.eq_ignore_ascii_case("on") {
        if !settings
            .exchanges
            .iter()
            .any(|id| id.eq_ignore_ascii_case(exchange))
        {
            settings.exchanges.push((*exchange).into());
        }
    } else if action.eq_ignore_ascii_case("off") {
        let Some(index) = settings
            .exchanges
            .iter()
            .position(|id| id.eq_ignore_ascii_case(exchange))
        else {
            return Err("该交易所当前已关闭。");
        };
        if settings.exchanges.len() == 1 {
            return Err("至少保留一家启用的交易所。");
        }
        settings.exchanges.remove(index);
    } else {
        return Err("请使用 on 或 off。");
    }
    Ok(())
}

pub(super) fn add_monitor_coin(
    settings: &mut MonitorSettings,
    coin: &str,
) -> Result<(), &'static str> {
    validate_coin(coin)?;
    if settings
        .stablecoins
        .iter()
        .any(|saved| saved.eq_ignore_ascii_case(coin))
    {
        return Err("该代币已在监控列表中。");
    }
    settings.stablecoins.push(coin.into());
    Ok(())
}

pub(super) fn set_monitor_coin(
    settings: &mut MonitorSettings,
    coin: &str,
    action: &str,
) -> Result<(), &'static str> {
    validate_coin(coin)?;
    let selected = settings
        .stablecoins
        .iter()
        .position(|saved| saved.eq_ignore_ascii_case(coin));
    if action.eq_ignore_ascii_case("on") {
        if selected.is_none() {
            settings.stablecoins.push(coin.into());
        }
    } else if action.eq_ignore_ascii_case("off") {
        let Some(index) = selected else {
            return Err("该代币当前不在监控列表中。");
        };
        if settings.stablecoins.len() == 1 {
            return Err("至少保留一个监控代币。");
        }
        settings.stablecoins.remove(index);
    } else {
        return Err("请使用 on 或 off。");
    }
    Ok(())
}

pub(super) fn exchange_list(settings: &MonitorSettings) -> String {
    let exchanges = SUPPORTED_EXCHANGES
        .iter()
        .map(|exchange| {
            let state = if settings.exchanges.iter().any(|enabled| enabled == exchange) {
                "✅"
            } else {
                "▫️"
            };
            format!("{state} {exchange}")
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("全局交易所监控设置：\n{exchanges}\n使用 /exchange <交易所 ID> on|off 修改。")
}

pub(super) fn monitor_help() -> &'static str {
    "/status 查看行情\n/exchanges 查看交易所\n/exchange <ID> on|off 开启或关闭交易所\n/coins 查看监控代币\n/coin <代币> on|off 选择代币\n/addcoin <代币> 添加代币\n这些设置对所有已授权聊天生效。"
}

fn validate_coin(coin: &str) -> Result<(), &'static str> {
    if coin.is_empty()
        || coin.len() > 20
        || !coin
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        || coin.eq_ignore_ascii_case("USDT")
    {
        return Err("代币符号无效，且不能添加 USDT。");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_accept_bot_addresses_and_validate_coin_symbols() {
        assert!(is_command("/status", "/status"));
        assert!(is_command("/status@DepegBot", "/status"));
        assert!(is_command("/start@DepegBot", "/start"));
        assert!(is_command("/stop", "/stop"));
        assert!(!is_command("/start", "/status"));
        assert!(!is_command("status", "/status"));
        assert_eq!(
            command_args("/exchange@DepegBot gate off", "/exchange"),
            Some(vec!["gate", "off"])
        );
        assert!(validate_coin("USDe").is_ok());
        assert!(validate_coin("USDe/USDT").is_err());
    }

    #[test]
    fn monitor_selection_is_case_insensitive_and_keeps_one_active_item() {
        let mut settings = MonitorSettings {
            exchanges: vec!["binance".into()],
            stablecoins: vec!["USDe".into()],
        };
        set_exchange(&mut settings, "GATE", "ON").unwrap();
        set_monitor_coin(&mut settings, "usde", "on").unwrap();
        add_monitor_coin(&mut settings, "DAI").unwrap();
        set_monitor_coin(&mut settings, "USDE", "off").unwrap();

        assert_eq!(settings.exchanges, ["binance", "gate"]);
        assert_eq!(settings.stablecoins, ["DAI"]);
        set_exchange(&mut settings, "binance", "off").unwrap();
        assert_eq!(settings.exchanges, ["gate"]);
        assert!(set_exchange(&mut settings, "gate", "off").is_err());
        assert!(set_monitor_coin(&mut settings, "DAI", "off").is_err());
    }
}
