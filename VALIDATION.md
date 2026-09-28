# 验证结果

## 最新代码变更

2026-09-28 全仓检查：将 Telegram 监控命令拆到 `alert::commands`，复用代币校验，简化行情任务重启；修复关闭交易所后延迟通知回执可能恢复已清除状态的问题。此前已按大小写不敏感的规范化交易对去重、保留原始 symbol 订阅，并实现全局监控设置持久化及运行时重启。

- `cargo fmt --all -- --check`：通过。
- `cargo test --locked --offline -j 1`：39 项通过、0 失败、2 项联网 smoke 忽略。
- 回归覆盖逐市场盘口年龄、无更新状态和断线后逐市场重新同步。
- 回归覆盖大小写别名去重、监控设置持久化与运行时应用。
- 回归覆盖关闭交易所后延迟到达的通知回执不会重建告警状态。
- `cargo clippy --locked --offline --all-targets -j 1 -- -D warnings`：通过。
- `cargo build --release --locked --offline -j 1`：通过。
- 未重新运行五家交易所实时 smoke、Telegram 端到端验证或 48 小时部署观察。

## 历史网络观察

以下交易所结果来自规则调整前的代码，仅用于记录网络可达性。此前市场数量包含现已排除的 USDC 计价市场，不能作为当前覆盖数量。

2026-09-27 日本出口，CCXT Rust 4.5.84，spot-only 双市场 smoke：

| 交易所 | 历史结果 |
|---|---|
| Binance | 市场发现及两个 L2 流成功。 |
| OKX | CCXT Rust 市场目录返回 `NetworkError: error decoding response body`；未建立 L2 流。 |
| Bitget | CCXT Rust 市场目录返回 `NetworkError: error decoding response body`；未建立 L2 流。 |
| Bybit | 市场发现及两个 L2 流成功。 |
| Gate | 市场发现及两个 L2 流成功。 |

OKX spot instruments、Bitget spot symbols、Bitget margin currencies 和 Gate spot currency pairs 的只读 `curl` 请求均返回 HTTP 200 JSON；这不能证明 CCXT Rust HTTP 客户端能读取相同响应。仅凭 `error decoding response body` 不能区分短暂连接中断、响应传输问题或解压路径问题，也不能据此判定地区封锁。

美国出口的历史观察：Binance 市场发现返回 HTTP 451；Bybit 返回 HTTP 403；OKX、Bitget、Gate 的两个市场 L2 订阅成功。服务器地区仍需实际验证。

## 尚未完成的运行验证

- 未对 Telegram 做真实 `/start`、授权或告警端到端测试。
- systemd unit 尚未在 Debian 上验证；未连接目标服务器，未完成断线、重启演练或 48 小时观察。
- 规则修改后尚未重新运行实时交易所 smoke。
