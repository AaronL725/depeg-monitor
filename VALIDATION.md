# 验证结果

验证日期：2026-09-26。环境：macOS 27、aarch64、rustc/cargo 1.96.1；公网出口为美国地区。依赖固定在 `Cargo.lock`，CCXT Rust 版本为 4.5.84，仅启用五家交易所适配 feature。没有使用 API key 或交易接口。

## 日本出口复测（2026-09-27）

本机出口查询 `ipinfo.io/country` 返回 `JP`。使用五家现货双市场 smoke；无下单。初次结果为 Binance、OKX、Bybit 成功，Bitget 和 Gate 市场发现失败。

检查 CCXT 4.5.84 源码后，将五家的 `fetchMarkets.types` 限制为 `spot`，避免为现货监控额外请求衍生品目录。改动后又做了一次五家 smoke：

| 交易所 | spot-only 复测结果 |
|---|---|
| Binance | 5 个监控市场；两个 L2 流正常，USDC/USDT 锚定与 USDC 计价市场均可用。 |
| OKX | 市场发现返回 CCXT `NetworkError: error decoding response body`；未建立 L2 流。（spot-only 改动前的单次 smoke 曾成功。） |
| Bitget | 市场发现返回 CCXT `NetworkError: error decoding response body`；未建立 L2 流。 |
| Bybit | 4 个监控市场；两个 L2 流正常，锚定及 USDC 计价市场均可用。 |
| Gate | 4 个监控市场；两个 L2 流正常，USDC/USDT 锚定可用；白名单中无 USDC 计价目标。（spot-only 改动前的单次 smoke 报过请求错误。） |

spot-only 复测后有 3/5 家通过。额外的只读 `curl` 检查中，OKX spot instruments、Bitget spot symbols、Bitget margin currencies 和 Gate spot currency pairs 均返回 HTTP 200 JSON；这不能证明 CCXT 的 Rust HTTP 客户端能完整读取同一响应。改动后的 OKX、Bitget 错误未再次重试。

在本机安装的 `ccxt-base` 4.5.84 源码中，`fetch_typed` 在 JSON 解析前调用 `reqwest::Response::text().await?`。因此这两个错误发生在响应体读取阶段；现有证据不能区分短暂连接中断、响应传输问题或 reqwest 解压路径，也没有依据把它们归因于地区封锁。

[Reqwest issue #2839](https://github.com/seanmonstar/reqwest/issues/2839) 记录了 body read timeout 也会显示为 `error decoding response body`，因此仅凭这条错误文本不能判定为 JSON 格式错误。

## 自动验证

- `cargo fmt --all -- --check`：通过。
- `cargo test --locked --offline -j 1`：38 项通过，0 失败；2 项联网 smoke test 默认忽略。
- `cargo clippy --locked --offline -j 1 -- -D warnings`：通过。
- `cargo build --release --locked --offline -j 1`：通过。
- `cargo tree --locked --offline -e features -i ccxt-pro`：仅列出 Binance、OKX、Bitget、Bybit、Gate 五个 feature。
- 覆盖截图消息逐字匹配、示例配置占位符检查、授权命令与密码尝试锁定、授权聊天持久化及退订、未授权时不发送、多个授权聊天告警扇出、`/status` 和五家状态显示、USDC 锚定盘口缺失时的状态提示、方向和阈值边界、5 秒确认/失效重置、30 分钟提醒和重启去重、429 `retry_after`、可重试发送冷却、队列满后重试、排队告警在恢复期间等待发送结果、状态保存失败与无效持久化等级、退市市场状态清理、Decimal 极值溢出、队列容量上限与状态路径冲突、升级、对应方向恢复边界与持续时间、USDC 卖一换算及过期来源、Gate 多市场盘口同步。

Gate 离线回归使用 CCXT 自带模拟 WebSocket，按“BTC 快照 → BTC 增量 → ETH 快照 → BTC 增量”的顺序确认同一个 `GateCore` 能保留 BTC 的增量；通过。Gate 必须保持单 Core 批量订阅，不能为每个市场创建独立实例。

## 美国出口实时行情 smoke

使用公开 REST 和 WebSocket，无下单。不可访问的 Binance、Bybit 未重试。Gate 和 Bitget 的连接及两市场 L2 订阅在 Tokio 默认工作线程栈下通过。

| 交易所 | 观察结果 |
|---|---|
| Binance | 市场发现返回 HTTP 451；响应说明当前地区不符合交易资格。未建立 L2 流。 |
| OKX | 两个监控现货市场的 L2 流正常；发现 USDC/USDT 锚定市场。白名单中没有可用 USDC 计价监控市场。 |
| Bitget | 四个监控现货市场的 L2 流正常；USDC/USDT 锚定盘口和 USDC 计价监控盘口均更新。 |
| Bybit | 市场发现返回 HTTP 403，CloudFront 表示当前国家/地区被拦截。未建立 L2 流。 |
| Gate | 四个监控现货市场的 L2 流正常；发现 USDC/USDT 锚定市场。白名单中没有可用 USDC 计价监控市场。 |

USDC 换算逻辑还由离线用例验证，严格使用同所 USDC/USDT 卖一。OKX、Gate 当前没有白名单 USDC 计价市场，因此这两所本次没有真实的换算目标盘口。

## Telegram 和部署限制

- 未对 Telegram 做真实 `/start`、授权或告警端到端测试；密码尝试规则、聊天持久化、扇出请求、示例占位符检查和模板由本地测试覆盖。
- 本机是 macOS，未安装 `systemd-analyze`。systemd unit 已提供，尚未在 Debian 上验证。
- 未连接目标服务器；无法进行部署、断线/重启演练或 48 小时观察。
- 美国出口下 Binance 和 Bybit 被地区限制。若目标服务器也使用美国出口，这两家不能提供行情；需在部署前选择允许访问且符合用户要求的地区并重新验证。
