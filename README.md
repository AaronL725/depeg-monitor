# 稳定币脱锚监控

Rust 常驻程序，通过固定版本的 CCXT Rust `ccxt-pro` 读取 Binance、OKX、Bitget、Bybit、Gate 的现货 L2 盘口，并把稳定币相对 1 USDT 的偏离发送到 Telegram。

## 行为

- 每家交易所只监控其实际提供的白名单 USDT 活跃现货。币种和交易对匹配不区分大小写，大小写别名只订阅一次，订阅使用交易所原始 symbol，消息显示使用白名单币种名称。缺失的交易对不生成行情；以 USDC 为计价币的市场、合约、反向 USDT 市场和同币对会排除。
- 只监控向下偏离，使用 L2 卖一价（ask）判断。卖一低于固定公允价 1 USDT 达到 `depeg_bps`（默认 100 bp，即 1%）时开始确认；以 USDC 为计价币的市场不参与监控。
- 偏离连续确认 5 秒后发送告警；异常每 30 分钟发送一次最新状态；偏离严格低于 0.4% 并持续 30 秒后结束异常，不发恢复消息。
- 盘口无效或超过 15 秒未更新时暂停判断并清除未完成计时。无更新只说明本机没有收到该市场的新订单簿消息，不等同于交易所 WebSocket 已断开。交易所断连后会清掉该所缓存盘口，重连时各交易对收到新盘口后才恢复判断；已发送告警状态保留。
- Telegram 固定八行 HTML，按截图格式发送；标题使用 🚨，下偏使用 en dash 和 `below`。
- 用户私聊 bot 发送 `/start` 并输入访问密码后，该聊天会持久授权并接收告警；`/stop` 取消授权。只有授权的私聊可以使用 `/status` 和监控设置命令。状态会逐市场显示最近盘口年龄、无更新时间、失效或等待盘口。程序只监听私聊，避免把密码放进群聊。`getUpdates` 长轮询要求 bot 不配置 webhook。
- 已授权聊天可用 `/exchange binance off|on` 开关交易所、`/coin USDC off|on` 选择代币，或用 `/addcoin PYUSD` 添加代币；`/exchanges`、`/coins` 和 `/help` 查看设置。设置对所有授权聊天全局生效，保存在 `state/monitor_settings.json`，重启后保留；文件不存在时采用 `config.toml` 中的初始列表。修改会重启所有启用的行情任务并重新同步。
- 授权聊天保存在 `telegram.authorized_chats_path`。本机 `config.toml` 包含 Bot Token 和访问密码，已加入 `.gitignore`；提交和分享时只使用占位符模板 `config.example.toml`。Bot ID 已由 token 标识，无需另行配置。
- 告警会发送到所有已授权私聊。若部分发送遇到短暂错误，可能重试整个事件，已收到的聊天有机会看到重复告警。

## 代码结构

- `src/config.rs`：配置和监控范围类型。
- `src/market.rs`、`src/market/gate.rs`：交易所市场发现、L2 订阅和 Gate 多市场连接。
- `src/engine.rs`：盘口有效性、脱锚确认、恢复和告警状态。
- `src/alert.rs`、`src/alert/commands.rs`：Telegram 授权、消息发送、告警模板和 bot 控制命令。
- `src/state.rs`：告警、授权聊天和监控设置的 JSON 持久化。
- `src/main.rs`：配置加载、任务组装、行情任务重启和关闭处理。

可调阈值、Telegram token 和访问密码都在本机 `config.toml`。该文件已加入 `.gitignore`；从 `config.example.toml` 复制后，用你自己的值替换尖括号占位符。状态保存在 `state_path` 指定的 JSON 文件，不保存盘口历史。没有订单执行、DEX、网页或数据库。

## 日志和磁盘

程序日志写到 stderr，由 systemd 收进 journald；程序不会逐条记录盘口，也不会生成持续追加的应用日志文件。service 对本 unit 设置 journald 速率限制，并限制短时间内连续启动失败。journal 总保留空间仍由服务器的 journald 配置决定，可用 `journalctl --disk-usage` 查看。

告警 JSON 只覆盖写入当前未恢复事件的通知状态，恢复或市场退市后移除；授权聊天 JSON 只保存已授权的私聊 ID，用户发送 `/stop` 后移除；监控设置 JSON 保存 bot 选定的交易所和代币。上述运行文件位于 `state/`，已被 Git 忽略。授权文件随授权人数增长。`target/` 是编译缓存，不是运行时数据；部署后如不再本机重编译，可运行 `cargo clean` 删除它。

## 本机运行

需要 Rust stable、网络访问、Telegram bot token 和访问密码。公开行情不需要交易 API key。

```sh
[ -f config.toml ] || cp config.example.toml config.toml
chmod 600 config.toml
# 编辑 config.toml，填写 [telegram] 下的 bot_token 和 access_password
cargo test --locked
cargo run --release -- config.toml
```

Bot Token 从 Telegram 的 `@BotFather` 获取，填入 `bot_token`；访问密码填入 `access_password`。用户私聊 bot 发送 `/start`，收到提示后发送访问密码。输入错误 5 次后，该聊天会锁定 10 分钟。通过后即可接收后续监控消息并使用 `/status`；发送 `/stop` 可从授权列表移除自己。

如果需要运行真实行情 smoke test，它会访问所选交易所且不会下单。默认顺序覆盖五家；可以用 `DEPEG_SMOKE_EXCHANGES=okx,bitget,gate` 选择交易所。因 CCXT Rust 测试线程需要较大栈，运行示例：

```sh
DEPEG_SMOKE_EXCHANGES=okx,bitget,gate RUST_MIN_STACK=16777216 \
  cargo test --locked live_five_exchange_two_market_smoke -- --ignored --nocapture
```

## Debian + systemd

建议在目标 Debian 主机上构建，以匹配系统架构和 ABI。先将项目文件放到主机，再执行：

```sh
cargo build --release --locked
sudo install -m 0755 target/release/depeg-monitor /usr/local/bin/depeg-monitor
sudo groupadd --system depeg-monitor
sudo useradd --system --gid depeg-monitor --no-create-home --shell /usr/sbin/nologin depeg-monitor
sudo install -d -o root -g depeg-monitor -m 0750 /etc/depeg-monitor
sudo install -m 0644 deploy/depeg-monitor.service /etc/systemd/system/depeg-monitor.service
sudo install -o root -g depeg-monitor -m 0640 config.example.toml /etc/depeg-monitor/config.toml
sudoedit /etc/depeg-monitor/config.toml
```

在服务器配置中替换 `bot_token` 和 `access_password` 两个占位符。该文件权限设为 `0640`，不要提交或公开它。然后启动并查看日志：

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now depeg-monitor
sudo systemctl status depeg-monitor
sudo journalctl -u depeg-monitor -f
```

服务以普通用户运行；`StateDirectory` 管理 `/var/lib/depeg-monitor`，日志由 journald 保存。unit 限制单服务日志速率，并限制连续启动失败次数。journal 的总磁盘上限仍由服务器 journald 配置管理。停止服务时会处理 systemd 的 SIGTERM。

候选服务器地区必须先验证五家行情和 Telegram 的网络可达性。2026-09-27 日本出口 spot-only 复测中，Binance、Bybit、Gate 双市场 L2 正常；OKX、Bitget 的 CCXT Rust 市场目录读取报响应解码错误。两家的公开目录用 curl 均返回 HTTP 200 JSON，但不能据此证明 CCXT 能消费这些响应。此前美国出口的 Binance 451、Bybit 403 也记录在 [验证结果](VALIDATION.md)。目前还不能确认日本节点满足五家覆盖；实盘 48 小时观察和故障演练需要目标服务器。
