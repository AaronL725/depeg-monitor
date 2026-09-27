# 稳定币脱锚监控

Rust 常驻程序，通过固定版本的 CCXT Rust `ccxt-pro` 读取 Binance、OKX、Bitget、Bybit、Gate 的现货 L2 盘口，并把稳定币相对 1 USDT 的偏离发送到 Telegram。

## 行为

- 自动发现 `config.toml` 白名单中以 USDT、USDC 计价的活跃现货市场；排除合约、反向 USDT 市场和同币对。
- 下偏使用买一，上偏使用卖一。USDC 计价价格乘以同所 USDC/USDT 卖一价；公允价固定为 1 USDT。
- 两档分别连续确认 5 秒。满足第二档时可直接发送第二档；升档立即通知。异常每 30 分钟发送一次最新状态；对应方向偏离严格低于 0.4% 并持续 30 秒后结束异常，不发恢复消息。
- 盘口无效或超过 15 秒未更新时暂停判断并清除未完成计时。重连后重新确认，已发送等级保留。
- Telegram 固定八行 HTML，按截图格式发送。第一档标题 ⚠️、第二档 🚨；USDC 交易对显示 `USDe / USDC → USDT`。
- 用户私聊 bot 发送 `/start` 并输入访问密码后，该聊天会持久授权并接收告警；`/stop` 取消授权。只有授权的私聊可以使用 `/status`。程序只监听私聊，避免把密码放进群聊。`getUpdates` 长轮询要求 bot 不配置 webhook。
- 授权聊天保存在 `telegram.authorized_chats_path`。本机 `config.toml` 包含 Bot Token 和访问密码，已加入 `.gitignore`；提交和分享时只使用占位符模板 `config.example.toml`。Bot ID 已由 token 标识，无需另行配置。
- 告警会发送到所有已授权私聊。若部分发送遇到短暂错误，可能重试整个事件，已收到的聊天有机会看到重复告警。

可调阈值、Telegram token 和访问密码都在本机 `config.toml`。该文件已加入 `.gitignore`；从 [config.example.toml](/Users/aaronliang/Documents/Projects/depeg-monitor/config.example.toml) 复制后，用你自己的值替换尖括号占位符。状态保存在 `state_path` 指定的 JSON 文件，不保存盘口历史。没有订单执行、DEX、网页或数据库。

## 日志和磁盘

程序日志写到 stderr，由 systemd 收进 journald；程序不会逐条记录盘口，也不会生成持续追加的应用日志文件。service 对本 unit 设置 journald 速率限制，并限制短时间内连续启动失败。journal 总保留空间仍由服务器的 journald 配置决定，可用 `journalctl --disk-usage` 查看。

告警 JSON 只覆盖写入当前未恢复事件的通知状态，恢复或市场退市后移除；授权聊天 JSON 只保存已授权的私聊 ID，用户发送 `/stop` 后移除。授权文件随授权人数增长。`target/` 是编译缓存，不是运行时数据；部署后如不再本机重编译，可运行 `cargo clean` 删除它。

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
