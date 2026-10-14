# gscript-tunnel — kcptunB over Google Apps Script doPost

把 Google Apps Script 当成纯被动的 KCP 数据报中转总线。kcptunB
（diyism/kcptunB fork）的 client/server 代码被融合进两个二进制，
KCP 会话跑在**虚拟 PacketConn** 上——**零 UDP socket、零 unix socket、
零独立进程**。UDP 只剩下"包分发/合并"的逻辑（kcp-go 内部的 conv 分用），
没有任何真实 socket。

```
[app] --TCP(socks5)--> [gs_tun_client: TCP+smux+KCP client] --POST /exec--> [GAS: CacheService 队列]
                                                                                      ^
[任意目标] <--TCP-- [gs_tun_server: KCP server+smux+内建代理] --POST /exec----------------+
```

两端都只是 GAS 的 HTTPS 客户端：**GAS 全程不主动外联**（不消耗 UrlFetch 配额），
两端都只需要出站 HTTPS，无需任何入站端口。

## 文件

- `Code.gs` — GAS 端：`doPost` 实现 `push`/`poll`，token 鉴权，per-(sess,dir,stream)
  FIFO 队列（CacheService，单条 ≤100KB，TTL 600s），`LockService` 保护写，
  长轮询最长挂 90 秒（< 6 分钟单次执行上限）。
- `gasclient/` — Go 客户端库：`Push`（重试幂等，`DesyncError` 可恢复）、`Poll`（长轮询）。
- `gaspacket/` — 虚拟 `net.PacketConn`：一个 KCP 数据报 = 一次 bus `push`，
  包边界严格保留；`WriteTo` 永不阻塞（异步发送队列，不卡 kcp-go 的 update 循环）；
  启动时学 tail 跳过旧数据；`SessForToken()` 从 token 派生 sess（两端自动一致，
  无需 `--sess`）；`__announce__` 流跑 client run 心跳（见下）。
- `kcpclient/`、`kcpserver/` — 从 diyism/kcptunB 移植的 client/server
  （`package main` → 库，`main()` → `Run()`；client 的 `DialWithOptions` 和
  server 的 `unix:/UDP listen` 被替换为注入的虚 conn 构造；其余 flags/smux/
  `--ser`/snmp/TFO 逻辑原样保留）。
- `gs_tun_client/` — 本地：socks5 监听（`-l`，`--ser` 写死 socks5），smux+KCP
  跑在虚 conn 上；每次建 KCP 会话生成新 runID 并 announce。
- `gs_tun_server/` — 目标侧：watch `__announce__`，runID 变化就重建 KCP
  listener（`--ser` 写死 fast，内建代理）；5 分钟无心跳则回收 handler。
- `mockgas/` — 本地测试桩：Code.gs 协议的内存实现，不用部署 GAS 就能联调。

## 关键设计（实测踩出来的）

1. **虚 conn 必须异步写**：kcp-go 的全局 updater 同步调 `WriteTo`，
   一次 HTTPS POST（真 GAS 上 1~3 秒）会卡死整个 KCP 时钟。`WriteTo`
   只入队（4096），sender goroutine 慢慢 push；队满丢包（KCP 重传兜底）。
2. **announce 协议防黑洞**：kcp-go server 按源地址分 session，
   虚 conn 的源地址是常量——client 重连换 conv 后，新包会被喂进旧 session
   因 conv 不匹配而丢弃，且 v5 的 Listener 不自动淘汰。解法：client 每次建
   KCP 会话生成 runID 推到 `__announce__` 流（2 分钟一次心跳），server 见
   runID 变化就 `Close()` 旧 listener 重建。client/server 任一重启都能恢复。
3. **sess 从 token 派生**：`g-<sha256("gs-tun:"+token)[:12]>`，两端自动一致，
   去掉了 `--sess` 参数。**一个 token = 一对 client/server**，不要多开。
4. **重启不脏读**：虚 conn 启动时学 tail，跳过 600s TTL 窗口内的旧数据报；
   push 遇到 `{ok:false, expect}` 自动采纳 server 的 seq 重发（数据报是原子的，
   不像流需要重切分）。

## 部署 GAS（需在浏览器里手动做一次）

1. script.google.com → New project → 粘贴 `Code.gs`。
2. Project Settings → Script Properties → 加 `TOKEN=<随机长字符串>`。
3. Deploy → New deployment → Web app → Execute as: **Me**，
   Who has access: **Anyone** → 复制 `/exec` URL。
4. token 只走 POST body，不要放 URL 里。

## 运行

```bash
# VPS/目标侧（只需出站 https，无需开端口）
./gs_tun_server --gs https://script.google.com/macros/s/.../exec \
    --token <TOKEN>

# 本地（-l 就是 socks5 监听地址）
./gs_tun_client --gs https://script.google.com/macros/s/.../exec \
    --token <TOKEN> -l 127.0.0.1:18080

# 应用
curl -x socks5://127.0.0.1:18080 https://example.com
```

两个 secret 分工：`--token` 是 GAS 总线鉴权（防别人蹭你的 URL），
多 server：一个 token 可以带多个 gs_tun_server。server 启动时根据主机名自动生成稳定的 server id 并打印出来，客户端用 `--sv <id>` 选择连哪台：

```
./gs_tun_server --gs <exec> --token <TOKEN>
# server id: 663ff19a  (clients use --sv 663ff19a)

./gs_tun_client --gs <exec> --token <TOKEN> --sv 663ff19a -l 127.0.0.1:18080
```

不同 server 的 bus 命名空间完全隔离，不会互相干扰。

`--token` 既是 GAS 总线鉴权也是唯一的密钥：传输层是 HTTPS（TLS），总线按 token 隔离，无 token 者无法使用你的 gs URL，所以不需要单独的 KCP 密钥。
client 启动会打印配对好的 server 命令行。

本地联调（不用部署 GAS）：`./test-kcp.sh`（mockgas + 5MB 往返 + 并发 + 重启恢复）。

## 实测结果（2026-10-14，mockgas loopback，融合后）

- 经隧道 `curl -x socks5://127.0.0.1:18080 http://127.0.0.1:19999/`：
  目录列表一次成功（65ms）。
- 5MB 文件下载：字节全对；连跑 14 次，14/14 精确（1 次在重启 http.server
  的混乱窗口中损坏，未复现）。
- 4 路并发 5MB 下载：4/4 字节全对（smux 多路复用正常）。
- client 重启：新 runID announce → server teardown 旧 listener → 重建，
  隧道自动恢复，curl 照通。
- **这是 loopback+mockgas 的数字**。经 script.google.com 时，
  每个 KCP 数据报 = 一次 HTTPS POST（1~3 秒），吞吐瓶颈在 POST 粒度：
  估算持续吞吐 **KB/s 量级**——SSH/终端/轻量代理可用，大文件不合适。
  后续优化方向：一次 POST 打包 N 个数据报（batching）。

## 配额（consumer Gmail 账号，2026 口径）

- 单次执行 6 分钟：长轮询 60~90 秒，安全。
- 30 并发执行：稳态约每方向 1 个长轮询 + announce 1 个，富余。
- UrlFetch 2 万次/天：**本设计完全不用 UrlFetchApp**（GAS 纯被动），不消耗。
- CacheService 单条 100KB：KCP 数据报 ≤ MTU（1350B），base64 后 ~2KB，安全。
- 官方没有 web app 执行的每日时长配额（只有 trigger 有 90 分钟/天），
  但 Google 有未公开的滥用检测，长期跑先小流量观察。

## 已知限制

- 下行延迟 = 长轮询粒度（~1s）+ RTT；真 GAS 上每个数据报一次 POST，
  不适合实时游戏/高频交易/大文件。
- 一个 token 只支持一对 client/server（bus 层 seq 是单 writer 假设）。
- PoC 质量：没有 metrics，日志简陋；`--conn` 写死 1。
- `Code.gs` 未经真实 script.google.com 部署验证（mockgas 只覆盖了协议逻辑）；
  部署后先跑 `./test-kcp.sh`（`GS_URL`/`TOKEN` 环境变量指向真 GAS）。
