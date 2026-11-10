# billiard 对 cherry 的补丁

## 2026-11-10：合并官方 v1.6.9

- 用 merge 保留 fork 提交历史，官方 clone 只读。唯一冲突是 `net/proto/session.go`：旧全局锁无法保护 protobuf Marshal，采用 v1.6.9 的连接 `SessionData` 锁与 `NewSession(mid)` 请求快照；不再共享请求 map。MID 使用新 protobuf 字段，不支持新旧节点混跑，应全量停服切换。
- P2–P11 逐项保留：Loki、NATS nil 防护/重连、发现重注册/时间戳、写缓冲断开、连接器停止、消息丢弃回调与池 header 修复。
- P12：`net/actor/system.go` 固定本地 resultChan 后再投递，避免读取已回收 Message.ChanResult。等价回移官方标签之后 `2b41976` / `e1808b4` 的修复，未跟随整个 master。`system_callwait_test.go` 在修复前 20,000 次调用有 2 次超时，修复后通过。
- P13：`SessionData.Apply` 一把锁内完成关联字段的设置/删除，`GetOrSet` 保持路由钉住且不覆盖已绑定状态。并发快照、序列化、关联字段一致性和绑定回归见 `session_data_test.go`。升级时若官方实现同等语义可合并，保留测试。
- 验证：`go test -count=1 ./net/proto/ ./net/parser/pomelo/ ./net/actor/ ./logger/ ./net/nats/ ./net/discovery/` 通过；业务新依赖 gate/route 测试及 cmd 构建通过。业务不能继续使用旧 vendor。


当前基线已升级为 **官方 v1.6.9（bbcabe0）**，保留 v1.6.6 以来的 fork 历史与定制补丁。下文 P1–P11 的原始记录保留作历史说明，当前状态以本节与升级记录为准。合官方新版本、或把补丁回馈 cherry 时，先对这里再动手。跨仓目录分工见仓库外的 [`server/docs/WORKSPACE.md`](../../docs/WORKSPACE.md)（本机路径 `d:\object\billiard\server\docs\WORKSPACE.md`）。

不要改 `module` 名。GitHub 上的 fork 是 `ordinacb/cherry`，Go import 仍是 `github.com/cherry-game/cherry`。

| 项 | 值 |
|---|---|
| 升级分支 | `codex/cherry-v1.6.9`（从 `billiard-v1.6.6` 合并升级） |
| 基线 | `v1.6.9` / `bbcabe0` |
| origin | `https://github.com/ordinacb/cherry.git`（只推这里） |
| upstream | `https://github.com/cherry-game/cherry.git`（只 fetch / merge，禁止 push） |
| 升级前补丁头 | `5f07570`（P2–P11 均已提交） |
| 新增补丁 | P12 同节点 CallWait 回收竞态；P13 连接状态原子批量更新 |

列出相对基线的全部差异：

```powershell
cd d:\object\billiard\server\my_cherry
git checkout billiard-v1.6.6
git diff 4a6c56e HEAD
git status -sb   # 看未提交的补丁
```

合官方之后若某条补丁已无必要，不要只删代码：在本文件把状态改成「已并入官方 / 已废弃」，并写官方提交或 tag。

---

## 速查

| ID | 状态 | 文件 | 一句话 |
|---|---|---|---|
| P1 | v1.6.9 请求快照取代全局锁 | `net/proto/session.go` | `Session.Data` 全局读写锁，避免 gate 并发写 map 整进程崩溃 |
| P2 | 已提交 `99266f7` | `logger/logger.go`、`logger/logger_builder.go`、`logger/logger_test.go` | JSON 键对齐 Loki：`time`、小写 `level`、ISO8601、`dpanic`、`env` |
| P3 | 已提交 `ef50644`（测试文件未跟踪） | `net/nats/connect.go`、`net/nats/connect_error_test.go` | NATS `ErrorHandler` 在 `sub == nil` 时不再 panic |
| P4 | 已提交 `40d69f0` | `net/parser/pomelo/agent.go` | 回包和推送打正文；心跳不打日志 |
| P5 | 已提交 `f8bb042` | `net/nats/connect.go` | `max_reconnects` 为 0 时无限重连（原来落到 nats 默认 60 次后永久断开） |
| P6 | 已提交 `f8bb042` | `net/discovery/component_default.go`、`component_master.go` | 同 nodeID 重启时替换旧成员；worker 停服前广播下线；master 重启后按新列表清掉死节点 |
| P7 | 已提交 `f8bb042` | `net/parser/pomelo/agent.go` | 写缓冲满时断开连接而不是静默丢回包；心跳超时改为 2 倍间隔 |
| P8 | 已提交 `f8bb042` | `net/connector/connector.go`、`tcp_connector.go` | `Stop` 对 nil listener 判空；Accept 出错退避，listener 关闭时退出 |
| P9 | 已包含在升级前 `5f07570` | `net/actor/drop.go`、`actor.go`、`system.go`、`net/parser/pomelo/actor.go` | 消息被丢弃时回调 `SetDropHandler`（actor 不存在 / 未就绪 / 子 actor 不存在 / 方法未注册），业务据此给客户端回错误；pomelo 错误应答带上 `Data` 作为提示文字 |
| P10 | 已包含在升级前 `5f07570` | `net/nats/msg_pool.go`、`net/connector/*_test.go`、`net/parser/pomelo/client/client_test.go` | `ReleaseNatsMsg` 不再清空调用方的 header map；连接器测试改成真测试；pomelo 客户端测试需设 `POMELO_TEST_WS_ADDR` 才跑 |
| P11 | 已包含在升级前 `5f07570` | `net/discovery/component_master.go` | 超时删除带上当时的 LastAt；节点已经用更新的时间重新注册时，忽略这条过期删除，避免 master 收回自己的删除广播把活节点摘掉 |
| — | 不做 | `net/discovery/component_master.go` | 不要在 Init 里提前注册。actor 在 OnAfterInit 才创建，提前注册会让请求打到未就绪的节点 |

合 `upstream/master` 或新 tag 时，冲突几乎一定落在这三处。其余 cherry 文件我们没动。

---

## P1 `Session.Data` 锁

### 现象

gate 进程以 `fatal error: concurrent map writes` 退出。这不是 Go `panic`，`recover` 拦不住，该节点上所有在线连接一起掉。

### 原因

`pomelo.BuildSession` 返回的是 agent 持有的 **同一份** `*Session`，不是副本。两个 goroutine 都会写 `Session.Data`：

- agent 读循环：每条客户端消息 `SetMID`
- agent actor：`bindUID`、`setSession` 等写 uid / 房间 / 节点

`Session` 是 protobuf 生成类型，`Data` 是裸 `map[string]string`，官方 helper 没有锁。登录变慢、下一条客户端请求叠上来时，两边会撞在同一微秒。

业务侧无法在不改框架的前提下包住所有读写：`Get*` / `Set` / `Restore` / `Clear` 都在 cherry 里。

### 我们怎么改

在 `net/proto/session.go`（**不是** generated `.pb.go`）加包级 `sessionMu sync.RWMutex`：

- 所有对 `x.Data` 的读写走加锁的 `Set` / `get` / `Remove` / `Contains` / `Equal` / `ImportAll` / `Restore` / `Clear`
- 内部 `set` 不加锁，给已经持锁的 `ImportAll` / `Restore` 用
- `Restore` 在同一把锁里先清空再填入，避免读者看到半空 session
- 一把全局锁而不是每 session 一把：生成类型塞不进 mutex 字段；临界区是单次 map 操作

业务说明见 game-server [`CONCURRENCY.md`](../../server2026/game-server/docs/CONCURRENCY.md)「网关的 session 被两个 goroutine 共享」。

### 合官方时怎么判

| 官方新代码 | 做法 |
|---|---|
| 已经给 `Data` 加了锁（或改成 sync.Map / 每 session mutex） | 删掉我们的 `sessionMu`，保留官方实现；本条标「已并入官方」 |
| 只是改了 Get/Set 签名或新增 helper，仍然裸写 map | 把锁打到新 helper 上，**不要**整文件用我们的覆盖 |
| 重新生成了 `session.pb.go`，helper 仍在 `session.go` | 继续只改 helper 文件；不要去改 generated 代码 |
| 把 session 改成每次 `BuildSession` 都拷贝 | 确认 gate 读循环和 actor 不再共享 map 后，可以去掉锁 |

验证：

```powershell
go test ./net/proto/
```

没有专门的并发单测；回归要靠 gate 在登录尚未完成时叠客户端请求（或 game-server e2e）。

---

## P2 Loki JSON 编码器

### 现象

game-server 生产日志要进 Loki / Grafana，键名和 level 大小写必须和 auth-server 一致。官方 JSON encoder 用 `ts`、大写 `INFO`、可配置但非 ISO8601 的时间，且 `GetLevel` 不认识配置里的 `dpanic`。

### 原因

契约在 game-server [`docs/LOG.md`](../../server2026/game-server/docs/LOG.md)。cherry 的 `SetNodeLogger` 是唯一写入 `nodetype` / `nodeid` 的地方，编码器在 `logger` 包内部，业务仓包不住。

### 我们怎么改

| 文件 | 改动 |
|---|---|
| `logger/logger_builder.go` | JSON：`TimeKey = "time"`（不要 `ts`）、`EncodeLevel = LowercaseLevelEncoder`、`EncodeTime = ISO8601TimeEncoder`。console 仍用 `config.TimeEncoder()` |
| `logger/logger.go` | `GetLevel("dpanic")` → `DPanicLevel`；`SetNodeLogger` 若 `profile.Env()` 非空则 `SetCommonField("env", env)` |
| `logger/logger_test.go` | `TestGetLevel` 覆盖 dpanic；新增 `TestJSONEncoder_LokiContract`（必须有 `time`/`level`/`msg`/`caller`/`service`/`env`/`nodetype`/`nodeid`，禁止 `ts`，info 无 `stack`） |

### 合官方时怎么判

| 官方新代码 | 做法 |
|---|---|
| 已改成 `time` + 小写 level + JSON ISO8601 | 删重复补丁，**留下** `TestJSONEncoder_LokiContract`（或把断言并进官方测试） |
| 换了另一套 encoder 配置结构 | 按 LOG.md 把键名重新打到新结构上，不要整文件覆盖 |
| `GetLevel` 已支持 dpanic | 只留我们的测试用例 |
| 他们坚持 `ts` / 大写 level | **不要跟**。Loki 看板按 `time` + 小写 `level` 查；跟了会让 game 与 auth 对不上 |

验证：

```powershell
go test ./logger/
```

`TestJSONEncoder_LokiContract` 必须过。

---

## P3 NATS 异步错误回调空指针

### 现象

NATS（`127.0.0.1:4222`）断开或 flush 失败后，gate 立刻：

```
panic: runtime error: invalid memory address or nil pointer dereference
github.com/cherry-game/cherry/net/nats.(*Connect).natsOptions.func4
```

调用栈来自 `nats.go` 的 `flusher` → `asyncCBDispatcher`。Disconnect 日志本身是正常的；把进程打崩的是后续 ErrorHandler。

### 原因

nats.go 在写缓冲 flush 失败时调用 `AsyncErrorCB(nc, nil, err)`，**subscription 是 nil**（部分协议异步错误同样传 nil）。官方 `ErrorHandler` 无条件读 `nc.IsConnected()`、`err.Error()`、`sub.Subject`。`sub == nil` 时空指针，整个节点退出。

Reconnect / Closed 回调在 `nc == nil` 时同样不安全。

这是框架回调的缺陷，不能在业务仓包一层。

### 我们怎么改

`net/nats/connect.go`：

- `ErrorHandler` 经 `asyncErrorFields(nc, sub, err)` 取字段：任一参数为 nil 则用零值，不解引用
- `ReconnectHandler`：`nc == nil` 时 ConnectedUrl 记空串
- `ClosedHandler`：先判断 `nc != nil` 再 `LastError()`；`clearWaiters()` 仍要跑

`net/nats/connect_error_test.go`：`TestAsyncErrorFieldsNilSubscriptionDoesNotPanic`

本条在写清单时还在工作区，**未进 `99266f7`**。提交并 `git push origin billiard-v1.6.6` 之后，再刷新 game-server 的 `replace`。在那之前，本机若用 `vendor` 编译，需要 vendor 里同一份 `connect.go`（那是权宜，不是补丁源）。

### 合官方时怎么判

| 官方新代码 | 做法 |
|---|---|
| ErrorHandler 已对 `nc`/`sub`/`err` 做 nil 判断 | 删 `asyncErrorFields`，保留或改写测试仍覆盖 `sub == nil` |
| 换了 NATS 客户端大版本，flusher 不再传 nil sub | 仍建议保留 nil 防护；nats 文档允许 AsyncErrorCB 的 sub 为空 |
| 回调签名变了 | 按新签名做同样的 nil 安全日志，不要整文件覆盖 |

验证：

```powershell
go test ./net/nats/ -count=1 -run TestAsyncErrorFieldsNilSubscriptionDoesNotPanic
```

NATS 进程挂掉时，gate 应打 Disconnect / ErrorHandler 的 warn，**进程不能退出**。NATS 自己要另起，那是运维问题，不是这条补丁的范围。

---

## 合官方的步骤

```powershell
cd d:\object\billiard\server\my_cherry
git checkout billiard-v1.6.6
git fetch upstream --tags
git log --oneline HEAD..upstream/master
# 或：git merge v1.6.7
git merge upstream/master
```

冲突文件优先看：

1. `net/proto/session.go` → P1
2. `logger/logger.go`、`logger/logger_builder.go`、`logger/logger_test.go` → P2
3. `net/nats/connect.go` → P3

原则：官方已有**同等语义**就删我们的重复实现；官方改了同一段但语义不同，把我们的锁 / Loki 键 / nil 判断**打到新代码上**，不要整文件「用我的覆盖」。

```powershell
go test ./net/proto/ ./logger/ ./net/nats/
git push origin billiard-v1.6.6
```

然后在 **server2026** 的 `game-server`：

```powershell
$env:GOPRIVATE = 'github.com/ordinacb/cherry'
go mod edit -replace github.com/cherry-game/cherry=github.com/ordinacb/cherry@billiard-v1.6.6
go mod tidy
go build -o NUL ./cmd
```

不要：

- `git push upstream`
- 为了「干净」把 `billiard-v1.6.6` rebase 成官方 master 再把补丁扔到一边不记历史
- 提交 `replace => ../../my_cherry`
- 把 cherry 源码拷进 `game-server/` 当长期补丁源（`vendor/` 里的副本只服务当前编译，源在本仓库）

---

## P11 超时删除不要摘掉刚注册的节点

### 现象

master 判定心跳超时后广播删除。billiard 在同一毫秒重新注册，2 毫秒后 master 又把这个新注册删掉。网关在这一秒内找不到 `bl-billiard-1`，转发 `hitBallOver` 返回 500。

### 原因

master 用一条连接发布删除、另一条连接订阅同一主题，NATS 会把广播投回 master 自己。`removeSubscribe` 只按 nodeID 删除，不看 `LastAt`。回声到达时表里已经是新注册，照样被摘掉。worker 上也会发生同样的乱序。

### 我们怎么改

超时删除的消息带上当时的 `LastAt`。收到删除时，若本地成员的 `LastAt` 比消息里的更新，就忽略。停服下线仍不带时间，立即删除。

### 怎么测

```powershell
go test ./net/discovery/ -count=1 -run TestRemoveNoticeKeepsNewerRegistration
```

改完后本机 `replace => ../../my_cherry`，master 和全部节点一起重启。不要把这行 replace 提交。

---

## 明确没改、也不要在合入时顺手改的

- `module github.com/cherry-game/cherry`（不要改成 `ordinacb/cherry`）
- 不 `require github.com/cherry-game/components`（那套钉 v1.5，和核心 v1.6.6 不能混）
- pomelo 编解码、actor 信箱、discovery 默认实现：业务不兼容时先查 game-server，确认是框架 bug 再在本仓加新的 P 条

---

## 以后每加一条补丁

同一提交里改代码并更新本文件：

1. 新编号（P4…），写入上面的速查表
2. 现象、原因、改了哪些文件、合官方时怎么判、怎么测
3. 提交信息写「为什么改」，并提到基于 `v1.6.6`（或当时的新基线 tag）
4. 只推 `origin`；刷新 `game-server` 的 `replace` 伪版本
