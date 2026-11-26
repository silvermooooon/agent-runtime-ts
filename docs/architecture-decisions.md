# 架构决策与工程取舍记录（Architecture Decision Records）

> **文档定位（长期维护）**：本文档记录 `agent-runtime-ts` 在演进过程中做出的核心架构抉择、备选方案对比、最终取舍原因以及我们主动接受的边界妥协。后续任何涉及模块依赖、存储表结构、并发锁机制或外部依赖引入的重大改动，均应在此追加或修订对应的 ADR 条目。

---

## 目录

- [ADR-001：内网部署导向的源码内化与极简外部依赖策略](#adr-001内网部署导向的源码内化与极简外部依赖策略)
- [ADR-002：不引入外部重型工作流引擎，基于 PostgreSQL 自研多租户 Durable 层](#adr-002不引入外部重型工作流引擎基于-postgresql-自研多租户-durable-层)
- [ADR-003：严格遵循 `pi` 单向分层（L0–L5）与纯逻辑状态机隔离](#adr-003严格遵循-pi-单向分层l0l5与纯逻辑状态机隔离)
- [ADR-004：砍掉 `agent_session_snapshots` 快照表，采用 `context_start_seq` 窗口直读](#adr-004砍掉-agent_session_snapshots-快照表采用-context_start_seq-窗口直读)
- [ADR-005：砍掉 `agent_tool_executions` 对账表，采纳 `pi` 的工具中断诊断妥协方案](#adr-005砍掉-agent_tool_executions-对账表采纳-pi-的工具中断诊断妥协方案)
- [ADR-006：大对象存储（`BlobStore`）契约解耦与多协议 URI 平滑路由](#adr-006大对象存储blobstore契约解耦与多协议-uri-平滑路由)
- [ADR-007：永不删行的单调递增 `fence` 租约、数据库唯一时钟与容错续租](#adr-007永不删行的单调递增-fence-租约数据库唯一时钟与容错续租)
- [ADR-008：单会话事务锁红线、`agent_outbox` 异步通知与审批 Park 释放槽位](#adr-008单会话事务锁红线agent_outbox-异步通知与审批-park-释放槽位)
- [ADR-009：全表贯通 `tenant_id` + `user_id` 双维度与双层 `agent_quotas` 并发治理](#adr-009全表贯通-tenant_id--user_id-双维度与双层-agent_quotas-并发治理)
- [ADR-010：剥离 DDL 迁移表与资源审计表，明确冷归档三类数据生命周期](#adr-010剥离-ddl-迁移表与资源审计表明确冷归档三类数据生命周期)

---

## ADR-001：内网部署导向的源码内化与极简外部依赖策略

- **状态**：已采纳（2026-11-26）
- **背景**：本运行时面向企业内网环境的 SaaS 服务部署，外网供应链受限，审计与升级成本高，且不需要公网个人版 CLI 面向十余家小众大模型厂商的 SDK 依赖。
- **评估过的方案**：
  1. 直接通过 npm 依赖引入 `@earendil-works/pi-*`、`partial-json`、`@aws-sdk/client-s3` 及各家模型厂商 SDK。
  2. 将 `pi` 核心包（`schema`、`telemetry`、`ai`、`agent`）源码内化到本仓库 `packages/`，裁剪模型厂商至实际使用的 OpenAI 与 Azure OpenAI，将轻量工具库替换为内部可控实现，仅保留高协议复杂度的官方基础驱动。
- **最终抉择**：采用 **方案 2**。
  - **保留的运行时第三方依赖仅 2 个**：`openai`（处理复杂的 SSE 流、重试、Azure 鉴权与 Responses/Completions 协议边界）和纯 JS 的 `pg`（处理 PostgreSQL 二进制/文本线协议与连接池，无 C++ 原生编译产物）。
  - **替换为内部源码实现的部分**：
    - 用约 230 行确定性状态机 `StreamingJsonParser` 替换 `partial-json`（仅用于流式 UI 半截工具参数预览，最终执行前仍有 `JSON.parse` + `schema` 强校验兜底，零安全风险）。
    - `packages/blob-s3` 使用 Node.js 内置 `node:crypto`（AWS Signature V4）+ 原生 `fetch` 实现 S3/MinIO 对象读写，避免引入包含 90+ 子包的 `@aws-sdk/client-s3`。
    - 模型目录改为静态本地 TypeScript 定义，移除对公网 `models.dev` 的生成依赖。
- **影响与取舍**：需要自行维护内化代码的同步升级，但换来了极高的内网可移植性、零构建脚本攻击面以及毫秒级冷启动体积。

---

## ADR-002：不引入外部重型工作流引擎，基于 PostgreSQL 自研多租户 Durable 层

- **状态**：已采纳（2026-11-26）
- **背景**：我们需要支撑多租户、多用户、多 Pod 无状态集群部署的 SaaS Agent 服务。调研发现：
  - `pi` 官方的 `@earendil-works/pi-durable` 及 `p2/p3` 定位为 *"One Pi per person"*（单进程独占 SQLite/JSONL 或 Cloudflare Durable Objects 单实例内存串行），无 `tenant_id/user_id` 概念，也无跨进程行级租约与 Fencing。
  - 社区插件 `@netzlabor/pi-durable-postgres` 使用 Schema 级全局 Advisory Lock（整个库只允许 1 个活跃 Worker 进程），无法水平扩展；`@hatchet-dev/pi-durable` 或 Temporal 则需要在内网额外部署维护一整套重型外部调度集群。
- **最终抉择**：不引入 Temporal / Hatchet 等黑盒外部引擎，直接基于现有的 **PostgreSQL 14+** 构建职责清晰的 `packages/session` + `packages/postgres` + `packages/cluster`。
- **取舍原因**：
  - PostgreSQL 的 `FOR UPDATE SKIP LOCKED`、事务 ACID 与 `clock_timestamp()` 完全足以支撑每秒数千次会话状态推进规模的 SaaS Agent 集群。
  - 每一张表、每一条锁路径对团队完全白盒可控，内网运维只需维护一套 PostgreSQL + 对象存储/Workspace 基础设施。

---

## ADR-003：严格遵循 `pi` 单向分层（L0–L5）与纯逻辑状态机隔离

- **状态**：已采纳（2026-11-26）
- **抉择**：严格建立自底向上的单向有向无环图（DAG）：
  - **L0**：`schema`、`telemetry`（零依赖基础原语）
  - **L1**：`ai`、`agent`（保持上游纯净内存模型流与循环，绝不感知租户、会话持久化或数据库）
  - **L2**：`session`（Durable 核心纯逻辑层：定义 `SessionIdentity`、Journal 事件 Tagged Union、滑动窗口 Reducer、`DurableLoop`、`BlobStore` 契约与内存存储，**零数据库/网络驱动依赖**）
  - **L3**：`postgres`、`blob-s3`（物理存储实现层，互不依赖，不包含后台常驻调度定时器）
  - **L4**：`cluster`（多 Worker 租约续期、公平认领、审批挂起唤醒、子 Agent 协调与 Outbox 投递）
  - **L5**：工具、MCP、Skill、Memory、业务侧 `WorkspaceBlobStore` 等可插拔能力
- **取舍原因**：防止重蹈早期 Python 版中 Session、PostgreSQL SQL 与集群调度器互相耦合的覆辙；保证核心 Durable 恢复与压缩逻辑在无数据库的单元测试中可毫秒级确定性验证，且业务按需导入子包时不会连带拉起无关重依赖。

---

## ADR-004：砍掉 `agent_session_snapshots` 快照表，采用 `context_start_seq` 窗口直读

- **状态**：已采纳（2026-11-26）
- **背景**：在早期设计中，为了实现无状态 Worker 跨节点接手会话时 `O(1)` 恢复，曾设计 `agent_session_snapshots` 表存放折叠后的完整模型可见上下文 `messages[]`。
- **问题剖析（为什么放弃快照表）**：
  1. **PostgreSQL MVCC 写放大与 TOAST 死元组膨胀**：PG 的 `UPDATE` 物理上是标记旧行死亡并全量重写新行与 WAL。若在 30~40 轮工具循环中每轮 `UPDATE` 一份 200KB~300KB 的快照 JSON，会产生 **15~40 倍的磁盘写放大** 及大量死 TOAST 页，高并发下引发严重的 Autovacuum I/O 抖动；若改为低频惰性快照，又增加了“快照 + 增量事件”双路拼装的复杂度。
  2. **活跃窗口天然有界**：在已具备 `agent_session_configs`（工具 Schema 去重）、`BlobStore`（>64KB 大对象外置）以及自动 `Compaction`（上下文滑动压缩）的前提下，大模型当前可见的活跃事件数量本身始终被限制在最近的 **20~80 条** 之内。
- **最终抉择**：**彻底移除 `agent_session_snapshots` 表**。在 `agent_sessions` 主表上维护轻量整数游标 `context_start_seq`（及 `compaction_seq`）：
  - 写入路径变成 **100% 纯 `INSERT` 追加日志**，零大字段 `UPDATE`、零双份冗余存储。
  - 任意 Worker 接手或恢复会话时，沿聚簇主键 `(tenant_id, session_id, seq >= context_start_seq)` 顺序扫描最近几十条记录（PG 耗时仅 1~3ms，传输字节量与单行快照完全相当，内存折叠 < 0.5ms）。
  - 冷归档只归档删除 `seq < context_start_seq` 的历史事件，热表保留最近活跃窗口，老会话续聊同样免解压。

---

## ADR-005：砍掉 `agent_tool_executions` 对账表，采纳 `pi` 的工具中断诊断妥协方案

- **状态**：已采纳（2026-11-26）
- **背景**：当外部工具调用中途遭遇 Worker 崩溃重启时，理论上最严密的做法是建立独立的 `agent_tool_executions` 表，强制要求工具实现 `reconcile()` 对账接口，无法确认时将整个会话置为 `outcome_unknown / waiting_recovery` 挂起等待人工核实补录。
- **权衡与妥协**：
  - 严密对账方案虽然在理论上闭环，但代价极高：多出 1 张高频读写表、每次工具调用多出状态流转事务、要求所有业务工具编写额外对账逻辑，且还需配套管理后台人工补录流。
  - 当前主流前沿大模型的工具理解与审慎推理能力已显著增强：只要明确告知模型“上次工具执行期间发生节点中断，副作用未知，请先调用只读查询工具确认状态后再决定后续操作”，模型能够主动先查后做。
- **最终抉择**：采纳与官方 `pi-durable` 一致的**轻量妥协方案**，**彻底移除 `agent_tool_executions` 表、`reconcile()` 协议与 `waiting_recovery` 状态**：
  1. 大模型流结束落盘的 `model_completed` 事件已包含完整的 `toolCalls` 意图；随后每完成一个工具追加一条 `tool_completed`。
  2. 崩溃恢复时对比最后一轮 `model_completed` 与已有的 `tool_completed`：
     - 已完成的工具直接复用结果，绝不重跑；
     - 未完成且声明 `replay: "safe"` 的纯读/幂等工具自动重跑；
     - 未完成的默认工具（`replay: "never"` 或未声明）：不自动重跑，直接补写一条 `isError: true` 的结构化 `interrupted` 诊断消息交给大模型下一轮自行先核查再决策。
  3. 框架仍在内存中按 `(tenantId, sessionId, runId, turn, callId)` 免费计算确定性的 `operationId` 注入工具执行上下文，供有强幂等需求的下游接口直接用作 `Idempotency-Key`。

---

## ADR-006：大对象存储（`BlobStore`）契约解耦与多协议 URI 平滑路由

- **状态**：已采纳（2026-11-26）
- **背景**：会话中的截图（`ImageContent`）或超长工具输出如果直接塞进 `agent_session_events` 会撑爆数据库缓存；但不同部署环境的大对象介质不同（内网前期可能用 S3/MinIO，独立小环境可能只用 PG，后续生产将统一接入内部 `Workspace` 服务并以 URL 引用）。
- **最终抉择**：
  - 在 L2 `packages/session` 定义与介质无关的 `BlobStore` 接口与自描述引用 `BlobRef { type: "blob_ref", uri, sha256, mimeType, byteLength, preview }`；不注入 `blobStore` 时外置行为完全关闭。
  - 内置提供零 DB 表的 `S3BlobStore`（`s3://`）与可选 DB 兜底的 `PostgresBlobStore`（`pg-blob://`，对应唯一的可选表 `agent_blobs`）。
  - 提供 `createBlobRouter({ primaryWriter, readers })` 按 URI 协议头自动分发读取请求，使未来从 S3 切换到内部 `Workspace` URL 时无需迁移历史老会话数据。

---

## ADR-007：永不删行的单调递增 `fence` 租约、数据库唯一时钟与容错续租

- **状态**：已采纳（2026-11-26）
- **背景**：审查早期 Python 版实现时发现三个致命隐患：
  1. 释放租约若采用 `DELETE` 行，重新插入时若 `fence` 重置为 `1` 会产生 ABA 脑裂漏洞；
  2. 混用 Worker 本地时钟会导致时钟漂移下提前误抢锁；
  3. 续租协程遇到单次瞬时网络抖动即把本机所有运行中会话全部 Abort 强杀。
- **最终抉择**：
  1. `agent_session_leases` 行一旦创建**永不物理删除**；主动释放仅将 `expires_at` 更新为 `clock_timestamp()`，后续任何节点重抢严格执行 `fence = agent_session_leases.fence + 1`。
  2. 所有过期比较与续期计算 **100% 在 SQL 内部使用 PostgreSQL `clock_timestamp()`**。
  3. 续租间隔设为 `leaseTtlMs / 3`（如 TTL 30s、每 10s 批量续租一次）；瞬时网络错误触发指数退避重试，**仅当本地单调时钟观测距离上次成功续租已逼近 TTL 安全边界（或数据库明确返回 `fence` 已被夺走）时**，才定向中止对应会话。

---

## ADR-008：单会话事务锁红线、`agent_outbox` 异步通知与审批 Park 释放槽位

- **状态**：已采纳（2026-11-26）
- **抉择**：
  1. **严禁单事务跨多个 `session_id` 加锁**：子 Agent 完结通知父会话、或外部审批结果唤醒会话时，一律向无行锁争抢的自增表 `agent_outbox` 插入通知，由后台投递器异步转为目标会话的 `continuation` 指令，彻底根除父子会话互锁死锁。
  2. **父子并发槽位池物理隔离**：`WorkerHost` 将 `rootSlots` 与 `childSlots` 分池管理（配合 `agent_commands.is_child`），防止所有槽位被等待子任务的父会话占满而引发集群级饥饿死锁。
  3. **高危审批挂起（Park）即刻释放资源**：生成 `agent_approvals (status='pending')` 后，立即释放会话 Lease 与 Worker 内存槽位；审批完成后按需唤醒并二次核验 `action_hash` 未被篡改。

---

## ADR-009：全表贯通 `tenant_id` + `user_id` 双维度与双层 `agent_quotas` 并发治理

- **状态**：已采纳（2026-11-26）
- **背景**：SaaS 平台同时存在企业租户（`tenant_id`）与企业内员工用户（`user_id`）两个隔离层级。仅有 `tenant_id` 会导致两类问题：① 按用户维度查询/清理数据需要跨表 JOIN；② 同一家租户内的单个员工高频刷任务会耗尽全公司的租户并发额度，导致同公司其他员工饥饿。
- **最终抉择**：
  - 全套 11 张核心表及可选表 `agent_blobs` 全部冗余携带 `tenant_id` 与 `user_id` 非空列，并在 `SessionIdentity` 中强制绑定 `{ tenantId, userId, sessionId }`。
  - 设计统一的 `agent_quotas (tenant_id, scope_type, user_id)` 表，同时管控**租户总上限**、**租户内普通用户默认上限（`per_user_max_*`）**以及**特定 `user_id` 独立提额或置 `0` 熔断**。

---

## ADR-010：剥离 DDL 迁移表与资源审计表，明确冷归档三类数据生命周期

- **状态**：已采纳（2026-11-26）
- **抉择**：
  1. **不建 `agent_schema_migrations`**：数据库 DDL 变更统一由内部数据库运维平台纳管；运行时严禁自动执行 DDL，也严禁在写入路径调用 `to_regclass()` 探测表。
  2. **不建 `agent_audit_events`**：该表对运行时无任何控制与状态影响，且核心操作已在 `commands / approvals / model_usage / events` 留痕；改为零耦合的可选 `AuditSink` 接口供宿主平台对接自身统一审计中心。
  3. **冷归档三类分治**：
     - **上冷存并物理删热行**：`agent_session_events` 中 `seq < context_start_seq` 的历史区间（释放 90%+ 库容）；
     - **不归档、超窗口直接按 TTL 物理清理**：已终态的 `agent_commands` 与已投递的 `agent_outbox`；
     - **常驻热库永不随日志归档删除**：`agent_sessions`、`agent_session_configs`、`agent_session_leases`、`agent_quotas`、`agent_runs`、`agent_model_usage`、`agent_approvals`（保障列表秒开、老会话免解压续聊、`fence` 不回退、历史计费报表随时热查）。
