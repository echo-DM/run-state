# runState MVP — Durable Fixed Workflow Runtime

Status: ready-for-agent
Labels: ready-for-agent

## Problem Statement

长时间运行的多步骤任务会因 Worker 崩溃、进程重启或执行中断丢失内存进度。使用者需要从已经提交的步骤继续执行，并能判断任务正在运行、等待重试、等待审批还是已经结束。多个 Worker 竞争、旧 Worker 恢复、工具副作用与数据库提交之间的失败窗口，使“重新运行”不能简单等同于“安全恢复”。

本项目同时用于学习 Go 并发、Context、PostgreSQL 事务及持久化状态机。成功标准是可重复证明故障恢复行为，而不是实现 Agent 推理能力。

## Solution

提供 Go 实现、PostgreSQL 支撑的 Durable Task Runtime。用户提交创建时确定的固定 Step Sequence，通过 HTTP 查询、取消或审批；独立 Worker 进程竞争领取任务，按顺序执行并原子保存结果和进度。Scheduler 负责定时唤醒、重试唤醒及无人执行时的取消/超时处理。

Worker 崩溃后，其他 Worker 在租约到期后接管。已成功提交的 Step 不重跑，未完成 attempt 按预算恢复；旧所有者不能再修改持久化状态。外部副作用仅在工具支持稳定幂等键时验证去重，不保证通用 exactly-once。

## User Stories

1. As a 任务提交者, I want 提交固定顺序的 Step Sequence, so that 执行计划创建后可明确追踪。
2. As a 任务提交者, I want Task 和全部 Step 一起创建, so that 创建失败不会留下半个 Workflow。
3. As a 任务提交者, I want 非法类型、输入及超限请求被拒绝, so that 错误不会拖到执行中才暴露。
4. As a 任务提交者, I want 立即执行或指定一次性 run_at, so that 同一系统支持当前任务和未来任务。
5. As a 任务使用者, I want 查询 Task 状态及持久化执行进度, so that 我能判断任务是否完成或需要干预。
6. As a 任务使用者, I want Step 严格按顺序执行, so that 后续步骤只消费已提交的前置结果。
7. As a 任务使用者, I want Step 定义在创建后不变, so that 恢复不会执行另一个计划。
8. As a 任务使用者, I want 中间结果及解析后的输入持久化, so that 恢复和重试使用一致的数据。
9. As a 运维者, I want 运行多个独立 Worker, so that 多个任务能并发处理。
10. As a 运维者, I want 并发 Claim 只产生一个有效所有者, so that Worker 不会同时合法推进同一任务。
11. As a 运维者, I want Worker 在执行期间续租, so that 健康的长步骤不会被误判为无人持有。
12. As a 运维者, I want 租约过期后可以接管任务, so that 杀死 Worker 不会永久丢失任务。
13. As a 任务使用者, I want 已提交成功的 Step 不重跑, so that 崩溃恢复保留已有进度。
14. As a 运维者, I want 遗留 RUNNING Step 有明确恢复规则, so that 执行中崩溃不会使任务卡住。
15. As a 运维者, I want 旧 Worker 的迟到写入被拒绝, so that 新所有者的进度不被覆盖。
16. As a 运维者, I want 已过期但尚未接管的租约也不能复活, so that 所有权规则始终一致。
17. As a 任务使用者, I want 暂时失败按持久化预算和退避重试, so that 短暂故障可以自动恢复。
18. As a 任务使用者, I want 永久错误或预算耗尽明确失败, so that 任务不会无限重试。
19. As a 工具集成者, I want 每个 Step 使用稳定的幂等键和请求输入, so that 支持幂等的工具可以复用结果。
20. As a 工具集成者, I want 明确外部副作用的可靠性边界, so that 不会将 Runtime 误认为通用 exactly-once 系统。
21. As a 任务使用者, I want 为每次 Step attempt 设置超时, so that 单次执行不会无限等待。
22. As a 任务使用者, I want Task 截止时间跨恢复保持不变, so that 重启不能无限延长任务生命周期。
23. As a 任务使用者, I want 取消正在执行的任务, so that 取消信号能传递到协作式工具。
24. As a 任务使用者, I want 取消排队、重试或审批等待中的任务, so that 无需 Worker 再领取就能结束任务。
25. As a 任务使用者, I want 取消与成功竞争时有稳定裁决, so that 已结束状态不会来回变化。
26. As a 审批者, I want 在指定 Step 暂停并释放 Worker, so that 长时间人工等待不占执行容量。
27. As a 审批者, I want 批准指定 Step 后继续执行, so that 只有明确批准的操作才进入后续阶段。
28. As a 审批者, I want 拒绝审批后结束任务, so that 后续步骤不会被执行。
29. As a 审批者, I want 重复决定安全返回且旧请求不能批准下一步, so that 网络重试不会改变审批对象。
30. As a 运维者, I want Scheduler 重启后继续处理持久化等待状态, so that 定时执行、重试和超时不依赖内存计时器。
31. As a 运维者, I want Worker 只领取有容量执行的任务, so that 本地积压不会提前耗尽租约。
32. As a 运维者, I want 数据库通信失败时停止新的执行, so that 不确定所有权时不会继续推进任务。
33. As a 开发者, I want 状态、结果、进度和事件原子提交, so that 故障不会产生互相矛盾的持久化事实。
34. As a 开发者, I want 用真实数据库和进程故障验证恢复, so that 测试结论覆盖事务和进程生命周期。
35. As a 项目验收者, I want 完成包含崩溃、审批和重试的长任务演示, so that MVP 的核心承诺有可检查的证据。

## Implementation Decisions

1. **范围与执行模型**：固定、非空、线性的 Step Sequence；创建后不能增删、排序或替换类型。普通执行器通过编译时注册提供，接受 Context、resolved_input 和 idempotency_key，返回结果或有明确重试分类的错误，不直接写 Runtime 表。
2. **模块职责**：API 处理创建、查询与控制请求；Worker 管理领取、容量、租约和执行；Scheduler 处理持久化等待与控制条件；Runtime 解释步骤和结果；PostgreSQL Store 封装完整事务转换。不得让调用者自行拼装 Step、Task、事件更新，也不为假想数据库建立复杂抽象。
3. **进程与基础设施**：开发阶段 PostgreSQL 通过 Docker Compose 运行，固定明确的镜像版本，不使用 latest，并配置数据库健康检查；数据库端口仅绑定本机。Go API、Worker、Scheduler 在宿主机作为独立进程运行，通过显式连接配置访问数据库，使用 pgx/pgxpool。进程故障验收运行编译后的二进制。Channel 仅作进程内协调，不是持久队列。
4. **数据模型**：Task 保存状态、run_at、租约所有者/版本/到期时间、retry_at、取消标记、current_step、checkpoint、总超时、首次执行时间和 deadline；Step 保存类型、索引、状态、attempt/max_attempts、幂等键、不可变 input、resolved_input、output、error、审批决定和执行时间；Event 保存任务关联、类型、payload 和时间。
5. **约束**：Step 具有 Task 外键、唯一 Task/索引及唯一非空幂等键；索引从 0 连续排列。attempt 非负，max_attempts 和 timeout 为正。run_at 非空，省略时采用创建时间。对领取、租约到期、重试与 deadline 建立相应索引。
6. **进度事实**：task_steps 是事实来源；current_step 是首个非成功 Step，全部成功时等于数量。checkpoint 保存跨 Step 数据，不重复保存进度索引。首次 StartStep 保存 resolved_input，后续 attempt 不重新计算不同请求。
7. **Task 状态**：SCHEDULED、RUNNABLE、RUNNING、RETRY_WAIT、WAITING_APPROVAL；终态为 SUCCEEDED、FAILED、CANCELLED、TIMED_OUT。Task 不使用 PENDING。仅允许设计契约列出的转换，终态不可覆盖。
8. **Step 状态**：PENDING、RUNNING、WAITING_APPROVAL、SUCCEEDED、FAILED。仅当前 Step 可执行，全部前置 Step 必须成功。Task 失败、取消或超时时将首个未成功 Step 结束为 FAILED 并记录原因，后续未执行 Step 保持 PENDING。
9. **Claim 与容量**：先保留执行容量再 Claim，或由固定数量空闲 Executor 各自 Claim。通过行锁和 SKIP LOCKED 竞争 RUNNABLE 或租约过期的 RUNNING。领取后即管理租约，无任务与数据库错误分开处理。禁止无界 goroutine 和超容量预取。
10. **所有权**：Worker 写入要求 Task 为 RUNNING、Worker ID/lease_version 匹配且租约有效；正常推进/续租还要求未取消、未到 deadline。退出 RUNNING 清空所有者和租约期限，保留版本；接管增加版本。租约过期即不可续租复活。
11. **事务**：始终先锁 Task 再访问 Step；获锁后使用数据库当前时间判断资格，该检查为逻辑生效点。短事务内原子更新 Step、Task、checkpoint 与事件；任一校验或行数不符全部回滚。锁内禁止工具调用，并设置有限锁等待/执行超时。
12. **Store 行为**：ClaimTask、RenewLease、StartStep、FinishStep 及审批/取消/Scheduler 行为封装合法转换。FinishStep 验证租约、当前 Step、attempt 和状态；重复提交返回冲突，不重复推进。提交响应丢失时重读事实，不直接重做外部调用。
13. **恢复**：已成功 Step 不重跑。遗留 RUNNING attempt 标记 worker_lost 并结束；耗尽预算则 Task FAILED，否则新所有者立即开始下一 attempt，沿用 key。Claim 后、StartStep 前崩溃不耗预算；StartStep 提交后即使尚未调用工具也耗预算。全部 Step 已成功则收敛 Task 为 SUCCEEDED。
14. **重试**：仅 Step 有预算，默认 max_attempts=3，包含首次执行。暂时错误和 Step Timeout 可重试，第 n 次失败等待 min(2^(n-1), 60) 秒。Task 保存 retry_at，Scheduler 到期唤醒同一步。永久错误及耗尽预算失败；取消、Task Timeout、失去租约不按业务错误重试。
15. **时间**：Task 总墙钟时间从首次 Claim 开始，持久化首次执行时间与 deadline，包含此后的重试、审批、宕机和排队，不包含首次 Claim 前的等待。每次 Step attempt 单独计时，不能超过 Task 剩余时间。恢复不重置 Task deadline。
16. **取消与 Context**：非运行 Task 同步取消；RUNNING 先保存请求，由 Worker/Scheduler 完成终态。成功先提交终态则取消冲突；取消先提交则成功写入拒绝。最终状态使用独立有界控制 Context，失去租约者不能落库。进程退出不等于用户取消。
17. **审批**：Approval 为控制步骤，不消耗 attempt，等待只受 Task deadline 限制。进入等待同时释放租约。请求必须指定 step_id；批准原子完成该 Step、推进并恢复 RUNNABLE，最后一步直接 SUCCEEDED。拒绝令 Task FAILED，原因为 approval_rejected。
18. **HTTP 控制契约**：提供创建、查询 Task，以及 cancel、approve、reject。相同审批决定重放返回 200 及原决定/当前状态，不重复推进；相反决定或错误状态返回 409，不存在返回 404，非法参数返回 400。取消已 CANCELLED 返回 200，其他终态返回 409；同步取消返回 200，RUNNING 请求持久化返回 202。事件查询为可选接口，持久化事件本身必须实现。
19. **Scheduler 优先级**：锁内先处理已请求取消，再处理 deadline，最后执行 scheduled/retry 唤醒。可运行多个实例并重新检查条件；租约到期直接由 Worker 接管。Scheduler/取消轮询默认 1 秒，Heartbeat 10 秒，租约 30 秒。
20. **错误边界**：区分无任务、失去租约、冲突、取消、到期和数据库错误。数据库通信失败时停止启动新步骤并取消当前执行；恢复依赖后续正常领取，不把通信失败直接记作业务失败。
21. **幂等性**：每个 Step 的稳定 key 跨 attempt 不变；工具支持 key 时复用原结果。仅检查本地成功结果或唯一键不足以消除外部成功而本地未提交的窗口。fake tool 必须能持久化去重记录并拒绝同 key 不同 payload。
22. **限制与部署**：最多 100 个 Step；创建请求和每份输入、输出、checkpoint 各不超过 1 MiB。输入非法在创建时拒绝，输出超限为不可重试失败。未完成任务要求执行器版本兼容；不支持运行中迁移 Workflow。tenant_id 仅为可选 metadata。
23. **观测**：结构化日志关联 Task、Step、索引、Worker、lease_version、attempt 和 event；事件与状态同事务提交。明确区分已提交成功 Step 和未完成 attempt，不以日志代替数据库事实。
24. **开发数据生命周期**：PostgreSQL 使用 named volume 持久化数据；日常停止、启动和容器重建保留开发及演示任务。提供分离的启动、停止与显式数据重置操作，日常停止不得删除数据卷。开发数据库与自动测试数据库使用独立数据库及连接配置；测试清理只能作用于明确的测试目标，不得回退到开发连接。数据库镜像版本在开发与测试环境保持一致。

## Testing Decisions

- **主要 seam**：通过 HTTP 创建/查询/控制 Task，运行真实 Worker/Scheduler 二进制并操纵进程生命周期，以公开行为验证整个 Runtime。数据库和 fake tool 的持久结果用于核对不变量，不断言 goroutine 数量、私有函数调用顺序或目录布局。
- **补充 seam**：仅在精确验证事务回滚、迟到提交或提交响应丢失时，通过 Store 的行为接口连接真实 PostgreSQL，并注入可控故障。内存 fake 不能代替行锁、事务、条件更新的证据。故障注入属于测试设施，不增加生产控制接口。
- **当前基础**：仓库目前只有设计及协作说明，没有 Go 实现、测试套件或可复用测试先例。测试入口需随首个纵向切片建立；上述 seam 方案已向用户发出核对，未收到不同偏好时采用本方案。
- **好的测试**：给定持久化状态及可控故障，验证最终状态、进度、attempt、租约、事件与外部副作用。用屏障精确定位窗口，避免靠随机 sleep 碰撞；轮询必须有界。短时间配置用于快速测试，不改变数据库时间和持久 deadline 的语义。
- **数据库环境**：集成测试连接 Compose 中的真实 PostgreSQL，等待健康检查通过并完成迁移后开始。测试数据库与开发/演示数据库隔离；并行测试使用各自隔离的数据空间，清理范围限定为本次测试资源。分别控制 Worker 进程和数据库容器的生命周期，以区分 Worker 崩溃与数据库不可用。

| 验收组 | 必须覆盖的行为 |
| --- | --- |
| 环境与数据隔离 | 数据库普通停止/启动及保留卷的容器重建后数据仍在；测试初始化与清理不改变开发/演示数据；只有显式重置操作清空指定数据 |
| 创建与顺序 | 创建全有或全无；非法定义拒绝；固定定义不变；后续步骤只在前置成功后执行；输入/输出限制生效 |
| Claim 与容量 | 多 Worker 只有一个有效所有者；满载超过租约期限仍不超容量领取；交接期间管理租约 |
| 原子性 | Step/Task/Event 任意写入中断全部回滚；成功提交后的响应丢失可重读，不重复推进或事件 |
| 崩溃恢复 | Claim 前后、StartStep 前后、执行中及成功提交后崩溃；计数符合契约；前置成功步骤不重跑 |
| Fencing | 租约过期未接管时续租/提交拒绝；接管后旧 Worker 迟到写入不能修改任何相关状态或事件 |
| Retry | 重启不重置预算或 retry_at；永久失败终止；连续崩溃耗尽预算；到期恢复失败的同一步 |
| 外部幂等 | 副作用成功、本地提交前崩溃；相同 key 返回原结果且副作用为 1；不同 payload 冲突 |
| Timeout | Step Timeout 按预算处理；重试/审批/宕机期间 Task 到期；重新领取及批准不能延长 deadline |
| Cancellation | 取消全部非运行状态无需领取；运行中 Context 到达工具；取消/成功竞争；已取消 Context 不妨碍有权控制事务落库 |
| Approval | 释放资源并跨重启保留；重复/相反决定；与取消竞争；旧请求不能批准后续审批；最后一步直接结束 |
| Scheduler | 重启及多实例不丢失/重复有效转换；取消、deadline、唤醒优先级一致；终态不被唤醒 |
| 故障边界 | 数据库不可用停止新执行；进程退出不错误标记用户取消；失去租约的控制写入仍被拒绝 |

- **第一切片**：交付 Compose 数据库环境、固定镜像版本、健康检查、持久卷、开发/测试连接配置及启动/停止/显式重置说明，并实现两个宿主机 Worker、固定普通步骤、有效租约、原子提交、执行中崩溃接管及旧 Worker 迟到提交拒绝。fencing 与原子性不能延后；之后扩展重试与幂等、取消/超时、审批及定时执行。
- **最终验收**：自动故障矩阵全部通过，并完成约 30 分钟的独立进程演示，包含至少一次 Worker 崩溃、接管、人工批准、步骤失败重试，最终 SUCCEEDED，已提交成功步骤重跑数为 0。记录命令、配置、数据库/工具断言与结果；未运行的场景不得记作通过。

## Out of Scope

- LLM 推理框架、Prompt/RAG/Memory、MCP、工具市场及真实 Agent 智能。
- 动态 Workflow/DAG、运行时增删 Step、并行子任务与 fan-out/fan-in。
- exactly-once 外部副作用、撤回已发生副作用、强制终止不响应 Context 的任意代码。
- 多租户隔离、并发配额、RBAC、计费、优先级公平性和复杂限流。
- Cron、周期任务、时区日历调度、misfire 策略。
- Redis、Kafka、NATS、Kubernetes、分布式共识和多存储后端。
- Web 前端、Workflow 编辑器、完整可观测平台、动态插件及运行中 Workflow 版本迁移。

## Further Notes

- 依据：[MVP 技术设计](../../docs/mvp-technical-design.md)，尤其第 49 节规范契约和第 50 节故障验收矩阵。早期概念图/伪代码不足以替代这些规范；如发现冲突，应修正文档再实现，不能静默选择更弱保证。
- 本 spec 为已讨论设计的汇总，可直接进入拆票，无需额外 triage。拆票应按可运行的纵向切片组织，声明 blocking edges，避免只按数据库/API/Worker 横向分层。
- 本次仅发布 spec，不创建实现票、不写运行时代码、不提交或推送 Git。
- 可靠性承诺是 durable execution、at-least-once step execution、可用时的幂等重试及 stale-worker fencing；“已提交步骤不重跑”不等于外部 exactly-once。

## Comments

- 2026-09-21：根据设计 review 及补齐后的执行契约生成 spec，标记 ready-for-agent。
- 2026-09-21：确认开发环境采用 Compose PostgreSQL 与宿主机 Go 进程；补充持久卷、固定镜像版本、健康检查、开发/测试数据隔离和数据库生命周期验收，纳入首条实现切片。
