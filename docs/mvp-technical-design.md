# Go Durable Agent Worker

## 1. Project Overview

这是一个用 Go 实现的简化版 **Durable Agent Worker Runtime**。

项目重点不是 Agent 的推理能力，也不是 Prompt、RAG、Memory 或工具生态，而是解决一个基础但非常重要的问题：

> 一个运行 30 分钟、调用多个工具的 Agent 任务，即使 Worker 崩溃、进程重启或任务被中断，也能够从最近的持久化状态继续执行，而不是从头开始。

项目主要训练：

- Goroutine
- Channel
- `context.Context`
- Cancellation
- Timeout
- PostgreSQL Transaction
- Task Claiming
- Lease
- Heartbeat
- Checkpoint
- Retry
- Idempotency
- Distributed State Machine
- Crash Recovery

---

# 2. Core Principle

系统的核心原则：

> **Database is the source of truth. Worker memory is disposable.**

任何只存在于以下位置的状态：

- Goroutine
- Channel
- Process memory

都应该被认为是临时状态。

如果 Worker 在任意时刻被：

```bash
kill -9
```

任务的重要执行状态依然必须能够从 PostgreSQL 恢复。

---

# 3. MVP Success Criteria

MVP 最重要的 Demo：

```text
Task starts
│
├── Step 1 ✓
├── Step 2 ✓
├── Step 3 ✓
├── Step 4 ✓
├── Step 5 ✓
│
💥 Worker A crashes
│
│ lease expires
│
▼
Worker B claims task
│
├── restore checkpoint
├── Step 6 ✓
├── Step 7 ✓
│
├── WAITING_APPROVAL
│
👤 human approves
│
├── Step 8 ✓
├── Step 9 fails
├── retry
├── Step 9 ✓
│
▼
SUCCEEDED
```

最终系统应该能够证明：

```text
Task duration: 32m
Workers involved: 2
Worker crashes: 1
Completed durable steps re-executed: 0
Final status: SUCCEEDED
```

如果这个场景能够稳定运行，MVP 即成立。

---

# 4. MVP Scope

## 4.1 Must Have

第一阶段必须实现：

1. Durable Task State Machine
2. PostgreSQL-backed Task Queue
3. Fixed Step Sequence
4. Worker Claiming
5. Worker Lease
6. Heartbeat
7. Worker Crash Recovery
8. Step Checkpoint
9. Retry
10. Timeout
11. Cancellation
12. Basic Idempotency
13. Human Approval

---

# 5. Fixed Workflow Model

MVP 中，每个 Task 都是一个：

> **预先定义好的、固定顺序的 Step Sequence。**

例如：

```text
Task
│
├── Step 1: fetch_data
├── Step 2: analyze
├── Step 3: call_tool
├── Step 4: approval
└── Step 5: generate_result
```

Task 创建时，所有 Step 已经确定。

运行过程中只允许改变：

```text
step status
attempt
input
output
error
checkpoint
task status
```

运行时不允许：

```text
新增 Step
删除 Step
修改 Step 顺序
替换 Step 类型
动态生成 Workflow
动态生成 DAG
Agent 自行修改执行计划
```

---

# 6. Why Fixed Step Sequence

固定 Workflow 是 MVP 的主动设计选择。

这样可以大幅简化：

```text
Checkpoint
Crash Recovery
Retry
Idempotency
State Transition
Testing
```

恢复任务时，系统不需要重新计算 Workflow。

只需要：

```text
load task
↓
load persisted step states
↓
find next executable step
↓
continue
```

例如：

```text
Step 1 SUCCEEDED
Step 2 SUCCEEDED
Step 3 SUCCEEDED
Step 4 PENDING
Step 5 PENDING
```

恢复后直接：

```text
next step = Step 4
```

而不是让 Agent 再次决定下一步做什么。

这保证：

> **Execution plan is deterministic; execution itself may fail and recover.**

---

# 7. Simplified Features

## 7.1 Scheduled Task

MVP 只支持：

```text
run_at
```

例如：

```json
{
  "run_at": "2026-09-22T10:00:00Z"
}
```

Scheduler 寻找：

```text
run_at <= now()
AND status = SCHEDULED
```

然后：

```text
SCHEDULED
↓
RUNNABLE
```

暂时不支持：

```text
Cron expression
Timezone scheduling
Calendar scheduling
Recurring tasks
Misfire policy
```

---

# 8. Parallel Execution

Parallel Child Tasks 不属于第一核心版本。

如果后续加入，只支持简单：

```text
fan-out
fan-in
```

例如：

```text
        Parent
          │
      ┌───┼───┐
      ↓   ↓   ↓
      A   B   C
      └───┼───┘
          ↓
         Join
```

注意：

即使加入并行任务，Workflow 结构依然必须在 Task 创建时确定。

不支持运行时动态生成新的 branch。

第一阶段可以完全不实现 Parallel Task。

---

# 9. Multi-tenancy

MVP 如需展示多租户，只保留：

```text
tenant_id
max_concurrent_tasks
```

例如：

```text
Tenant A: max 5 running tasks
Tenant B: max 2 running tasks
```

暂时不实现：

```text
RBAC
Billing
Usage Accounting
Token Accounting
Priority Fairness
Complex Quotas
Tenant Isolation Platform
```

---

# 10. Explicit Non-Goals

MVP 不实现：

- LLM reasoning framework
- Prompt orchestration
- RAG
- Vector Database
- Agent Memory
- MCP
- Tool marketplace
- Workflow visual editor
- Dynamic workflow generation
- Dynamic DAG
- Kubernetes
- Redis
- Kafka
- NATS
- Distributed consensus
- Exactly-once execution
- Complex cron
- Billing
- Full observability platform
- Web frontend

Agent 本身可以非常简单。

例如：

```text
Step 1: sleep 3s
Step 2: fake HTTP call
Step 3: write result
Step 4: approval
Step 5: fake analysis
```

项目关注的是：

```text
execution durability
```

而不是：

```text
agent intelligence
```

---

# 11. Technology Stack

## Language

```text
Go
```

## Database

```text
PostgreSQL
```

推荐：

```text
pgx
pgxpool
```

MVP 不优先使用 ORM。

原因是：

```text
SQL transaction
row locking
task claiming
conditional updates
lease handling
```

本身就是项目最值得学习的内容。

## Infrastructure

MVP 只依赖：

```text
Go
PostgreSQL
Docker Compose
```

不引入 Redis、Kafka、NATS 等额外基础设施。

---

# 12. High-Level Architecture

```text
                    ┌──────────────┐
                    │     API      │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  PostgreSQL  │
                    │              │
                    │ Tasks        │
                    │ Steps        │
                    │ Events       │
                    └──────┬───────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
      ┌──────────┐    ┌──────────┐    ┌──────────┐
      │ Worker A │    │ Worker B │    │ Worker C │
      └──────────┘    └──────────┘    └──────────┘
```

PostgreSQL 同时承担：

```text
Persistent State Store
Task Queue
Checkpoint Store
Lease Coordination
```

---

# 13. Process Model

建议最终支持独立进程：

```bash
go run ./cmd/api
go run ./cmd/worker --id worker-a
go run ./cmd/worker --id worker-b
go run ./cmd/worker --id worker-c
```

这样可以真实测试：

```text
Worker A running task
↓
kill -9 Worker A
↓
lease expires
↓
Worker B claims task
↓
restore checkpoint
↓
continue execution
```

---

# 14. Go Concurrency Model

PostgreSQL 是 durable queue。

Channel 只负责单个 Worker 进程内部协调。

推荐：

```text
PostgreSQL
    │
    ▼
Poller Goroutine
    │
    ▼
Local Channel
    │
┌───┼────────┐
▼   ▼        ▼
G1  G2       G3
```

例如：

```go
type Worker struct {
    taskCh chan Task
}
```

Poller：

```text
claim task
↓
push to local channel
```

多个 goroutine：

```text
execute task
```

Channel 不是任务的永久存储。

因此：

```text
Worker process dies
```

不会导致任务永久丢失。

---

# 15. Task State Machine

建议第一版状态：

```text
create → SCHEDULED → RUNNABLE → RUNNING → SUCCEEDED
                       ▲          │
                       ├── RETRY_WAIT
                       └── WAITING_APPROVAL

RUNNING -- expired lease / new claim --> RUNNING
Any nonterminal state → FAILED / CANCELLED / TIMED_OUT
```

Task 不使用 `PENDING`；立即执行的 Task 创建为 `RUNNABLE`，未来执行的创建为 `SCHEDULED`。以上是概览，允许的转换及其条件以第 49 节为准。

Terminal states：

```text
SUCCEEDED
FAILED
CANCELLED
TIMED_OUT
```

典型转换：

```text
RUNNING → RETRY_WAIT
RUNNING → WAITING_APPROVAL
RUNNING → SUCCEEDED
RUNNING → FAILED
RUNNING → CANCELLED
RUNNING → TIMED_OUT
```

---

# 16. Step State Machine

Task 由固定数量的 durable steps 构成：

```text
Task
│
├── Step 1
├── Step 2
├── Step 3
├── Step 4
└── Step 5
```

Step 状态：

```text
PENDING
RUNNING
WAITING_APPROVAL
SUCCEEDED
FAILED
```

第一版本不需要：

```text
SKIPPED
PAUSED
PARTIAL
```

除非实现过程中确实产生需求。

---

# 17. Step Ordering

每个 Step 有：

```text
step_index
```

例如：

```text
0
1
2
3
4
```

一个 Step 只有在：

```text
所有前置 Step 已 SUCCEEDED
```

时才允许执行。

第一版因为是固定线性序列，所以：

```text
next_step = current_step + 1
```

即可。

不需要 DAG dependency resolver。

---

# 18. Checkpoint Model

Checkpoint 表示：

```text
Task 的 durable execution progress
```

例如：

```json
{
  "state": {
    "search_result": "...",
    "tool_output": "..."
  }
}
```

不过因为 Workflow 固定，核心恢复依据应该优先来自：

```text
task_steps
```

而不是完全依赖一个 JSON checkpoint。

恢复流程：

```text
load task
↓
load task_steps
↓
find last SUCCEEDED step
↓
find first non-SUCCEEDED step (including abandoned RUNNING)
↓
resume
```

Checkpoint JSONB 可以用于保存：

```text
跨 Step 的业务状态
中间数据
任务级 metadata
```

---

# 19. Task Claiming

多个 Worker 必须安全竞争 runnable task。

推荐使用 PostgreSQL：

```sql
BEGIN;

SELECT id
FROM tasks
WHERE status = 'RUNNABLE'
  AND run_at <= clock_timestamp()
  AND cancel_requested = false
  AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

然后在同一个 transaction：

```sql
UPDATE tasks
SET
    status = 'RUNNING',
    worker_id = $1,
    lease_version = lease_version + 1,
    lease_expires_at = clock_timestamp() + interval '30 seconds',
    first_started_at = COALESCE(first_started_at, clock_timestamp()),
    deadline_at = COALESCE(deadline_at,
        clock_timestamp() + task_timeout_seconds * interval '1 second')
WHERE id = $2;

COMMIT;
```

目标：

```text
Worker A
Worker B
Worker C
```

并发 poll 时不会取得同一个 Task。

这是首次领取的简化 SQL。实际 `ClaimTask` 同时支持锁定并接管租约到期的 `RUNNING` Task；锁定后重新检查资格，并在同一事务写入 Claim/Recovery 事件。无候选返回明确的 `NoTask`，不视作数据库错误。完整恢复规则见第 49 节。

---

# 20. Lease

Worker Claim 一个 Task 后，只获得：

```text
temporary ownership
```

即 Lease。

核心字段：

```text
worker_id
lease_version
lease_expires_at
```

例如：

```text
worker_id = worker-a
lease_version = 14
lease_expires_at = 10:00:30
```

Lease 到期意味着：

> 当前 Worker 不再被系统认为可靠拥有该 Task。

---

# 21. Heartbeat

Worker 在任务执行期间定期续租。

例如：

```text
lease duration = 30 seconds
heartbeat interval = 10 seconds
```

Heartbeat：

```sql
UPDATE tasks
SET lease_expires_at = clock_timestamp() + interval '30 seconds'
WHERE id = $1
  AND worker_id = $2
  AND lease_version = $3
  AND status = 'RUNNING'
  AND lease_expires_at > clock_timestamp()
  AND deadline_at > clock_timestamp()
  AND cancel_requested = false;
```

如果：

```text
rows affected = 0
```

Worker 必须认为：

```text
I no longer own this task.
```

然后取消当前 Task Context 并停止继续写状态。

已过期的租约不能通过 Heartbeat 复活。Heartbeat 返回取消、到期或失去租约的明确结果；数据库通信失败时停止启动新 Step，并取消当前执行，等待数据库恢复后由正常接管流程处理。不得把通信错误直接记作 Step 业务失败。

---

# 22. Fencing Token

`lease_version` 同时作为 fencing token。

例如：

```text
Worker A
lease_version = 5
```

Worker A 卡死。

Lease 超时后：

```text
Worker B
lease_version = 6
```

之后 Worker A 恢复。

Worker A 继续尝试：

```sql
UPDATE ...
WHERE lease_version = 5;
```

更新：

```text
0 rows
```

因此旧 Worker 无法继续修改任务状态。

---

# 23. Crash Recovery

Worker 崩溃：

```text
Worker A
    │
 executing
    │
   💥
```

数据库中 Lease 最终：

```text
lease_expires_at <= clock_timestamp()
```

其他 Worker 可以 reclaim。

恢复流程：

```text
find expired task
↓
acquire new lease
↓
load task
↓
load persisted step states
↓
determine next fixed step
↓
resume execution
```

固定 Step Sequence 使恢复过程是确定性的。

系统不需要重新调用 Agent Planner。

---

# 24. Retry

Step 执行失败时支持：

```text
attempt
max_attempts
retry_at
```

例如：

```text
attempt = 2
max_attempts = 5
```

失败：

```text
RUNNING
↓
RETRY_WAIT
```

设置：

```text
retry_at
```

到期：

```text
RETRY_WAIT
↓
RUNNABLE
```

然后继续执行失败的同一个 Step。

不会修改 Workflow。

`attempt/max_attempts` 只属于 Step，包含第一次执行；Task 不另设重试预算。`retry_at` 位于 Task，表示当前失败 Step 的唤醒时间。开始执行前持久化增加 attempt；崩溃遗留的 `RUNNING` 已消耗该次 attempt。详细规则见第 49 节。

---

# 25. Retry Strategy

MVP 支持：

```text
Exponential Backoff
```

例如：

```text
1s
2s
4s
8s
16s
```

暂时不实现：

```text
Retry DSL
Advanced Jitter Configuration
Complex Error Classification
Per-provider Retry Strategies
```

---

# 26. Idempotency

系统不宣称：

```text
Exactly Once Execution
```

采用：

```text
At-Least-Once Execution
+
Idempotency
```

每个 durable step 有：

```text
task_id
step_id
idempotency_key
```

例如：

```text
task-123:step-7
```

执行前检查：

```text
successful result already exists?
```

如果存在：

```text
reuse persisted result
```

否则：

```text
execute
↓
persist result
↓
advance task
```

固定 Step ID 也让 idempotency key 更容易保持稳定。

---

# 27. Important Failure Window

需要特别考虑：

```text
External API succeeds
↓
Worker crashes
↓
Checkpoint not saved
```

这是系统无法仅靠数据库 transaction 完全解决的问题。

第一阶段：

### Internal deterministic step

可以安全重新执行，或读取已有 Step Result。

### External API supporting idempotency key

使用稳定的：

```text
task_id + step_id
```

生成外部 idempotency key。

### External non-idempotent side effect

明确承认：

```text
exactly-once cannot be guaranteed
```

这应该记录在 README 的 reliability semantics 中。

---

# 28. Cancellation

API：

```text
POST /tasks/:id/cancel
```

数据库：

```text
cancel_requested = true
```

对 `SCHEDULED/RUNNABLE/RETRY_WAIT/WAITING_APPROVAL`，取消请求在事务内直接结束为 `CANCELLED`。对 `RUNNING`，先记录请求，Worker 或 Scheduler 完成终态转换；后续成功提交必须检查该标记。终态和竞争规则见第 49 节。

Worker 在：

```text
heartbeat
polling
step boundary
```

检查取消状态。

然后调用：

```go
cancel()
```

所有 Step Executor 都接受：

```go
context.Context
```

例如：

```go
func ExecuteStep(
    ctx context.Context,
    step Step,
) error
```

---

# 29. Context Propagation

Task Context：

```text
taskCtx
```

派生：

```text
taskCtx
│
├── heartbeatCtx
├── stepCtx
│    └── toolCtx
└── execution operation contexts
```

取消传播：

```text
Task cancel
↓
taskCtx.Done()
↓
step cancelled
↓
tool call cancelled
```

这是 Go 语言部分的重要训练点。

取消/超时后的最终状态落库使用独立、有短超时的控制 Context，不能复用已经取消的执行 Context。失去租约的 Worker 无权执行最终状态落库。进程退出、租约丢失、用户取消和业务超时必须保留不同原因。

---

# 30. Timeout

支持两个层级。

## Task Timeout

例如：

```text
30 minutes
```

字段：

```text
task_timeout_seconds
```

MVP 从首次 Claim 开始计算总墙钟时间，持久化 `first_started_at` 和 `deadline_at`。创建后的首次排队时间不计入；首次执行后的重试、审批等待、宕机和再次排队均计入。后续 Claim 不重置截止时间。Scheduler 负责无人执行时的到期处理。

## Step Timeout

例如：

```text
60 seconds
```

通过：

```go
context.WithTimeout
```

实现。

Step Timeout 是每次 attempt 的执行时限，恢复后新 attempt 重新计时，但不能超过 Task 剩余时间。所有执行器必须协作响应 Context；MVP 不支持强制终止不响应取消的任意代码。数据库终态不承诺撤回已经发生的外部副作用。

---

# 31. Human Approval

固定 Workflow 中可以预先定义：

```text
Step 4 = APPROVAL
```

执行到该 Step：

```text
RUNNING
↓
WAITING_APPROVAL
```

Worker 立即释放执行资源。

不能：

```go
<-approvalChannel
```

等待数小时。

Approval API：

```text
POST /tasks/:id/approve
POST /tasks/:id/reject
```

批准：

```text
WAITING_APPROVAL
↓
RUNNABLE
```

随后任意 Worker 可以 Claim 该 Task 并继续执行后续固定 Step。

批准请求必须包含当前审批的 `step_id`。同一事务完成审批 Step、保存决定、推进进度、写事件并恢复 Task；若该 Step 是最后一步，直接结束为 `SUCCEEDED`。拒绝结束为 `FAILED`，原因为 `approval_rejected`。重复及竞争规则见第 49 节。

---

# 32. Scheduled Execution

Task 包含：

```text
run_at
```

Scheduler：

```sql
UPDATE tasks
SET status = 'RUNNABLE'
WHERE status = 'SCHEDULED'
  AND run_at <= now();
```

Scheduler 可以是独立 process。

Scheduler 还负责唤醒到期 `RETRY_WAIT`、结束到期 Task，以及完成已请求的取消。每项转换锁定 Task 后重新判断，状态、Step 和事件在同一事务提交；多实例可安全竞争。过期租约由 Worker 的 `ClaimTask` 直接接管。

MVP 推荐：

```bash
go run ./cmd/scheduler
```

---

# 33. Minimal Database Schema

建议从三个核心表开始。

## tasks

```text
id
tenant_id

status

created_at
updated_at

run_at

worker_id
lease_version
lease_expires_at

retry_at

cancel_requested

current_step

checkpoint JSONB

task_timeout_seconds
first_started_at
deadline_at
```

---

## task_steps

```text
id
task_id

step_index
step_type

status

attempt
max_attempts

idempotency_key

input JSONB
resolved_input JSONB
output JSONB
error
approval_decision  # nullable: approved / rejected

timeout_seconds

started_at
finished_at
```

建议增加约束：

```text
UNIQUE(task_id, step_index)
UNIQUE(idempotency_key)
```

另要求 Task 外键、合法状态约束、`attempt >= 0`、`max_attempts >= 1`、正数 timeout、非空唯一幂等键。`run_at` 非空，省略时取创建时间。创建请求必须含至少一个 Step，索引从 0 连续排列，类型与输入通过白名单校验。Task 与 Step 的定义字段创建后不可改；运行状态字段仍按状态机更新。

`current_step` 表示首个未成功 Step 的索引，全部成功时等于 Step 数量；`task_steps` 是进度事实来源，checkpoint 仅保存跨步骤数据，不重复保存进度索引。非 `RUNNING` Task 的 `worker_id/lease_expires_at` 必须为空，`lease_version` 保留且单调增加。终态 Task 不得遗留 `RUNNING/WAITING_APPROVAL` Step；当前未完成 Step 标记 `FAILED` 并记录原因，未执行的后续 Step 保持 `PENDING`。

队列索引至少覆盖各状态下的 `run_at`、`retry_at`、`lease_expires_at`、`deadline_at`；迁移中建立对应部分索引。MVP 不实现 tenant concurrency limit，`tenant_id` 仅作可选 metadata。

---

## task_events

```text
id
task_id
event_type
payload JSONB
created_at
```

典型事件：

```text
TASK_CREATED
TASK_SCHEDULED
TASK_RUNNABLE
TASK_CLAIMED

STEP_STARTED
STEP_SUCCEEDED
STEP_FAILED

TASK_RETRYING
LEASE_EXPIRED
TASK_RECOVERED

TASK_WAITING_APPROVAL
TASK_APPROVED
TASK_REJECTED

TASK_CANCELLED
TASK_TIMED_OUT
TASK_FAILED
TASK_SUCCEEDED
```

---

# 34. Workflow Creation

Task 创建时，同时创建所有 Steps。

例如：

```text
BEGIN

INSERT task

INSERT step 0
INSERT step 1
INSERT step 2
INSERT step 3
INSERT step 4

COMMIT
```

因此一个 Task 从创建开始，其 Workflow 就已经完整持久化。

Worker 不负责生成 Workflow。

---

# 35. Suggested Go Project Structure

```text
cmd/
  api/
    main.go

  worker/
    main.go

  scheduler/
    main.go

internal/

  task/
    model.go
    state.go
    step.go

  worker/
    worker.go
    poller.go
    heartbeat.go
    executor.go

  scheduler/
    scheduler.go

  store/
    postgres.go
    tasks.go
    steps.go
    events.go

  runtime/
    executor.go
    checkpoint.go
    retry.go
    idempotency.go

  api/
    handler.go

  events/
    events.go

migrations/

docker-compose.yml
go.mod
```

原则：

> 不要过早抽象。

不要一开始构建：

```text
generic workflow framework
plugin architecture
repository factory
storage driver abstraction
```

---

# 36. Core Store Interface

接口保持小。

例如：

```go
type Store interface {
    ClaimTask(
        ctx context.Context,
        workerID string,
    ) (*Task, error)

    RenewLease(
        ctx context.Context,
        taskID string,
        workerID string,
        leaseVersion int64,
    ) error

    LoadTask(
        ctx context.Context,
        taskID string,
    ) (*Task, error)

    LoadSteps(
        ctx context.Context,
        taskID string,
    ) ([]Step, error)

    StartStep(
        ctx context.Context,
        lease LeaseToken,
        stepID string,
    ) (StepAttempt, error)

    FinishStep(
        ctx context.Context,
        lease LeaseToken,
        attempt StepAttempt,
        outcome StepOutcome,
    ) error
}
```

这表示 Worker 所需的行为接口，不要求提前建立可替换的 Storage 抽象。`LeaseToken` 包含 Task ID、Worker ID、lease_version；`StepAttempt` 标识 Step 和 attempt 编号。`StepOutcome` 为成功结果或失败原因/是否可重试，输入输出须有明确类型与大小限制。

`StartStep` 原子验证执行资格并增加 attempt；`FinishStep` 原子处理 Step、checkpoint、Task 进度、重试/终态、租约释放和事件，调用方不得再单独推进 `current_step`。审批、取消和 Scheduler 使用对应的 Store 行为方法，复用同一套事务规则。错误至少区分 `NoTask`、`LeaseLost`、`Conflict`、`Cancelled`、`DeadlineExceeded` 和数据库错误。

暂时不为了未来：

```text
SQLite
MySQL
Redis
```

设计复杂 Storage abstraction。

---

# 37. Worker Execution Loop

概念流程：

```text
poll
↓
claim task
↓
create task context
↓
start heartbeat
↓
load fixed steps
↓
find next executable step
↓
execute step
↓
persist result
↓
advance current_step
↓
next fixed step
```

伪代码：

```go
for {
    // 先取得空闲执行容量；失败/无任务时归还容量。
    if err := slots.Acquire(ctx); err != nil {
        return
    }
    task, err := store.ClaimTask(ctx, workerID)

    if err != nil {
        slots.Release()
        backoff(ctx)
        continue
    }

    // 从 Claim 成功起管理租约；发送须响应进程取消。
    dispatchWithLease(ctx, taskCh, task)
}
```

Executor Pool：

```go
for task := range taskCh {
    executeTask(ctx, task)
    slots.Release()
}
```

Worker concurrency 例如：

```text
8
```

不要为每个 Task 无限制创建 goroutine。

上述为概念代码：容量覆盖已领取、交接中和执行中的全部任务，禁止额外预取；交接失败必须停止租约管理并归还容量，任务随后按租约恢复。Heartbeat 从 Claim 成功开始，首次执行前再次验证所有权。也可以由固定数量 Executor 各自 Claim，避免交接队列。

---

# 38. Next Step Resolution

因为 Workflow 固定，恢复和普通执行使用同一套逻辑：

```text
load all steps ordered by step_index
↓
find first step whose status is not SUCCEEDED
↓
check whether it is executable
↓
execute
```

例如：

```text
0 SUCCEEDED
1 SUCCEEDED
2 FAILED retryable
3 PENDING
4 PENDING
```

下一步仍是：

```text
Step 2
```

而不是 Step 3。

---

# 39. Transaction Boundaries

MVP 中必须特别关注 transaction。

例如 Step 成功：

```text
BEGIN

update task_steps
set status = SUCCEEDED,
    output = ...

update tasks
set current_step = current_step + 1,
    checkpoint = ...

insert task_events

COMMIT
```

目标：

```text
Step Result
+
Task Progress
+
Event
```

必须原子提交；事务内先锁定 Task 并验证有效租约和执行资格，再更新 Step、Task 和 Event。任一检查失败或预期更新行数不符，整个事务回滚。不能只跳过 Task 更新却提交 Step 和 Event。

不能出现：

```text
step = SUCCEEDED
task.current_step = old value
```

这种内部不一致。

---

# 40. Observability

MVP 不做完整 Observability Platform。

但必须有结构化日志。

推荐字段：

```text
task_id
step_id
step_index
worker_id
lease_version
event
attempt
```

例如：

```text
task=123
step=step-6
worker=worker-b
lease=17
attempt=1
event=step_started
```

这样 crash recovery 能完整追踪。

---

# 41. Minimal API

第一阶段 API：

```text
POST /tasks
GET  /tasks/:id

POST /tasks/:id/cancel

POST /tasks/:id/approve
POST /tasks/:id/reject
```

可选：

```text
GET /tasks/:id/events
```

Task 创建请求需要包含固定 Step 定义。

概念示例：

```json
{
  "steps": [
    {
      "type": "sleep",
      "input": {
        "duration": "3s"
      }
    },
    {
      "type": "fake_tool",
      "input": {}
    },
    {
      "type": "approval",
      "input": {}
    }
  ]
}
```

Task 创建之后：

```text
steps immutable
```

---

# 42. Development Phases

## Phase 1 — Fixed Workflow + Durable State

实现：

```text
task table
task_steps table
fixed step creation
sequential execution
state machine
checkpoint
```

成功标准：

```text
restart process
↓
load fixed workflow
↓
resume from correct step
```

---

## Phase 2 — Multiple Workers

加入：

```text
SKIP LOCKED
lease
heartbeat
lease_version
multiple worker processes
```

成功标准：

```text
3 workers concurrently process tasks
without duplicate claiming
```

---

## Phase 3 — Crash Recovery

加入：

```text
kill -9
lease expiration
reclaim
resume
```

成功标准：

```text
kill Worker A mid-task
↓
Worker B eventually resumes task
```

---

## Phase 4 — Reliability

加入：

```text
retry
timeout
cancellation
idempotency
```

---

## Phase 5 — Human Approval

加入：

```text
approval step
WAITING_APPROVAL
approve API
resume
```

---

## Phase 6 — Scheduler

加入：

```text
run_at
SCHEDULED → RUNNABLE
```

---

## Phase 7 — Optional Features

最后再考虑：

```text
parallel child tasks
tenant concurrency limits
priority
metrics
```

这些不是 MVP 核心。

---

# 43. Testing Strategy

这个项目不能只测试 Happy Path。

Failure Testing 是核心。

## Test 1 — Crash Before Step

```text
Worker crashes before Step starts
```

预期：

```text
another Worker can reclaim task
```

---

## Test 2 — Crash After Step Commit

```text
Step committed successfully
↓
Worker crashes
```

预期：

```text
completed Step remains SUCCEEDED
next Worker starts next Step
```

---

## Test 3 — Lose Lease

```text
Worker A loses lease
Worker B gets new lease
```

预期：

```text
Worker A cannot persist further progress
```

---

## Test 4 — Concurrent Claim

```text
Worker A
Worker B
```

同时 Claim。

预期：

```text
same Task only belongs to one Worker
```

---

## Test 5 — Cancellation

```text
Task cancelled
while long Step is running
```

预期：

```text
Context cancellation reaches Step Executor
```

---

## Test 6 — Timeout

```text
Step execution exceeds timeout
```

预期：

```text
context deadline exceeded
↓
retry or fail
```

---

## Test 7 — Waiting Approval Restart

```text
Task enters WAITING_APPROVAL
↓
all processes restart
```

预期：

```text
WAITING_APPROVAL survives restart
```

---

## Test 8 — Workflow Immutability

Task 创建后尝试改变：

```text
Step order
Step count
Step type
```

预期：

```text
not supported / rejected
```

---

## Test 9 — Recovery Resolution

数据库：

```text
Step 0 SUCCEEDED
Step 1 SUCCEEDED
Step 2 PENDING
Step 3 PENDING
```

Worker 重启。

预期：

```text
resume Step 2
```

---

# 44. Engineering Questions

开发过程中应该不断问：

1. 一个 Step 到底什么时候算完成？
2. Task ownership 的准确含义是什么？
3. Worker 崩溃时哪些状态可能处于中间态？
4. Lease 过期后旧 Worker 是否还能写数据库？
5. Step 成功和 Task Progress 更新是否原子？
6. Retry 是否可能产生重复副作用？
7. Cancellation 是否能够传递到正在执行的 Tool？
8. Queue 中的重要状态是否全部 Durable？
9. Scheduler 重启会不会丢任务？
10. 两个 Worker 同时 Claim 时会发生什么？
11. 恢复时如何确定下一个 Step？
12. Workflow 是否可能在运行中发生变化？

对于第 12 个问题，MVP 的答案明确是：

> **No. Workflow is immutable after Task creation.**

---

# 45. Reliability Semantics

MVP 应明确声明：

系统目标：

```text
Durable execution
At-least-once step execution
Idempotent retry where possible
Crash recovery
Stale-worker fencing
```

不保证：

```text
Exactly-once external side effects
Distributed consensus
Zero duplicate execution under arbitrary external failures
```

这样项目的可靠性边界是清晰的。

---

# 46. MVP Definition

最终 MVP：

> **A PostgreSQL-backed durable task runtime written in Go, executing fixed predefined workflows across multiple worker processes with leases, checkpoints, retries, cancellation and crash recovery.**

核心：

```text
Fixed Workflow
+
Postgres-backed State Machine
+
Worker Lease
+
Checkpoint
+
Retry
+
Cancellation
+
Crash Recovery
```

---

# 47. Architectural Philosophy

项目不应该变成一个简化版完整 Temporal。

原则：

```text
small scope
deep semantics
failure-oriented design
```

优先真正做好：

```text
task ownership
step lifecycle
transaction boundary
lease
checkpoint
idempotency
recovery
```

宁可只有几个可靠 Feature，也不要堆很多半完成 Feature。

---

# 48. First Implementation Target

不要先写完整 API。

首先做一个最小 Crash Recovery Demo。

固定 Task：

```text
Step 1 — sleep
Step 2 — fake tool
Step 3 — sleep
Step 4 — fake tool
Step 5 — sleep
```

启动：

```bash
go run ./cmd/worker --id worker-a
```

运行中：

```bash
kill -9 <worker-a>
```

再启动：

```bash
go run ./cmd/worker --id worker-b
```

期望日志：

```text
task=123 lease_expired worker=worker-a

task=123
worker=worker-b
event=task_claimed
lease_version=2

task=123
event=task_recovered
completed_step=2

task=123
step=3
event=step_started
```

核心要求：

> 已经成功提交的 Step 不能因为 Worker Crash 而重新从 Step 1 开始。

只要这个 Demo 成功，项目最重要的架构就已经成立。

之后按照：

```text
Crash Recovery
↓
Multiple Workers
↓
Retry
↓
Cancellation
↓
Approval
↓
Scheduler
```

逐步增加能力。

---

# 49. Normative Execution Contract

本节是实现与验收契约；前文概念图及伪代码不代表完整实现。

## 49.1 Ownership and Atomic Writes

修改同一 Task 的事务必须先锁 Task 行，再访问当前 Step。锁内禁止执行工具、HTTP 或 sleep。Worker 写入要求 Task 为 RUNNING、worker_id 和 lease_version 匹配且租约未到期；正常推进与续租还要求无取消请求、未到 Task deadline。

有效性检查使用获得行锁后的数据库当前时间，作为事务的逻辑生效点。已通过检查的短事务可以完成，竞争接管必须等待锁释放再检查；配置有限的锁等待与事务执行超时。所有状态、进度和事件共同提交或回滚。API/Scheduler 通过合法控制转换结束或唤醒任务，不冒充 Worker 所有权。

退出 RUNNING 必须清空 worker_id/lease_expires_at，保留 lease_version；接管时版本递增。租约过期不可续租复活。Fencing 只保护 Runtime 数据，不保证外部工具停止或撤回副作用。

FinishStep 还必须检查当前 Step ID、attempt 编号和 RUNNING 状态。重复完成返回 Conflict，不再次推进或追加事件。数据库提交响应丢失时重读持久化事实，不得直接重复工具调用或记为业务失败。

## 49.2 State Transition Table

| 当前状态 | 触发者及条件 | 同一事务的结果 |
| --- | --- | --- |
| 不存在 | API 创建合法固定序列 | 创建 Task、全部 Step、事件；未来 run_at 为 SCHEDULED，否则 RUNNABLE |
| SCHEDULED | Scheduler：run_at 到期 | RUNNABLE，写事件 |
| RUNNABLE | Worker：有容量、无取消、未超时 | RUNNING，分配租约，版本加一；首次设置 deadline，写 Claim 事件 |
| RUNNING | Worker：租约过期、无取消、未超时 | 重新分配租约并增加版本，处理遗留 Step，写租约过期/恢复/Claim 事件 |
| RUNNING | Worker：当前普通 Step 成功 | Step SUCCEEDED、保存 output/checkpoint、推进 current_step、写事件；最后一步则 Task SUCCEEDED 并释放租约 |
| RUNNING | Worker：可重试失败且有预算 | Step FAILED、Task RETRY_WAIT、设置 retry_at、释放租约、写事件 |
| RUNNING | Worker：永久失败或预算耗尽 | Step FAILED、Task FAILED、释放租约、写事件 |
| RETRY_WAIT | Scheduler：retry_at 到期 | RUNNABLE、清空 retry_at、写事件；失败 Step 保留供下次开始 |
| RUNNING | Worker：当前 Step 为审批 | Step 和 Task 均 WAITING_APPROVAL、释放租约、写事件 |
| WAITING_APPROVAL | API：批准当前 step_id | 保存决定、Step SUCCEEDED、推进进度和事件；有后续则 RUNNABLE，否则 SUCCEEDED |
| WAITING_APPROVAL | API：拒绝当前 step_id | 保存决定、Step FAILED、Task FAILED，原因 approval_rejected，写事件 |
| 非 RUNNING 的非终态 | API：取消 | Task CANCELLED、结束当前未完成 Step、写事件 |
| RUNNING | API：取消请求 | 设置 cancel_requested；Worker 或 Scheduler 后续完成 CANCELLED、结束当前 Step、释放租约、写事件 |
| 任意非终态 | Worker/Scheduler：deadline 到期 | TIMED_OUT、结束当前未完成 Step、释放租约、写事件 |

未列出的转换拒绝；终态不可覆盖。结束当前 Step 指：若存在首个非成功 Step，将其置 FAILED 并保存原因，后续 PENDING Step 不变。Scheduler 锁内先处理已请求取消，再判断 deadline，最后判断唤醒；正常推进和审批也必须检查取消及 deadline。

## 49.3 Attempts and Recovery

普通 Step 只有 `PENDING/FAILED → RUNNING → SUCCEEDED/FAILED`。仅当前 Step 可开始；StartStep 先持久化增加 attempt，再执行工具。max_attempts 默认 3，包含第一次执行，可在创建时指定正整数。started_at/finished_at 表示最近 attempt，历史保存在事件中。

第 n 次业务失败后退避 `min(2^(n-1), 60)` 秒。仅执行器显式标记的暂时错误和 Step Timeout 可重试；非法输入、未知类型、永久错误直接失败。用户取消、Task Timeout、租约丢失不走业务重试分支。

接管时已成功 Step 不执行；遗留 RUNNING Step 在接管事务内记 FAILED、原因 worker_lost，并记录该 attempt 结束事件。预算耗尽则 Task FAILED 并释放租约；否则由新所有者立即 StartStep，增加 attempt，沿用原幂等键，不额外退避。Claim 后但 StartStep 前崩溃不消耗 attempt；StartStep 提交后即使工具尚未调用也消耗一次。持续崩溃允许耗尽预算，不承诺无限恢复。

若全部 Step 已成功，恢复事务收敛 Task 为 SUCCEEDED；WAITING_APPROVAL 不得自动越过。Approval 为控制步骤，不消耗 attempt；等待受 Task deadline 限制。

## 49.4 Control Requests and Race Resolution

批准/拒绝请求体必须包含 step_id。相同 Step 的相同已保存决定重复请求返回 200，包含原决定及当前 Task 状态，不新增事件、不推进；相反决定返回 409。未有决定时，仅当前 WAITING_APPROVAL 可决定，其他状态返回 409；资源不存在返回 404，非法参数返回 400。旧审批重放不能批准后续 Step。

取消已 CANCELLED Task 返回 200；取消其他终态返回 409；同步结束非运行任务返回 200；RUNNING 保存取消请求后返回 202，重复请求亦同。202 不表示工具已停止。

成功与取消通过行锁串行化：成功先提交进入终态则取消冲突；取消请求先提交则后续成功写入拒绝。批准先提交后可取消尚未完成的 Task；取消先提交则审批失败。已记录决定的重复请求只返回原决定。

Scheduler 与取消轮询默认每 1 秒一次；Heartbeat 每 10 秒、租约 30 秒。数据库不可用时不保证实时取消；Worker 停止新执行并取消当前 Context。进程退出不等于用户取消，不写 CANCELLED，遗留任务通过租约恢复。

## 49.5 Durable Input and Executor Contract

创建时的 Step 类型、顺序、配置 input 和幂等键不可变。首次 StartStep 从已提交的前置 output/checkpoint 解析输入，保存独立 `resolved_input JSONB`；后续 attempt 重用，不得用相同 key 发送不同请求。前文允许运行时 input 变化仅指首次生成解析结果，不是覆盖定义。

MVP 使用编译时注册的固定执行器，接受 Context、resolved_input、idempotency_key，返回 output 或带重试分类的错误，不直接修改 Runtime 表。不支持运行中替换执行器语义；升级期间有未完成任务时须维持执行器兼容。

默认限制最多 100 个 Step，创建请求和每份输入、输出、checkpoint 各不超过 1 MiB。未知类型、非法输入及非正 timeout/max_attempts 在创建时拒绝；输出超限视为不可重试错误。

---

# 50. Failure Acceptance Matrix

涉及事务、行锁和并发的测试使用真实 PostgreSQL；进程测试运行编译后的二进制，终止真实 Worker PID。使用可控屏障/故障注入定位窗口，不依赖随机 sleep；断言数据库事实和事件，日志不能作为唯一证据。

| 注入点或竞争场景 | 必须断言 |
| --- | --- |
| 多 Worker 同时 Claim | 只有一个有效租约，版本与事件一致 |
| Claim 后、StartStep 前崩溃 | 接管继续原 Step，未提前消耗 attempt |
| StartStep 后、执行中崩溃 | 遗留 RUNNING 正确结束，新 attempt 沿用 key，前置成功 Step 不重跑 |
| Step、Task、Event 写入之间失败 | 全部回滚或全部提交 |
| COMMIT 成功但响应丢失 | 重读确认，不重复推进或写成功事件 |
| 租约到期尚未接管，旧 Worker 续租/提交 | 均拒绝，不能复活租约 |
| 接管后旧 Worker 迟到提交 | Step、Task、Event 均不被旧 Worker 修改 |
| 满载超过一个租约周期 | 不超容量 Claim，交接期间仍管理租约 |
| 重试等待期间重启、到期唤醒 | 同一步恢复，预算和退避不重置 |
| 连续崩溃耗尽预算 | Task FAILED，不无限恢复 |
| fake tool 副作用已持久化，Runtime 提交前崩溃 | 相同 key 重试返回原结果，副作用计数为 1 |
| 相同 key、不同 payload | fake tool 拒绝冲突，Runtime 使用保存的 resolved_input |
| Step 超时 | 按 Step 预算处理，Task deadline 不变 |
| 审批/重试/宕机期间 Task 到期 | TIMED_OUT，重启/批准/Claim 不延长期限 |
| 取消全部非运行状态 | 无需 Worker Claim 即 CANCELLED，不再次唤醒 |
| 运行中取消与成功竞争 | 按提交顺序裁决，取消后成功写入拒绝，Context 传递到工具 |
| 执行 Context 取消后最终落库 | 独立有界 Context 可以落库；失去租约者仍不可写 |
| 审批重复、相反决定、与取消竞争 | 唯一有效转换，冲突明确，无重复事件 |
| 两个审批 Step，重放第一次请求 | 返回第一步决定，不批准第二步 |
| 最后一步为审批 | 批准直接 SUCCEEDED，拒绝 FAILED |
| 创建事务失败、非法定义、超限结果 | 无残缺 Workflow，明确拒绝或终态失败 |

首条实现切片包含真实数据库、两个 Worker、固定 Step、事务提交、租约、执行中崩溃接管和迟到提交拒绝。第 42 节的分阶段不能把 fencing 与原子性推迟到该切片之后。随后完成有预算的重试与幂等 fake tool，再加入取消、超时、审批及 Scheduler。

快速自动故障测试与第 3 节约 30 分钟进程演示都须通过。`Completed durable steps re-executed: 0` 仅指已成功提交的 Step，不代表未提交 attempt 或任意外部副作用具备 exactly-once 保证。
