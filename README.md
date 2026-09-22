# runState

runState 是一个用 Go 和 PostgreSQL 实现的固定步骤 durable task runtime。当前支持通过 HTTP 创建与查询立即执行的 `echo` / `sleep` / `flaky` / `fake_tool` / `approval` 步骤，多个 Worker 以数据库租约竞争任务，并由独立 Scheduler 持久唤醒暂时失败或等待超时的任务。

## 本地启动

需要 Go 1.24+、Docker 与 Docker Compose。PostgreSQL 使用固定的 `postgres:17.6-alpine` 镜像，仅监听 `127.0.0.1:54329`，开发数据保存在 Compose named volume 中。

```bash
make db-up
make migrate
go run ./cmd/api
```

在另外的终端启动一个或多个宿主机 Worker：

```bash
go run ./cmd/worker --id worker-a --concurrency 1
go run ./cmd/worker --id worker-b --concurrency 1
go run ./cmd/scheduler
go run ./cmd/fake-tool
```

默认开发连接是：

```text
postgres://runstate:runstate@127.0.0.1:54329/runstate_dev?sslmode=disable
```

可用 `RUNSTATE_DATABASE_URL` 覆盖。其他配置见 [.env.example](.env.example)。进程不会自动执行迁移；应在 API/Worker 启动前运行 `make migrate`。

创建并查询任务：

```bash
curl -sS -X POST http://127.0.0.1:8080/tasks \
  -H 'content-type: application/json' \
  -d '{"steps":[{"type":"echo","input":{"value":"hello"}},{"type":"sleep","input":{"duration":"2s"}}]}'

curl -sS http://127.0.0.1:8080/tasks/<task-id>
curl -sS http://127.0.0.1:8080/tasks/<task-id>/events
curl -sS -X POST http://127.0.0.1:8080/tasks/<task-id>/cancel
```

取消 `RUNNABLE`、`RETRY_WAIT` 等非运行任务会在请求事务内结束为 `CANCELLED` 并返回 200；取消 `RUNNING` 任务只先持久化请求并返回 202，Worker 或 Scheduler 随后完成终态转换。重复取消已取消任务返回 200，重复请求仍在运行的任务返回 202，取消其他终态返回 409。202 只表示请求已持久化，不表示工具已经停止。

审批是固定 Workflow 中的控制步骤，不运行工具、不增加 attempt，也不占用 Worker 执行槽。Worker 到达 approval step 后会把 Step 和 Task 持久化为 `WAITING_APPROVAL` 并释放租约；进程重启后仍等待同一个 `step_id`。审批等待只受 Task deadline 限制。使用当前待审批 Step 的 ID 作出决定：

```bash
curl -sS -X POST http://127.0.0.1:8080/tasks/<task-id>/approve \
  -H 'content-type: application/json' \
  -d '{"step_id":"<step-id>"}'

curl -sS -X POST http://127.0.0.1:8080/tasks/<task-id>/reject \
  -H 'content-type: application/json' \
  -d '{"step_id":"<step-id>"}'
```

批准会记录决定并推进到下一 Step；批准最后一步会直接结束为 `SUCCEEDED`。拒绝会以 `approval_rejected` 结束为 `FAILED`。相同 Step 的同一决定重放返回 200，包括原决定和当前 Task 状态，不会重复推进或写事件；相反决定、迟到决定或非当前 Step 返回 409，Task/Step 不存在返回 404，请求缺少 `step_id` 或 JSON 非法返回 400。取消审批等待会立即结束为 `CANCELLED`，Task deadline 到期由 Scheduler 结束为 `TIMED_OUT`。

`run_at` 仍属于后续 ticket；当前创建请求会明确拒绝定时任务定义。Scheduler 默认每秒扫描一次，可通过 `RUNSTATE_SCHEDULER_POLL_INTERVAL` 调整；可同时运行多个实例。

`flaky` 是用于故障验收的可控执行器。`temporary_failures` 表示成功前暂时失败的次数，`permanent` 表示立即返回永久错误，两者不能同时生效：

```json
{"steps":[{"type":"flaky","input":{"temporary_failures":2,"value":"eventual result"}}]}
```

Step 默认最多执行 3 次，包含首次执行。第 n 次暂时失败后持久等待 `min(2^(n-1), 60)` 秒；等待时释放 Worker 租约，Scheduler 到期后恢复同一步。永久错误或预算耗尽会明确结束为 `FAILED`。

`fake_tool` 是独立于 Worker 生命周期的 HTTP 服务，默认监听 `127.0.0.1:8090`。Worker 通过 `RUNSTATE_FAKE_TOOL_URL` 调用它，并发送 Step 的稳定 `idempotency_key` 与已持久化的 `resolved_input`。fake tool 在独立 ledger 中原子保存请求、副作用和结果：相同 key/payload 返回原结果，并发调用也只产生一次副作用；相同 key/不同 payload 返回冲突。

```json
{"steps":[{"type":"fake_tool","input":{"value":"charge once"}}]}
```

后续步骤可在允许任意 JSON 值的字段中使用只含一个字段的引用对象，首次 `StartStep` 会从同一事务中已提交的事实生成并保存 `resolved_input`：

```json
{"value":{"$ref":"previous_output"}}
{"value":{"$ref":"checkpoint"}}
```

`previous_output` 表示紧邻前一步的完整输出，因此第一步不能使用；`checkpoint` 表示当前 Task 的业务 checkpoint。重试和崩溃接管复用已保存的 `resolved_input`，不会用同一幂等键重新解析成不同请求。checkpoint 只保存跨步骤业务数据，不保存步骤编号等进度信息。

## 数据库生命周期

普通停止、启动和保留卷的容器重建不会删除开发数据：

```bash
make db-stop
make db-up

make db-down
make db-up
```

显式重置会删除 named volume 中的开发与测试数据，无法恢复：

```bash
make db-reset
make db-up
make migrate
```

`runstate_dev` 与 `runstate_test` 是独立数据库。测试要求 `RUNSTATE_TEST_DATABASE_URL`，并拒绝指向 `runstate_dev`；每个测试再创建独立 schema，因此并行执行不会共享 Task 数据。测试清理只删除自己的 schema，不影响开发或演示任务。

## 验证

完整测试会先等待数据库健康、编译真实 Worker 与 Scheduler，然后覆盖 HTTP 行为、并发 Claim/唤醒、事务回滚、持久退避、进程重启、租约过期、旧 Worker fencing 和真实进程崩溃接管：

```bash
make test
```

也可以显式运行：

```bash
make db-up
make build
RUNSTATE_TEST_DATABASE_URL='postgres://runstate:runstate@127.0.0.1:54329/runstate_test?sslmode=disable' \
RUNSTATE_WORKER_BINARY="$PWD/bin/runstate-worker" \
RUNSTATE_FAKE_TOOL_BINARY="$PWD/bin/runstate-fake-tool" \
go test ./... -count=1
```

## 当前可靠性语义

- PostgreSQL 是 Task、Step、checkpoint、租约和事件的事实来源；Worker 内存可丢弃。
- 每次 Claim 增加 `lease_version`。所有 Worker 写入验证 Task、Worker、版本及数据库时间下仍有效的租约。
- `StartStep` 提交后 attempt 即消耗；Claim 后、`StartStep` 前崩溃不消耗 attempt。
- `StartStep`/`FinishStep` 若丢失 COMMIT 响应，会通过独立的有界 Context 重读 attempt、结果、进度与事件；只有持久事实完整匹配才视为成功。
- 已成功提交的 Step 不会在接管时重跑；遗留 `RUNNING` attempt 记录为 `worker_lost`。
- 暂时业务错误和 Step timeout 在剩余预算内进入 `RETRY_WAIT`；永久错误、输出超限或预算耗尽直接失败。租约丢失、数据库错误和进程退出不作为业务重试写入。
- `retry_at`、attempt 与错误均保存在 PostgreSQL；Scheduler 只唤醒数据库时间下已到期且仍符合资格的等待任务。
- Task 总时限从首次 Claim 开始，`first_started_at` 与 `deadline_at` 只设置一次；之后的执行、重试等待、Worker 宕机和再次排队都消耗同一份墙钟预算，首次 Claim 前的排队不消耗。
- 每个 Step attempt 使用 Step 时限与 Task 剩余时间中较早的 deadline。`step_timeout` 在 Task 尚未到期且有预算时重试；Task 到期原子写入 `TIMED_OUT`/`task_timeout`，不会进入或唤醒 `RETRY_WAIT`。
- Scheduler 按取消请求、Task deadline、重试唤醒的顺序处理控制转换；多实例依靠行锁只产生一次有效终态事件。Worker 的超时收尾使用独立的短 Context，但仍必须通过 Worker/租约版本 fencing。
- Worker 默认每秒检查持久化取消请求，并把取消传播到当前 Step/tool 的 `context.Context`；它或 Scheduler 使用独立的短控制 Context 写入 `CANCELLED`/`user_cancelled`，清理租约且不启动后续 Step。成功、取消与超时由 Task 行锁裁决，失去租约者不能写取消终态。
- 对支持幂等键的外部工具，Worker 崩溃、普通重试或响应丢失后会复用原 key/input，从工具端取得已保存结果。测试用 fake tool 的 ledger 与 Worker 内存生命周期独立。
- 这保证 durable execution、at-least-once Step execution 与 stale-worker fencing；只对支持幂等键且遵守相同 payload 契约的工具验证去重。Runtime 本地唯一约束或跳过已成功 Step 都不等于通用 exactly-once；非幂等外部副作用不作去重承诺。
- Timeout 依赖执行器协作响应 `context.Context`；Runtime 不强制终止忽略 Context 的任意代码，也不承诺超时能撤回已经发生的外部副作用。审批等待的 deadline 行为由审批功能票验收。
- Cancellation 同样是协作式的：数据库不可用或工具忽略 Context 时不承诺硬实时停止，也不能撤回已发生的外部副作用；进程退出不会被记录为用户取消。
