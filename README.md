# runState

runState 是一个用 Go 和 PostgreSQL 实现的固定步骤 durable task runtime。当前支持通过 HTTP 创建与查询立即执行的 `echo` / `sleep` / `flaky` 步骤，多个 Worker 以数据库租约竞争任务，并由独立 Scheduler 持久唤醒暂时失败的步骤。

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
```

`run_at`、审批步骤以及其控制接口属于后续 ticket；当前创建请求会明确拒绝这些定义。Scheduler 默认每秒扫描一次，可通过 `RUNSTATE_SCHEDULER_POLL_INTERVAL` 调整；可同时运行多个实例。

`flaky` 是用于故障验收的可控执行器。`temporary_failures` 表示成功前暂时失败的次数，`permanent` 表示立即返回永久错误，两者不能同时生效：

```json
{"steps":[{"type":"flaky","input":{"temporary_failures":2,"value":"eventual result"}}]}
```

Step 默认最多执行 3 次，包含首次执行。第 n 次暂时失败后持久等待 `min(2^(n-1), 60)` 秒；等待时释放 Worker 租约，Scheduler 到期后恢复同一步。永久错误或预算耗尽会明确结束为 `FAILED`。

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
- 这保证 durable execution 与 stale-worker fencing，不承诺任意外部副作用 exactly-once。
