# 03 — 外部工具调用的幂等恢复

**What to build:** 用户运行调用外部 fake tool 的固定任务；即使工具副作用已成功而 Worker 尚未提交结果就崩溃，接管后也可通过同一幂等键取得原结果，避免重复副作用。

**Blocked by:** 02 — 暂时失败的持久化重试

**Status:** resolved

- [x] 提供独立于 Worker 内存生命周期的 fake tool，通过真实调用边界接收幂等键及 payload，持久保存副作用、请求身份和结果；去重与副作用记录必须原子。
- [x] 相同 key 和相同 payload 返回原结果；相同 key 不同 payload 返回明确冲突，不新增副作用。并发相同 key 也只产生一次副作用。
- [x] Runtime 首次 StartStep 从已提交的前置 output/checkpoint 解析并持久化 resolved_input；每次重试和接管复用原 input/key，工具执行器不直接修改 Runtime 表。
- [x] 用故障屏障在工具副作用成功、Runtime 结果提交前杀死真实 Worker；接管后最终成功，工具副作用计数为 1，Runtime 进度只推进一次。
- [x] 验证普通业务重试、工具响应丢失和 Runtime 提交响应丢失，不会因重新计算输入改变请求身份；HTTP 查询提供最终持久结果。
- [x] 文档明确这只证明支持幂等键的工具行为；不将本地唯一约束或已成功 Step 跳过等同于通用 exactly-once，非幂等外部副作用不作去重承诺。
- [x] 真实 PostgreSQL、fake tool 和 Worker 进程测试可重复运行，留下数据库结果、调用结果和副作用计数证据。

## Comments

- 2026-09-21：按已批准拆分发布；fake tool 去重状态不得随着 Worker 重启消失。
- 2026-09-21：实现独立 HTTP fake tool 与 PostgreSQL 幂等 ledger；并发同 key/payload 仅一次副作用，不同 payload 返回 409。真实 Worker/Scheduler/fake-tool 进程测试覆盖副作用提交后杀 Worker、工具响应超时重试、普通业务重试及 Runtime COMMIT 响应丢失，均复用原 key/resolved_input；HTTP 最终结果、唯一成功事件与副作用计数一致。
- 2026-09-21：验证通过：`make test`（真实 Compose PostgreSQL，全部包与进程测试）、`go test -race ./... -count=1`、`go vet ./...`、`git diff --check`。Standards review 无硬性违规，统一 executor registry 后消除重复分派；Spec review 的 envelope 上限、4xx 分类、真实响应丢失路径与唯一推进四项 finding 均已修复并复核通过。
