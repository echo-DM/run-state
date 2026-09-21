# 04 — Step 超时与持久化 Task 截止时间

**What to build:** 用户指定步骤和任务超时后，慢步骤按预算重试，整个 Task 在固定截止时间结束；重启、宕机或等待不会延长生命周期。

**Blocked by:** 02 — 暂时失败的持久化重试

**Status:** resolved

- [x] 创建时校验正数 Task/Step timeout；首次 Claim 设置 Task 的 first_started_at 和 deadline_at，之后不重置。首次 Claim 前排队不计时，之后的运行、重试、宕机和排队均计入。
- [x] 每个 Step attempt 的 Context 截止时间为 Step 时限与 Task 剩余时间的较早者；新 attempt 重新计算 Step 时限但复用 Task deadline。
- [x] Step Timeout 在 Task 尚未到期且仍有预算时进入业务重试；Task 到期则 TIMED_OUT，不进入 RETRY_WAIT。记录不同错误原因。
- [x] Worker 和 Scheduler 能原子结束到期任务、当前未完成 Step及事件并释放租约；无人执行或 Worker 已崩溃仍可结束。到期任务不得 Claim、续租或正常成功提交。
- [x] 最终控制落库使用独立有界 Context，不复用已超时执行 Context；失去租约的 Worker 仍无写权限。终态不可覆盖。
- [x] Scheduler 先检查取消请求，再处理 deadline，最后唤醒重试；多实例竞争只发生一次有效终态转换。
- [x] HTTP 和真实进程测试覆盖 Step 重试、Task 执行中/重试中/宕机中到期、接管不延时及成功提交竞争；使用协作式慢执行器验证 Context 传播。
- [x] 明确不支持强制终止不响应 Context 的任意代码，不声称超时撤回已发生的外部副作用。审批等待超时随审批票验收。

## Comments

- 2026-09-21：按已批准拆分发布；依赖重试 Scheduler，不依赖外部幂等工具票。
- 2026-09-21：实现持久 Task deadline 与每次 attempt 的 Step/Task 较早 deadline；Worker 使用独立 2 秒控制 Context 落库超时，Scheduler 按取消、deadline、retry 顺序原子处理，旧租约和终态均受 fencing 保护。
- 2026-09-21：新增真实 PostgreSQL、HTTP 与编译进程验收，覆盖 Step timeout 重试、Worker 独立收尾、RETRY_WAIT 到期、Worker 崩溃期间到期、接管不重置 deadline、多 Scheduler 与迟到成功竞争；明确 Context 协作及外部副作用边界。
- 2026-09-21：验证通过：focused timeout tests、`make test`（tests 18.398s）、`go vet ./...`、`go test -race ./... -count=1`（tests 20.750s）、`git diff --check`。Standards/Spec 双轴复审均无剩余 finding。
