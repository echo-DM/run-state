# 06 — 指定步骤的人工审批与恢复

**What to build:** 用户提交包含审批的固定任务，执行到审批时释放 Worker 并持久等待；批准指定步骤后继续，拒绝则结束，重复或迟到请求不会审批错误对象。

**Blocked by:** 05 — 任务取消与终态竞争

**Status:** ready-for-agent

- [ ] 创建接口接受 Approval Step；到达时原子将 Step/Task 置 WAITING_APPROVAL、写事件并释放租约和执行容量，不保持等待审批的执行 goroutine。
- [ ] 审批为控制步骤，不消耗 attempt；记录开始/完成时间，只受 Task deadline 限制。所有进程重启后仍等待指定 Step。
- [ ] approve/reject 必须携带 step_id，只能决定当前等待步骤。批准保存决定、完成 Step、推进进度和事件，有后续则 RUNNABLE，最后一步直接 SUCCEEDED。
- [ ] 拒绝原子令 Step/Task FAILED，原因 approval_rejected；后续 Step 不执行。
- [ ] 同 Step 相同决定重放返回 200、原决定及当前 Task 状态，不再推进或写事件；相反决定或错误状态返回 409，不存在返回 404，非法参数返回 400。
- [ ] 两个 Approval Step 场景下，重放第一次批准只能返回第一次决定，不能批准第二步；已保存决定的查询式重放不改变当前 Task。
- [ ] 取消审批等待立即 CANCELLED；等待期间 deadline 到期由 Scheduler 结束 TIMED_OUT。审批锁内检查取消及 deadline，不复活终态。
- [ ] HTTP 与真实进程覆盖批准/拒绝、全进程重启、资源释放、最后一步审批、重复/迟到请求及与取消/超时的竞争；状态、决定和事件原子一致。

## Comments

- 2026-09-21：按已批准拆分发布；无需依赖外部工具集成或定时执行。
