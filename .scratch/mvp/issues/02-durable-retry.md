# 02 — 暂时失败的持久化重试

**What to build:** 用户提交的步骤暂时失败后，任务释放执行资源并在持久化退避结束时恢复同一步；进程重启不重置预算，永久错误或预算耗尽明确结束。

**Blocked by:** 01 — 固定任务的持久执行与崩溃接管

**Status:** ready-for-agent

- [ ] 执行器返回明确的暂时/永久错误。暂时失败且有预算时，原子写 Step FAILED、Task RETRY_WAIT、retry_at、事件，并释放租约；永久错误或耗尽预算原子结束 Task FAILED。
- [ ] Step 独占 attempt/max_attempts，默认最多 3 次且包含首次执行；第 n 次失败退避 min(2^(n-1), 60) 秒。Task 不另设预算。
- [ ] 独立 Scheduler 默认每秒扫描，锁定并重新检查到期条件，将 RETRY_WAIT 转为 RUNNABLE、清空 retry_at 并写事件；多个 Scheduler 不产生重复有效转换。
- [ ] 通过 HTTP 创建可控失败任务，查询可观察到等待、重试及终态；恢复仅执行失败的同一步，前置成功步骤不重跑，resolved_input 和 key 不变。
- [ ] 重试等待期间重启所有进程，预算与 retry_at 保留；连续业务失败及连续 Worker 崩溃均可耗尽预算并结束。worker_lost 恢复不附加业务退避。
- [ ] 租约丢失、数据库错误及进程退出不误归类为业务重试。唤醒事务预留取消/到期资格检查，终态永不重新唤醒。
- [ ] 使用真实 PostgreSQL 和进程验收退避、重启、并发唤醒、永久失败及预算耗尽；事件和状态保持原子一致。

## Comments

- 2026-09-21：按已批准拆分发布；Scheduler 的首个完整用途为重试唤醒。
