# 05 — 任务取消与终态竞争

**What to build:** 用户能通过 HTTP 取消执行中或等待中的任务；取消信号传递到协作式工具，取消与成功、超时竞争时状态保持一致。

**Blocked by:** 04 — Step 超时与持久化 Task 截止时间

**Status:** ready-for-agent

- [ ] cancel 对非 RUNNING 非终态任务同步原子结束为 CANCELLED，结束首个未成功 Step、保存原因和事件；后续未执行 Step 保持 PENDING。
- [ ] 对 RUNNING 保存 cancel_requested 并返回 202；Worker/独立 Scheduler 将其结束并释放租约。202 仅表示请求持久化，不表示工具已停止。
- [ ] 已 CANCELLED 返回 200，其他终态返回 409；同步取消返回 200，RUNNING 重复请求返回 202；缺失资源返回 404。
- [ ] 取消检查默认每秒，传播到当前 Step/tool Context，停止启动新 Step。数据库不可用和不协作工具不承诺硬实时取消。
- [ ] Task 行锁裁决：成功先提交终态则取消冲突；取消先保存则迟到成功写入拒绝。取消已请求时 Scheduler 优先取消，再判断 deadline，事件不重复。
- [ ] 使用独立有界控制 Context 落最终状态，区分用户取消、超时、失去租约和进程退出；失去租约者不能写 CANCELLED。
- [ ] HTTP/真实进程覆盖运行中、RUNNABLE、RETRY_WAIT 取消，无需领取等待任务即可结束；覆盖重复请求、取消/成功/超时竞争和当前 Step 一致性。
- [ ] 控制转换支持 SCHEDULED/WAITING_APPROVAL 的同步取消；这些状态的公开创建与端到端验收分别由定时和审批票完成，不提供伪造状态的生产入口。

## Comments

- 2026-09-21：按已批准拆分发布；继承超时控制事务及 Scheduler 优先级。
