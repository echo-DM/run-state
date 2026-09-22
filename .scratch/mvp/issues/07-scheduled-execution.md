# 07 — 一次性定时执行

**What to build:** 用户提交未来 run_at 的固定任务，到期后由任意 Worker 执行；Scheduler 重启不丢任务，开始前可以取消。

**Blocked by:** 05 — 任务取消与终态竞争

**Status:** resolved

- [x] 创建接口接受一次性 run_at：未来时间创建为 SCHEDULED，省略或已到期则 RUNNABLE；非法时间拒绝，持久化时间具有明确时区含义。
- [x] 独立 Scheduler 到期锁定并重新检查条件，原子转换 RUNNABLE 并写事件；到期前 Worker 不领取。Scheduler 默认每秒扫描，采用数据库时间。
- [x] 首次排队不消耗 Task timeout，首次 Claim 才设置 deadline；调度和随后重试分别使用 run_at 与 retry_at，不混淆两者。
- [x] 取消 SCHEDULED 同步结束，取消先于唤醒生效时不得再执行；与到期唤醒竞争通过同一 Task 锁裁决。
- [x] Scheduler 重启及多个实例竞争均不丢任务、不重复有效转换，终态不被唤醒；处理优先级保持取消、deadline、唤醒。
- [x] 通过 HTTP 和真实进程验收未来任务、立即任务、重启恢复、并发 Scheduler、开始前取消及查询状态，不靠手工修改数据库模拟完整用户路径。
- [x] 文档和接口范围明确只支持一次性调度，不加入 Cron、周期任务、日历调度或 misfire 策略。

## Comments

- 2026-09-21：按已批准拆分发布；可在取消票完成后独立于审批票实施。
- 2026-09-22：实现 RFC3339 明确时区的 run_at 创建、数据库时间裁决的 SCHEDULED/RUNNABLE 状态、带事件原子唤醒、一次性调度索引及开始前取消；补充真实 PostgreSQL/Worker/Scheduler 重启与多实例、retry_at 分离、事务回滚和 HTTP 验收，并更新 README。首次完整测试有一例既有 Worker deadline 测试超时；隔离重复 5 次通过，后续完整 `make test` 通过（tests 31.390s），`go vet ./...` 与 `git diff --check` 通过。
