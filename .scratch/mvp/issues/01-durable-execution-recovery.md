# 01 — 固定任务的持久执行与崩溃接管

**What to build:** 用户通过 HTTP 创建并查询固定顺序任务；两个宿主机 Worker 连接 Compose PostgreSQL 执行最小普通 Step。执行中杀死一个 Worker 后，另一进程接管，已提交步骤不重跑，旧 Worker 不能提交迟到结果。

**Blocked by:** None — can start immediately

**Status:** resolved

- [x] 提供固定明确镜像版本的 Compose PostgreSQL、健康检查、仅本机绑定端口、named volume，以及宿主机 Go 进程连接配置。开发和测试使用独立数据库，测试连接不得回退到开发连接；并行测试的数据空间隔离。
- [x] 提供启动、停止和显式重置说明。普通停止/启动及保留卷的容器重建不丢开发数据，测试清理不影响开发/演示任务；健康检查和迁移完成后再运行测试。
- [x] HTTP 创建非空固定普通 Step Sequence 并可查询 Task、按序 Step 及结果。Task、全部 Step、创建事件原子创建；索引连续、类型白名单、唯一幂等键、外键和状态约束有效。立即任务创建为 RUNNABLE。
- [x] 提供最小协作式普通执行器，类型和定义创建后不变；拒绝非法输入、未知类型、非正 timeout/max_attempts、超过 100 步或 1 MiB 创建请求。每份输入、输出和 checkpoint 的 1 MiB 限制生效，输出超限明确失败。尚未实现的审批和定时请求明确拒绝，不能接受后卡住。
- [x] 每个 Step 只在前置全部成功后执行。StartStep 先持久化 attempt 和 resolved_input；FinishStep 原子保存 Step、output/checkpoint、current_step 和事件，最后一步同时结束 Task 并释放租约。checkpoint 不重复保存进度索引。
- [x] 多 Worker 通过真实 PostgreSQL 行锁安全 Claim；有空闲容量才领取，从 Claim 起管理租约。租约默认 30 秒、Heartbeat 10 秒；交接失败可恢复，满载超过租约周期不产生超容量预取。
- [x] Worker 写入锁定 Task 后验证 RUNNING、所有者、lease_version 和数据库时间下的有效租约；接管版本递增，退出 RUNNING 清空所有者/到期时间。过期未接管也不能续租或提交；过期接管后旧 Worker 的 Step、Task、事件写入全部拒绝。
- [x] 事务先锁 Task 后访问 Step，设置有限锁等待/执行超时，锁内不运行工具。验证 Step ID、attempt 和状态，重复 FinishStep 不再次推进；结果、进度和事件写入途中故障全回滚，提交响应丢失时重读事实。
- [x] 遗留 RUNNING attempt 以 worker_lost 结束；有预算则立即新 attempt，耗尽则 Task FAILED。max_attempts 默认 3，包含首次执行；Claim 后、StartStep 前崩溃不计数，StartStep 提交后即计数；已成功 Step 不重跑。业务失败暂时直接失败，延迟重试由后续票增加。
- [x] 首次 Claim 保存 first_started_at/deadline_at，接管不得重置，过期任务不得正常推进；完整超时处理由后续票完成。取消标记检查和终态不可覆盖作为事务不变量保留。
- [x] 无任务、冲突、失去租约和数据库错误可区分。数据库通信失败停止新执行并取消当前 Context，进程退出不写 CANCELLED；输出含 Task、Step、Worker、版本、attempt 的结构化日志及持久事件。
- [x] 通过 HTTP 与编译后的真实 Worker 二进制验证顺序执行、并发 Claim、各崩溃窗口和恢复；必要的 Store 故障测试仍连接真实 PostgreSQL。明确记录测试命令与结果，不用内存 fake 或日志替代数据库断言。

## Comments

- 2026-09-21：按已批准拆分发布。第一切片包含租约、fencing 和事务原子性，不延后到可靠性收尾票。
- 2026-09-21：实现完成。`make test` 通过真实 Compose PostgreSQL、HTTP、编译 Worker、进程 kill、并发 Claim、事务故障、COMMIT 响应丢失、容量和恢复矩阵；`go vet ./...` 与带同一数据库/Worker 配置的 `go test -race ./... -count=1` 通过。双轴 review 的 resolved_input、checkpoint、提交事实重读及缺失故障窗口问题已修正。
