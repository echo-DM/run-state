# 03 — 外部工具调用的幂等恢复

**What to build:** 用户运行调用外部 fake tool 的固定任务；即使工具副作用已成功而 Worker 尚未提交结果就崩溃，接管后也可通过同一幂等键取得原结果，避免重复副作用。

**Blocked by:** 02 — 暂时失败的持久化重试

**Status:** ready-for-agent

- [ ] 提供独立于 Worker 内存生命周期的 fake tool，通过真实调用边界接收幂等键及 payload，持久保存副作用、请求身份和结果；去重与副作用记录必须原子。
- [ ] 相同 key 和相同 payload 返回原结果；相同 key 不同 payload 返回明确冲突，不新增副作用。并发相同 key 也只产生一次副作用。
- [ ] Runtime 首次 StartStep 从已提交的前置 output/checkpoint 解析并持久化 resolved_input；每次重试和接管复用原 input/key，工具执行器不直接修改 Runtime 表。
- [ ] 用故障屏障在工具副作用成功、Runtime 结果提交前杀死真实 Worker；接管后最终成功，工具副作用计数为 1，Runtime 进度只推进一次。
- [ ] 验证普通业务重试、工具响应丢失和 Runtime 提交响应丢失，不会因重新计算输入改变请求身份；HTTP 查询提供最终持久结果。
- [ ] 文档明确这只证明支持幂等键的工具行为；不将本地唯一约束或已成功 Step 跳过等同于通用 exactly-once，非幂等外部副作用不作去重承诺。
- [ ] 真实 PostgreSQL、fake tool 和 Worker 进程测试可重复运行，留下数据库结果、调用结果和副作用计数证据。

## Comments

- 2026-09-21：按已批准拆分发布；fake tool 去重状态不得随着 Worker 重启消失。
