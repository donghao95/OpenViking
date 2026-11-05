# 队列停滞恢复守护

当 HTTP 健康检查正常而持久队列停止推进时，按实际完成计数恢复运行。该工具不修复锁租约失效根因，也不证明知识采集或 Compile 完整。

独立进程每分钟执行一次，读取原生鉴权 API：队列完成与失败数（包括 `Semantic-Nodes`）、模型调用数、运行任务及阶段。积压增长、重新入队计数增长不算进展。任一真实进展都会重新计时；仅凭任务运行时间长不会重启。

默认恢复条件：

- 有积压、10 分钟无进展，且同一停滞窗口出现锁租约失效错误。
- 有积压、40 分钟无进展；单次模型或媒体处理超时更长时，至少等待该超时再加 2 分钟。
- 已建立成功观察基线后，鉴权业务 API 持续 10 分钟无法连接、超时或返回 5xx。

401、403、接口不存在和响应结构变化只报告探测错误，不触发重启。维护暂停文件存在或部署锁被占用时不执行检查。监控状态与重启意图写入权限 `0600` 的独立文件；不记录凭据、业务正文、来源名称或原始错误响应。

恢复优先调用原生 `/api/v1/admin/restart`，使用重新读取的配置 revision。请求结果不明确时保留意图，不重放 POST。业务 API 已不可用时，仅重启配置指定且 Compose 标签匹配的原容器。原生重启超过 10 分钟仍不可达时，允许一次容器重启升级。每次动作均计入持久预算：重启间隔至少 1 小时，任意 6 小时最多 2 次；仅上述一次升级可越过 1 小时冷却。耗尽后持续报告 `recovery_budget_exhausted`，不无限重启。

本工具不取消任务、不重新发起来源请求、不删除或编辑 Task、队列、锁、WAL 或数据库；未确认消息由 OV 原生启动恢复。重启可能中断当前请求，恢复投递遵循原生至少一次语义；如果同一消息不断复发或底层存储故障，预算会停止恢复，仍需人工处理。已经失败的历史任务保留失败状态。

可选配置既有自动接续服务。仅当其原状态文件的 phase 为 `collecting` 且服务为 `failed` 时恢复该服务；不会启动被人工停止的 `inactive` 服务，不改变原来的来源、投递回执、幂等键或 Compile 决策。恢复间隔至少 1 小时。

配置保存在服务器受控目录中，仅引用既有 Root 接入文件：

```json
{
  "base_url": "http://127.0.0.1:1933",
  "access_file": "/srv/ov/secrets/root-access.private.json",
  "observer_access_file": "/srv/ov/secrets/user-access.private.json",
  "account_id": "example-account",
  "user_id": "example-admin",
  "container": "example-openviking-1",
  "compose_project": "example",
  "compose_service": "openviking",
  "state_file": "/srv/ov/recovery/state.private.json",
  "pause_file": "/srv/ov/recovery/paused",
  "deployment_lock": "/srv/ov/deployment.lock"
}
```

`access_file` 使用原生接入材料的 `root_api_key` 字段；`observer_access_file` 从既有用户接入材料的 `users` 数组选择指定 `user_id`，用于读取该账户真实模型用量。值不复制到此配置。Root 身份的模型表可能仅有就绪状态，不包含用户调用统计；未提供用户文件时仍能观察队列和任务进展。部署锁必须是既有部署进程实际使用的 flock 文件。容器重启需要 Docker 访问权限；可选自动接续恢复需要对应 systemd 服务管理权限。

先观察，确认接口、状态文件和范围均正确，再显式启用恢复：

```sh
python3 watchdog.py --config /srv/ov/recovery/config.private.json
python3 watchdog.py --config /srv/ov/recovery/config.private.json --apply
python3 -m unittest discover -s scripts/maintenance/queue_watchdog -p 'test_*.py' -v
```

用 systemd oneshot service 与每分钟 timer 运行固定提交的脚本，设置 `TimeoutStartSec=180`，仅给该脚本所需的状态目录、接入文件、Docker 和可选服务管理权限。停用 timer 即可回退；暂停文件适合临时维护。不要为了验证恢复而故意卡住生产模型、取消业务任务或向生产队列注入错误；隔离测试覆盖停滞、持续进展、鉴权失败、维护窗口、未知 POST、容器身份、进程重建、冷却与预算。
