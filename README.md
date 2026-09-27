# FastAPI Task Orchestrator（任务提交 / 执行 / 状态查询）

一个**进程内**的本地任务编排服务，基于 FastAPI + 有界工作线程池实现，重点解决三件事：

1. **提交幂等**：相同客户端令牌的重复提交只对应一个任务，绝不重复执行；同一令牌提交不同内容会被拒绝并返回可区分的错误码。
2. **受限并发与排队**：并发度与等待队列都有硬上限，超载时可预期地拒绝（不会无限堆积、丢失或重复执行）。
3. **运行中取消与失败分类**：取消协作式中断执行，取消后不再产出结果；完成与取消几乎同时发生时，由一次锁内的原子判定产生唯一终态。

> 说明：状态保存在单进程内存中，重启即清空；定位是本地可跑、行为可解释的参考实现，不是分布式持久化调度器。

## 目录结构

```
app/
  models.py    # 状态机、请求/响应/视图模型、失败类别
  errors.py    # 错误码体系（每种失败有稳定 code + HTTP 状态码）
  store.py     # 内存任务表 + 令牌指纹 + 有界在途计数 + 状态流转
  runner.py    # 协作式可取消的任务执行体（含超时与异常分类）
  executor.py  # 有界工作线程池、完成/取消竞态的唯一终态提交
  service.py   # 提交（幂等编排）、取消、查询
  api.py       # HTTP 路由、错误映射、生命周期
main.py        # 本地启动入口
tests/         # 状态机 / 幂等 / 并发排队 / 取消竞态 / HTTP 契约测试
```

## 任务状态机

```
                 ┌─────────────────────── cancel（排队中立即取消）
                 ▼
  PENDING ───────────────► RUNNING ──成功──► SUCCEEDED   （终态）
    ▲                        │  │
    │（无回流边）             │  ├──失败──► FAILED        （终态）
    │                        │  │
    └────                    └──┴──取消──► CANCELLED     （终态）
```

| 边 | 触发者 | 说明 |
| --- | --- | --- |
| PENDING → RUNNING | worker 出队 | 只有 worker 能让任务开始运行 |
| PENDING → CANCELLED | `POST /tasks/{id}/cancel` | 排队中取消，立即生效，**永不会执行** |
| RUNNING → SUCCEEDED | worker 提交成功结果 | 仅当尚未收到取消请求（锁内原子判定） |
| RUNNING → FAILED | worker（任务抛异常 / 超时） | 附带分类化的错误信息 |
| RUNNING → CANCELLED | 取消信号 | 协作式中断；取消获胜时结果被丢弃 |

- 三个终态 `SUCCEEDED / FAILED / CANCELLED` 是**封闭**的：没有任何出边。
- 任何非法流转（如对已成功的任务取消、PENDING 直接 SUCCEEDED）都会被明确拒绝，
  返回 `409 invalid_state_transition`。

## 幂等规则

- 每个请求必须带 `client_token`（非空字符串）。
- 服务对 **payload + timeout_seconds** 做规范化指纹（排序 JSON）：
  - **令牌相同 + 指纹相同** → 返回同一个 `task_id`，响应中 `duplicate=true`；不会再次入队或执行。
    无论任务是排队中、运行中还是已终态，行为都一致（可安全重试）。
  - **令牌相同 + 指纹不同** → 返回 `409 idempotency_conflict`，原始任务不受影响。
- 「重复提交」与「内容冲突」「队列已满」「任务不存在」使用彼此不同的错误码，便于客户端区分处理。

## 并发与排队规则

- `TASK_MAX_WORKERS`（默认 2）：同时运行任务数的**硬上限**，由固定数量的工作线程保证。
- `TASK_MAX_QUEUE`（默认 100）：等待队列容量。
- **在途容量** = `workers + queue`。提交时在同一把锁内做「令牌判重 + 在途计数 + 入队」：
  - 容量未满 → 接受并入队；
  - 容量已满 → 返回 `503 queue_full`（可重试），任务不会被悄悄丢弃或无限堆积；
  - 排队中被取消的任务会立刻释放其在途槽位。
- worker 通过 Condition 唤醒，`入队先于 notify` + 条件锁内复查保证不丢唤醒；即使唤醒丢失，50ms 轮询兜底。
- 每个任务只可能被一个 worker 取出执行（出队与 PENDING→RUNNING 在锁内完成），因此**不会重复执行**。

## 取消与「完成/取消」竞态

- runner 是**协作式**的：每 20ms（或 `cancel_event.wait` 中）检查一次取消信号，因此运行中取消能快速生效。
- 终态由 executor 在 **store 锁内原子提交**，规则确定：
  - runner 自己观察到取消 → `CANCELLED`，不产出结果；
  - runner 成功返回，但提交时发现取消信号已到 → **取消获胜**，转 `CANCELLED` 且结果被丢弃；
  - runner 成功提交后取消才到达 → **完成获胜**，取消请求得到 `409 invalid_state_transition`；
  - runner 失败（异常/超时）→ `FAILED`，携带错误分类。
- 因此「取消」和「正常完成」几乎同时发生时，**有且只有一个终态、一个 finished_at**；已 CANCELLED 的任务永远不带 result。
- 对已经 CANCELLED 的任务再次取消是幂等 no-op（200）。

## 失败原因分类（`error.category`）

| category | 含义 |
| --- | --- |
| `execution_error` | 任务体自身抛出异常（含 `exception_type` 与 message） |
| `task_timeout` | 任务执行超过 `timeout_seconds` |
| `cancelled` | 取消获胜（排队中取消或运行中取消） |

错误码（HTTP body 中 `error.code`）：

| code | HTTP | 场景 |
| --- | --- | --- |
| `task_not_found` | 404 | 任务 ID 不存在 |
| `idempotency_conflict` | 409 | 同一令牌提交了不同内容 |
| `invalid_state_transition` | 409 | 非法状态流转（如取消已完成任务） |
| `queue_full` | 503 | 在途容量已满，可稍后重试 |

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务；body：`{"client_token": "...", "payload": {...}, "timeout_seconds": 0.5}` |
| GET  | `/tasks/{task_id}` | 查询状态/结果/错误（排队中含 `position_in_queue`） |
| POST | `/tasks/{task_id}/cancel` | 请求取消 |
| GET  | `/healthz` | 健康检查 |
| GET  | `/stats` | 各状态计数、`queue_depth`、`inflight/max_inflight` |

Demo payload 约定（`payload` 字段）：

- `sleep`：浮点秒数，可被取消中断；
- `raise`：为 `true` 时在 sleep 后抛出 `RuntimeError(message)`；
- `message`：上述异常使用的消息。

## 本地验证方法

依赖：Python 3.14、[uv](https://docs.astral.sh/uv/)。

```bash
# 1. 安装依赖
uv sync

# 2. 跑测试（-s 可实时看到 令牌/任务状态/判定依据 日志）
uv run pytest -s
uv run pytest tests/test_concurrency.py tests/test_cancellation.py -s

# 3. 启动服务（可用环境变量调节并发与队列）
TASK_MAX_WORKERS=2 TASK_MAX_QUEUE=100 uv run uvicorn main:app --reload
# 或： uv run python main.py

# 4. 另开终端手工验证
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"client_token":"t1","payload":{"sleep":2}}'
# -> {"task_id":"...","status":"pending","duplicate":false}

# 重复提交（同令牌同内容）-> 同一 task_id，duplicate=true
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"client_token":"t1","payload":{"sleep":2}}'

# 同令牌不同内容 -> 409 idempotency_conflict
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"client_token":"t1","payload":{"sleep":3}}'

# 运行中取消
curl -s -X POST http://127.0.0.1:8000/tasks/<task_id>/cancel

# 查询状态
curl -s http://127.0.0.1:8000/tasks/<task_id>
curl -s http://127.0.0.1:8000/stats
```

## 测试覆盖说明

| 测试文件 | 覆盖点 |
| --- | --- |
| `tests/test_state_machine.py` | 状态流转表、终态封闭、非法边拒绝、各错误码可区分 |
| `tests/test_idempotency.py` | 20 路并发相同提交只产生 1 个任务且只执行 1 次；同令牌不同内容被拒；完成后重放不重跑；并发混合「重放/冲突」不覆盖原任务 |
| `tests/test_concurrency.py` | 60 路突发提交不丢不重；在途容量硬上限（满则 `queue_full`），取消释放槽位；观测并发峰值不超过 worker 数；排队位置 FIFO |
| `tests/test_cancellation.py` | 排队中取消不执行、运行中取消无结果、60 轮完成/取消竞态每轮唯一终态、取消幂等、失败原因三分类 |
| `tests/test_api.py` | HTTP 状态码、错误码、JSON 结构与端到端取消 |

所有关键用例的日志都会打印 **client token、task_id、任务状态与判定依据（verdict）**，用 `pytest -s` 可直接观察。
