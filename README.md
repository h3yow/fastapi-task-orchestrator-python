# FastAPI Task Orchestrator

一套本地可跑的任务提交 / 执行 / 状态查询服务，保证：

- **提交幂等**：相同客户端令牌只对应一个任务，重复提交不重复执行；
- **内容冲突可区分**：同一令牌提交不同内容会被拒绝，错误码 `TOKEN_CONFLICT`，与其他失败明确区分；
- **受限并发**：固定工作线程 + 有界排队，超出容量可预期地拒绝（`429 / QUEUE_FULL`），不无限堆积、不丢失、不重复；
- **运行中取消**：排队任务直接取消不执行，运行中任务协作式中断；取消后不再产出结果；
- **终态唯一**：取消与正常完成竞态时，由存储层同一把锁决定唯一终态。

## 任务状态机

```
                 排队满拒绝
        ┌─────────────────────────┐
        │                         ▼
   ┌─────────┐ 取走执行   ┌─────────┐            ┌───────────┐
   │ PENDING │ ─────────▶ │ RUNNING │ ─成功────▶ │ SUCCEEDED │
   └─────────┘            └─────────┘            └───────────┘
        │                     │ ├────────────▶ │ FAILED    │
        │取消(排队中)          │ 失败分类       └───────────┘
        ▼                     ├────────────▶ ┌───────────┐
   ┌───────────┐              │ 取消/完成竞态 │ CANCELLED │
   │ CANCELLED │ ◀────────────┘ 锁竞争二选一  └───────────┘
   └───────────┘
```

合法流转（其余流转一律抛 `IllegalTransitionError`）：

| 当前状态 | 可流转到 |
| --- | --- |
| PENDING | RUNNING、CANCELLED、FAILED（排队拒绝） |
| RUNNING | SUCCEEDED、FAILED、CANCELLED |
| SUCCEEDED / FAILED / CANCELLED | （终态，不可再流转） |

**终态唯一保证**：所有状态流转都在 `TaskStore` 的锁内完成。运行中取消时，
取消方与工作线程竞争 `RUNNING → CANCELLED / SUCCEEDED / FAILED` 的流转权，
先拿到锁的一方决定终态，另一方的流转被拒绝——因此取消与完成几乎同时发生时，
只会产生一个确定结果；若取消获胜，工作线程已算出的结果会被丢弃。

## 幂等与冲突规则

| 情形 | 行为 | HTTP |
| --- | --- | --- |
| 首次提交（token + payload） | 创建并排队，返回 `201` | 201 |
| 相同 token + 相同 payload 重复提交 | 返回已有任务，`deduplicated: true`，**不重复执行** | 200 |
| 相同 token + 不同 payload | 拒绝，错误码 `TOKEN_CONFLICT` | 409 |
| 排队队列已满 | 拒绝本次接收，任务记录为 `FAILED / QUEUE_FULL`（原因可查询） | 429 |

判定与写入在 `TaskStore.get_or_create` 同一把锁内原子完成，因此高并发下
同一令牌也只会创建一个任务。

## 失败原因分类

任务失败时 `error_kind` 可区分：

- `PAYLOAD_ERROR`：payload 非法（不是合法 JSON 对象等）
- `TASK_ERROR`：任务体主动报告的业务失败（payload 中 `"fail": true`）
- `INTERNAL`：任务体抛出的未预期异常
- `QUEUE_FULL`：因排队容量满而被拒绝

接口级错误码：`TOKEN_CONFLICT`(409)、`QUEUE_FULL`(429)、
`TASK_NOT_FOUND`(404)、`INVALID_STATE`(409，如对已结束任务取消)。

## API

默认任务体：`payload` 为 JSON 字符串，支持
`{"duration_ms": 运行毫秒数, "fail": true/false}`，便于本地模拟耗时、失败与取消。

```bash
# 提交
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"token":"client-token-1","payload":"{\"duration_ms\": 2000}"}'

# 查询（返回 status / result / error / error_kind）
curl -s http://127.0.0.1:8000/tasks/<task_id>

# 取消（排队中立即取消；运行中协作式中断）
curl -s -X POST http://127.0.0.1:8000/tasks/<task_id>/cancel
```

## 本地运行

```bash
uv sync
uv run python main.py                 # http://127.0.0.1:8000，文档 /docs
```

## 本地验证（测试）

```bash
uv run pytest tests/ -q               # 13 个用例
uv run pytest tests/ -o log_cli=true  # 日志含 token、任务状态与判定依据（VERDICT）
```

覆盖场景：

- 同令牌重复提交：返回同一任务、只执行一次；
- 同令牌不同内容：`TOKEN_CONFLICT` 拒绝，与其他失败码可区分；
- 16 线程并发提交同一令牌：恰好创建一个任务、执行一次；
- 30 个不同令牌高并发提交：每个恰好执行一次，不丢不重；
- 排队中取消：任务从未执行；运行中取消：终态 CANCELLED 且无 result；
- 取消/完成竞态：50 轮迭代，终态唯一且自洽（CANCELLED 必无结果）；
- 队列满：`QUEUE_FULL` 拒绝且任务可查询；
- 失败分类：`PAYLOAD_ERROR` / `TASK_ERROR` 可区分；
- 非法状态流转被拒绝。

测试日志形如：

```
VERDICT token=race task=f473... status=SUCCEEDED error_kind=None \
  basis=16 线程并发同令牌提交：仅 1 个创建、1 个任务 ID、执行 1 次
```

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `app/models.py` | 任务模型与状态机（合法流转表） |
| `app/store.py` | 线程安全存储；令牌幂等判定、加锁状态流转 |
| `app/executor.py` | 有界队列 + 固定工作线程；满则非阻塞拒绝 |
| `app/service.py` | 编排：提交/排队/取消/失败分类，处理竞态 |
| `app/api.py` | FastAPI 路由与错误码到 HTTP 状态码的映射 |
