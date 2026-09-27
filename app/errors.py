"""服务层错误分类，对外可区分。"""


class OrchestratorError(Exception):
    """所有编排器错误的基类，code 用于对外区分原因。"""

    code: str = "INTERNAL"


class TokenConflictError(OrchestratorError):
    """同一客户端令牌被用于提交不同内容。"""

    code = "TOKEN_CONFLICT"


class QueueFullError(OrchestratorError):
    """排队容量已满，拒绝接收新任务。"""

    code = "QUEUE_FULL"


class TaskNotFoundError(OrchestratorError):
    """任务不存在。"""

    code = "TASK_NOT_FOUND"


class InvalidStateError(OrchestratorError):
    """当前状态不允许该操作（如取消已结束任务）。"""

    code = "INVALID_STATE"
