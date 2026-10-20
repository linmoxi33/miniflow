"""DAG / Task 模型：定义、依赖、校验。

刻意保持小：一个 DAG 就是一个有向无环图 + 一点调度元数据。
没有 operator 继承树，没有 XCom 后端，没有插件系统。
"""

from __future__ import annotations

import contextvars
import functools
import inspect
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

from .errors import MiniflowError

# 当前正在定义的 DAG。用 contextvar 而不是全局变量，
# 这样在函数里嵌套定义 DAG 也不会互相污染。
_current_dag: "contextvars.ContextVar[Optional[DAG]]" = contextvars.ContextVar(
    "miniflow_current_dag", default=None
)

# 一次 import 出来的所有 DAG。loader 用 take_defined_dags() 取走，
# 这样一个文件里写多个 with DAG(...) 也能全部被发现。
_defined_dags: List["DAG"] = []


def take_defined_dags() -> List["DAG"]:
    """取走并清空本次 import 产生的 DAG 列表。"""
    global _defined_dags
    out = _defined_dags
    _defined_dags = []
    return out


def reset_defined_dags() -> None:
    global _defined_dags
    _defined_dags = []

TASK_STATE_ORDER = (
    "success",
    "failed",
    "upstream_failed",
    "skipped",
    "running",
    "queued",
)


def get_current_dag() -> "DAG":
    dag = _current_dag.get()
    if dag is None:
        raise MiniflowError(
            "MF013",
            "@task 必须写在 with DAG(...) 代码块里面",
            reason="装饰器要靠当前 DAG 才知道把任务注册到哪个图。",
            hint="把 @task 函数缩进到 with DAG(\"名字\", schedule=...): 内部，或用 dag.task 显式指定。",
            next_cmd="miniflow init  # 生成一份可直接跑的示例",
        )
    return dag


class Task:
    """一个任务 = 一个普通 Python 函数 + 执行策略。"""

    def __init__(
        self,
        fn: Callable[..., Any],
        dag: "DAG",
        task_id: Optional[str] = None,
        retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.fn = fn
        self.dag = dag
        self.task_id = task_id or fn.__name__
        self.retries = dag.retries if retries is None else retries
        self.retry_delay = dag.retry_delay if retry_delay is None else retry_delay
        self.timeout = dag.timeout if timeout is None else timeout
        self.upstream: Set[str] = set()

        _validate_task_id(self.task_id)

        # 让 Task 在大多数场景下表现得像原函数
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__
        self.__wrapped__ = fn
        functools.update_wrapper(self, fn)

    # -------------------------------------------------------------- 依赖
    def __rshift__(self, other: "Task") -> "Task":
        if not isinstance(other, Task):
            raise MiniflowError(
                "MF013",
                ">> 右边必须是另一个 @task 任务，现在是 %s" % type(other).__name__,
                hint="写成 upstream >> downstream，两边都是 @task 装饰过的函数。",
                next_cmd="miniflow show <dag_id>",
            )
        if other.dag is not self.dag:
            raise MiniflowError(
                "MF013",
                "不能跨 DAG 连依赖：%s 属于 %s，%s 属于 %s"
                % (self.task_id, self.dag.dag_id, other.task_id, other.dag.dag_id),
                hint="一个 DAG 内部的任务不能直接连到另一个 DAG 的任务。",
            )
        other.upstream.add(self.task_id)
        return other

    def __call__(self, *args: Any, **kwargs: Any) -> "Task":
        # 允许写 extract() >> transform()，但不接受真的传参：
        # 参数是靠「参数名 = 上游 task_id」在运行时绑定的。
        if args or kwargs:
            raise MiniflowError(
                "MF013",
                "任务 %s 被调用时传了参数" % self.task_id,
                reason="miniflow 的参数由上游返回值按名字自动注入，不通过调用传。",
                hint="把依赖写成 a >> b >> c；下游函数签名里用上游的 task_id 当参数名。",
                next_cmd="miniflow show %s" % self.dag.dag_id,
            )
        return self

    # -------------------------------------------------------------- 描述
    def required_params(self) -> List[str]:
        """必须由外部（--param）提供的参数。根任务才可能有。"""
        out = []
        for name, param in inspect.signature(self.fn).parameters.items():
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            if param.default is not inspect.Parameter.empty:
                continue
            if name not in self.upstream:
                out.append(name)
        return out

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "upstream": sorted(self.upstream),
            "retries": self.retries,
            "retry_delay": self.retry_delay,
            "timeout": self.timeout,
            "doc": (self.__doc__ or "").strip().splitlines()[0] if self.__doc__ else "",
            "runtime_params": self.required_params(),
        }

    def __repr__(self) -> str:
        return "<Task %s>" % self.task_id


class DAG:
    """调度单元。用法与 Airflow 神似，但只有它最常用的那几个参数。"""

    def __init__(
        self,
        dag_id: str,
        schedule: Optional[str] = None,
        start_date: Optional[Any] = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        timeout: Optional[float] = None,
        description: str = "",
        tags: Optional[Sequence[str]] = None,
        catchup: bool = False,
        owner: str = "",
    ) -> None:
        _validate_dag_id(dag_id)
        if retries < 0:
            raise MiniflowError("MF013", "retries 不能是负数：%s" % retries)
        if retry_delay < 0:
            raise MiniflowError("MF013", "retry_delay 不能是负数：%s" % retry_delay)
        if timeout is not None and timeout <= 0:
            raise MiniflowError("MF013", "timeout 必须大于 0 秒：%s" % timeout)

        self.dag_id = dag_id
        self.schedule = schedule
        self.retries = int(retries)
        self.retry_delay = float(retry_delay)
        self.timeout = None if timeout is None else float(timeout)
        self.description = description
        self.tags = list(tags or [])
        self.catchup = bool(catchup)
        self.owner = owner
        self.start_date = _coerce_datetime(start_date)
        self.tasks: Dict[str, Task] = {}
        self.source_file: Optional[str] = None

        self._cron = None
        self._token = None
        self._closed = False

    # ------------------------------------------------------- 上下文管理
    def __enter__(self) -> "DAG":
        if _current_dag.get() is not None:
            raise MiniflowError(
                "MF013",
                "with DAG(...) 不能嵌套",
                reason="同一时刻只能有一个正在定义的 DAG；当前是 %s。" % _current_dag.get().dag_id,
                hint="把两个 DAG 写成两个独立的 with 块，或拆到两个文件。",
                next_cmd="miniflow list",
            )
        self._token = _current_dag.set(self)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            _current_dag.reset(self._token)
        except (LookupError, ValueError):
            pass
        self._closed = True
        if exc_type is None:
            self.validate()
            _defined_dags.append(self)
        return False

    # ------------------------------------------------------------ 注册
    def add_task(self, task: Task) -> Task:
        if task.task_id in self.tasks:
            existing = self.tasks[task.task_id]
            raise MiniflowError(
                "MF013",
                "DAG %s 里有两个同名任务：%s" % (self.dag_id, task.task_id),
                reason="两个函数用了同一个名字，或 @task(task_id=...) 重复了。",
                hint="给其中一个加 @task(task_id=\"另一个名字\")；现有函数是 %s"
                % getattr(existing.fn, "__qualname__", existing.task_id),
                next_cmd="miniflow show %s" % self.dag_id,
            )
        self.tasks[task.task_id] = task
        return task

    def get_task(self, task_id: str) -> Task:
        if task_id not in self.tasks:
            raise MiniflowError(
                "MF003",
                "DAG %s 里没有任务 %r" % (self.dag_id, task_id),
                hint="现有任务：%s" % (", ".join(self.tasks) or "（空）"),
                next_cmd="miniflow show %s" % self.dag_id,
            )
        return self.tasks[task_id]

    # ------------------------------------------------------------ 校验
    def validate(self) -> None:
        if not self.tasks:
            raise MiniflowError(
                "MF013",
                "DAG %s 里一个任务都没有" % self.dag_id,
                reason="with 块里没有 @task 装饰的函数。",
                hint="在 with DAG(...) 里定义至少一个 @task 函数。",
                next_cmd="miniflow init  # 看一份能跑的最小示例",
            )

        for task in self.tasks.values():
            for up in task.upstream:
                if up not in self.tasks:
                    raise MiniflowError(
                        "MF013",
                        "任务 %s 依赖了不存在的任务 %r" % (task.task_id, up),
                        hint="检查 >> 两边是不是同一个 DAG 里的任务。",
                        next_cmd="miniflow show %s" % self.dag_id,
                    )

        cycle = self.find_cycle()
        if cycle:
            raise MiniflowError(
                "MF004",
                "依赖关系成环：%s" % " -> ".join(cycle),
                reason="环上的任务互相等待，永远不会有任务先开始。",
                hint="删掉环上任意一条 >> 依赖边，或把共用逻辑抽成独立的上游任务。",
                next_cmd="miniflow show %s" % self.dag_id,
            )

        self._validate_bindings()

        if self.schedule is not None:
            from .cron import parse_cron

            self._cron = parse_cron(self.schedule)

    def _validate_bindings(self) -> None:
        for task in self.tasks.values():
            sig = inspect.signature(task.fn)
            for name, param in sig.parameters.items():
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                if name in task.upstream:
                    continue
                if param.default is not inspect.Parameter.empty:
                    continue
                # 参数名正好是同一 DAG 里的另一个任务，但没连 >>。
                # 这是新手最容易踩的坑，必须在加载时就报出来。
                if name in self.tasks:
                    raise MiniflowError(
                        "MF005",
                        "任务 %s 的参数 %r 是同一个 DAG 里的任务，但它们之间没有依赖"
                        % (task.task_id, name),
                        reason="参数名看起来想接那个任务的输出，可是没有写 >>，miniflow 不会自动猜。",
                        hint="补一行依赖：%s >> %s。" % (name, task.task_id),
                        next_cmd="miniflow show %s" % self.dag_id,
                    )
                if not task.upstream:
                    # 根任务：允许从 --param 拿运行时参数
                    continue
                available = ", ".join(sorted(task.upstream))
                suggestion = ""
                if len(task.upstream) == 1:
                    suggestion = "；看起来你想接唯一的上游，把参数名改成 %r" % next(
                        iter(task.upstream)
                    )
                raise MiniflowError(
                    "MF005",
                    "任务 %s 的参数 %r 绑不到任何上游输出" % (task.task_id, name),
                    reason="miniflow 用「参数名 = 上游 task_id」来传数据，%r 既不是上游 id 也没有默认值。"
                    % name,
                    hint="可用参数名：%s%s。" % (available, suggestion),
                    next_cmd="miniflow show %s" % self.dag_id,
                )

    # ------------------------------------------------------------ 图算法
    def find_cycle(self) -> Optional[List[str]]:
        """返回一条环路径（首尾同名），没环则返回 None。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {tid: WHITE for tid in self.tasks}
        stack: List[str] = []

        def visit(tid: str) -> Optional[List[str]]:
            color[tid] = GRAY
            stack.append(tid)
            for up in sorted(self.tasks[tid].upstream):
                if color[up] == GRAY:
                    idx = stack.index(up)
                    # DFS 走的是 upstream 方向，展示时翻回 >> 的箭头方向，
                    # 否则用户看到的环和自己写的代码是反的。
                    raw = stack[idx:] + [up]
                    return [raw[0]] + list(reversed(raw[1:-1])) + [raw[0]]
                if color[up] == WHITE:
                    found = visit(up)
                    if found:
                        return found
            stack.pop()
            color[tid] = BLACK
            return None

        # 按定义顺序遍历，保证同一份 DAG 每次报同一个环
        for tid in self.tasks:
            if color[tid] == WHITE:
                found = visit(tid)
                if found:
                    return found
        return None

    def topological_order(self) -> List[str]:
        """Kahn 算法，同层按定义顺序，结果稳定可复现。"""
        indegree = {tid: len(t.upstream) for tid, t in self.tasks.items()}
        ready = [tid for tid in self.tasks if indegree[tid] == 0]
        order: List[str] = []

        while ready:
            tid = ready.pop(0)
            order.append(tid)
            for other_id, other in self.tasks.items():
                if tid in other.upstream:
                    indegree[other_id] -= 1
                    if indegree[other_id] == 0:
                        ready.append(other_id)
            ready.sort(key=lambda x: list(self.tasks).index(x))

        if len(order) != len(self.tasks):
            cycle = self.find_cycle()
            raise MiniflowError(
                "MF004",
                "依赖关系成环：%s" % " -> ".join(cycle or []),
                hint="删掉环上任意一条 >> 依赖边。",
                next_cmd="miniflow show %s" % self.dag_id,
            )
        return order

    def ancestors(self, task_id: str) -> List[str]:
        """返回执行 task_id 必须先跑完的任务（含自身），按拓扑序。"""
        self.get_task(task_id)
        needed: Set[str] = set()

        def walk(tid: str) -> None:
            if tid in needed:
                return
            needed.add(tid)
            for up in self.tasks[tid].upstream:
                walk(up)

        walk(task_id)
        return [tid for tid in self.topological_order() if tid in needed]

    def roots(self) -> List[str]:
        return [tid for tid, t in self.tasks.items() if not t.upstream]

    def leaves(self) -> List[str]:
        has_downstream = set()
        for task in self.tasks.values():
            has_downstream |= task.upstream
        return [tid for tid in self.tasks if tid not in has_downstream]

    # ------------------------------------------------------------ 描述
    def cron(self):
        if self.schedule is None:
            return None
        if self._cron is None:
            from .cron import parse_cron

            self._cron = parse_cron(self.schedule)
        return self._cron

    def schedule_text(self) -> str:
        if self.schedule is None:
            return "手动触发"
        return "%s (%s)" % (self.schedule, self.cron().describe())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "description": self.description,
            "schedule": self.schedule,
            "schedule_text": self.schedule_text(),
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "catchup": self.catchup,
            "tags": self.tags,
            "owner": self.owner,
            "source_file": self.source_file,
            "tasks": [self.tasks[tid].to_dict() for tid in self.topological_order()],
        }

    def ascii_graph(self) -> str:
        """把 DAG 画成一张能贴进 issue 的文本图。"""
        lines: List[str] = []
        seen: Set[str] = set()
        order = self.topological_order()
        downstream_map: Dict[str, List[str]] = {tid: [] for tid in self.tasks}
        for tid in order:
            for up in self.tasks[tid].upstream:
                downstream_map[up].append(tid)

        def render(tid: str, prefix: str, is_last: bool, is_root: bool) -> None:
            connector = "" if is_root else ("└─ " if is_last else "├─ ")
            task = self.tasks[tid]
            flags = []
            if task.retries:
                flags.append("retries=%d" % task.retries)
            if task.timeout:
                flags.append("timeout=%gs" % task.timeout)
            suffix = ("  [%s]" % ", ".join(flags)) if flags else ""
            repeat = "  (已出现，展开省略)" if tid in seen else ""
            lines.append("%s%s%s%s%s" % (prefix, connector, tid, suffix, repeat))
            if tid in seen:
                return
            seen.add(tid)

            downstream = downstream_map[tid]
            child_prefix = prefix + ("" if is_root else ("   " if is_last else "│  "))
            for index, child in enumerate(downstream):
                render(child, child_prefix, index == len(downstream) - 1, False)

        for root in self.roots():
            render(root, "", True, True)
        return "\n".join(lines)

    def __repr__(self) -> str:
        return "<DAG %s tasks=%d>" % (self.dag_id, len(self.tasks))


# ------------------------------------------------------------------ 装饰器
def task(
    fn: Optional[Callable[..., Any]] = None,
    *,
    task_id: Optional[str] = None,
    retries: Optional[int] = None,
    retry_delay: Optional[float] = None,
    timeout: Optional[float] = None,
):
    """把一个普通函数登记成当前 DAG 里的任务。

    用法：
        @task                      # 任务名 = 函数名
        @task(task_id="extract")   # 显式命名
        @task(retries=2, timeout=30)
    """

    def wrap(func: Callable[..., Any]) -> Task:
        dag = get_current_dag()
        return dag.add_task(
            Task(
                func,
                dag,
                task_id=task_id,
                retries=retries,
                retry_delay=retry_delay,
                timeout=timeout,
            )
        )

    if fn is None:
        return wrap
    return wrap(fn)


# ------------------------------------------------------------------ 校验工具
def _validate_dag_id(dag_id: str) -> None:
    if not dag_id or not str(dag_id).strip():
        raise MiniflowError("MF013", "dag_id 不能为空")
    text = str(dag_id)
    if text != text.strip():
        raise MiniflowError(
            "MF013", "dag_id 首尾不能有空格：%r" % text, hint="改成 %r。" % text.strip()
        )
    bad = [ch for ch in text if not (ch.isalnum() or ch in "_-.")]
    if bad:
        raise MiniflowError(
            "MF013",
            "dag_id %r 含有不支持的字符：%s" % (text, " ".join(sorted(set(bad)))),
            hint="dag_id 只允许字母、数字、下划线、短横线、点。",
        )


def _validate_task_id(task_id: str) -> None:
    if not task_id or not str(task_id).strip():
        raise MiniflowError("MF013", "task_id 不能为空")
    text = str(task_id)
    bad = [ch for ch in text if not (ch.isalnum() or ch in "_-.")]
    if bad:
        raise MiniflowError(
            "MF013",
            "task_id %r 含有不支持的字符：%s" % (text, " ".join(sorted(set(bad)))),
            hint="task_id 只允许字母、数字、下划线、短横线、点。",
        )


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        raise MiniflowError(
            "MF013",
            "start_date 无法解析：%r" % value,
            hint="用 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS。",
        )
    raise MiniflowError(
        "MF013",
        "start_date 只接受 datetime 或字符串，收到 %s" % type(value).__name__,
    )
