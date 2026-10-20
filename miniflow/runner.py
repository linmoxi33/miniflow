"""执行器：把 DAG 跑起来。

一个 DAG 就是一条拓扑序，逐个任务丢进子进程。
失败的下游标 upstream_failed，无关分支继续跑（和 Airflow 默认行为一致）。
"""

from __future__ import annotations

import inspect
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .dag import DAG, Task
from .errors import MiniflowError
from .store import Store, now_iso

STATE_SUCCESS = "success"
STATE_FAILED = "failed"
STATE_UPSTREAM_FAILED = "upstream_failed"
STATE_RUNNING = "running"
STATE_SKIPPED = "skipped"

# 交互终端里用 \r 覆盖同一行；重定向到文件时不用，否则日志里全是回车
_IS_TTY = False
try:
    _IS_TTY = bool(sys.stdout.isatty())
except Exception:
    _IS_TTY = False


@dataclass
class TaskResult:
    task_id: str
    state: str
    attempts: int = 0
    duration_ms: int = 0
    log_path: Optional[str] = None
    error: Optional[str] = None


@dataclass
class RunResult:
    run_id: str
    dag_id: str
    state: str
    started_at: datetime
    finished_at: datetime
    logical_date: Optional[datetime] = None
    tasks: List[TaskResult] = field(default_factory=list)
    outputs: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at).total_seconds() * 1000)

    @property
    def ok(self) -> bool:
        return self.state == STATE_SUCCESS

    def counts(self) -> Dict[str, int]:
        out = {STATE_SUCCESS: 0, STATE_FAILED: 0, STATE_UPSTREAM_FAILED: 0}
        for task in self.tasks:
            out[task.state] = out.get(task.state, 0) + 1
        return out


def _fmt_ms(ms: int) -> str:
    if ms < 1000:
        return "%dms" % ms
    if ms < 60000:
        return "%.1fs" % (ms / 1000.0)
    return "%dm%02ds" % (ms // 60000, (ms % 60000) // 1000)


def _begin_task(task_id: str) -> None:
    if _IS_TTY:
        print("  RUN   %-24s ..." % task_id, end="", flush=True)


def _end_task(state: str, task_id: str, detail: str, log_path: Optional[str] = None) -> None:
    lead = "\r" if _IS_TTY else ""
    print("%s  %-5s %-24s %s" % (lead, state, task_id, detail))
    if log_path and state == "FAIL":
        print("        日志 %s" % log_path)


def make_run_id(dag_id: str, logical_date: Optional[datetime] = None) -> str:
    stamp = (logical_date or datetime.now()).strftime("%Y%m%dT%H%M%S")
    return "%s__%s" % (dag_id, stamp)


def _unique_run_id(store: Store, dag_id: str, logical_date: Optional[datetime]) -> str:
    base = make_run_id(dag_id, logical_date)
    candidate = base
    suffix = 2
    while store.get_run(candidate) is not None:
        candidate = "%s-%d" % (base, suffix)
        suffix += 1
    return candidate


# ---------------------------------------------------------------- 参数绑定
def bind_params(task: Task, outputs: Dict[str, Any], runtime_params: Dict[str, Any]) -> Dict[str, Any]:
    sig = inspect.signature(task.fn)
    kwargs: Dict[str, Any] = {}

    for name, param in sig.parameters.items():
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        if name in outputs:
            kwargs[name] = outputs[name]
        elif name in runtime_params:
            kwargs[name] = runtime_params[name]
        elif param.default is not inspect.Parameter.empty:
            continue
        else:
            # 最像新手会踩的坑：参数名写对了，但忘了写 >>
            if name in task.dag.tasks:
                raise MiniflowError(
                    "MF005",
                    "任务 %s 的参数 %r 是同一个 DAG 里的任务，但它不是 %s 的上游"
                    % (task.task_id, name, task.task_id),
                    reason="依赖没连上，miniflow 不知道要不要把它的返回值传过来。",
                    hint="补一行依赖：%s >> %s。" % (name, task.task_id),
                    next_cmd="miniflow show %s" % task.dag.dag_id,
                )
            if not task.upstream:
                raise MiniflowError(
                    "MF005",
                    "任务 %s 需要运行时参数 %r，但没提供" % (task.task_id, name),
                    reason="这个任务没有上游，参数只能从命令行传进来。",
                    hint="运行时补上：--param %s=值（可重复多次）。" % name,
                    next_cmd="miniflow run %s --param %s=VALUE"
                    % (task.dag.dag_id, name),
                )
            raise MiniflowError(
                "MF005",
                "任务 %s 的参数 %r 绑不到上游输出" % (task.task_id, name),
                reason="上游只有：%s。" % ", ".join(sorted(task.upstream)),
                hint="把参数名改成某个上游 task_id，或给它一个默认值。",
                next_cmd="miniflow show %s" % task.dag.dag_id,
            )
    return kwargs


# ---------------------------------------------------------------- 子进程
def _spawn(
    spec: Dict[str, Any],
    spec_path: Path,
    result_path: Path,
    log_path: Path,
    timeout: Optional[float],
) -> Dict[str, Any]:
    spec_path.write_text(json.dumps(spec, ensure_ascii=False), encoding="utf-8")

    package_root = str(Path(__file__).resolve().parent.parent)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    env["MINIFLOW_WORKER"] = "1"
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = package_root + (os.pathsep + existing if existing else "")

    cmd = [sys.executable, "-m", "miniflow._worker", str(spec_path)]
    timed_out = False
    started = time.time()

    with open(log_path, "w", encoding="utf-8") as log_handle:
        log_handle.write(
            "=== miniflow worker ===\n"
            "dag=%s task=%s attempt=%s\ncwd=%s\npython=%s\n\n"
            % (spec["dag_id"], spec["task_id"], spec.get("attempt"), os.getcwd(), sys.executable)
        )
        log_handle.flush()

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
            )
        except OSError as exc:
            log_handle.write("[spawn failed] %s\n" % exc)
            return {
                "ok": False,
                "error_code": "MF008",
                "error_type": "SpawnError",
                "message": "起不来子进程：%s" % exc,
                "traceback": "",
                "duration_ms": 0,
            }

        def pump() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:
                log_handle.write(line)
                log_handle.flush()

        reader = threading.Thread(target=pump, daemon=True)
        reader.start()

        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass

        reader.join(timeout=5)
        if proc.stdout is not None:
            try:
                proc.stdout.close()
            except Exception:
                pass
        duration_ms = int((time.time() - started) * 1000)

        if timed_out:
            log_handle.write("\n[timeout] 超过 %ss，已强制终止\n" % timeout)
            return {
                "ok": False,
                "error_code": "MF007",
                "error_type": "Timeout",
                "message": "任务执行超过 %ss 被终止" % timeout,
                "traceback": "",
                "duration_ms": duration_ms,
                "timed_out": True,
            }

        log_handle.write("\n=== exit_code=%s duration=%sms ===\n" % (proc.returncode, duration_ms))

    if not result_path.exists():
        return {
            "ok": False,
            "error_code": "MF008",
            "error_type": "NoResult",
            "message": "worker 没有写回结果（exit_code=%s），常见原因是进程被系统杀掉或调用了 os._exit"
            % proc.returncode,
            "traceback": "",
            "duration_ms": duration_ms,
        }

    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {
            "ok": False,
            "error_code": "MF008",
            "error_type": "BadResult",
            "message": "结果文件读不出来：%s" % exc,
            "traceback": "",
            "duration_ms": duration_ms,
        }

    payload["duration_ms"] = duration_ms
    return payload


# ---------------------------------------------------------------- 主流程
def run_dag(
    dag: DAG,
    store: Store,
    home: Path,
    params: Optional[Dict[str, Any]] = None,
    only_task: Optional[str] = None,
    logical_date: Optional[datetime] = None,
    verbose: bool = False,
    echo: bool = True,
) -> RunResult:
    dag.validate()
    params = dict(params or {})

    if only_task:
        order = dag.ancestors(only_task)
    else:
        order = dag.topological_order()

    run_id = _unique_run_id(store, dag.dag_id, logical_date)
    started = datetime.now()
    store.start_run(run_id, dag.dag_id, logical_date, params)

    outputs: Dict[str, Any] = {}
    blocked: set = set()
    results: List[TaskResult] = []

    for task_id in order:
        task = dag.tasks[task_id]

        if any(up in blocked for up in task.upstream):
            blocked.add(task_id)
            results.append(TaskResult(task_id=task_id, state=STATE_UPSTREAM_FAILED))
            store.record_task(run_id, task_id, 0, STATE_UPSTREAM_FAILED, error="上游失败，未执行")
            if echo:
                _end_task("SKIP", task_id, "上游失败，未执行")
            continue

        try:
            kwargs = bind_params(task, outputs, params)
        except MiniflowError as exc:
            blocked.add(task_id)
            results.append(
                TaskResult(task_id=task_id, state=STATE_FAILED, error=exc.message)
            )
            store.record_task(run_id, task_id, 0, STATE_FAILED, error=exc.message)
            if echo:
                _end_task("FAIL", task_id, exc.message)
            continue

        if echo:
            _begin_task(task_id)

        result = _run_task_with_retries(
            task, kwargs, run_id, home, store, verbose=verbose
        )
        results.append(result)

        if result.state == STATE_SUCCESS:
            outputs[task_id] = result.value  # type: ignore[attr-defined]
            if echo:
                _end_task("OK", task_id, _fmt_ms(result.duration_ms))
        else:
            blocked.add(task_id)
            if echo:
                _end_task("FAIL", task_id, result.error or "", log_path=result.log_path)

    finished = datetime.now()
    counts = {STATE_SUCCESS: 0, STATE_FAILED: 0, STATE_UPSTREAM_FAILED: 0}
    for item in results:
        counts[item.state] = counts.get(item.state, 0) + 1

    state = STATE_SUCCESS if counts[STATE_FAILED] == 0 else STATE_FAILED
    store.finish_run(run_id, state)

    return RunResult(
        run_id=run_id,
        dag_id=dag.dag_id,
        state=state,
        started_at=started,
        finished_at=finished,
        logical_date=logical_date,
        tasks=results,
        outputs=outputs,
    )


def _run_task_with_retries(
    task: Task,
    kwargs: Dict[str, Any],
    run_id: str,
    home: Path,
    store: Store,
    verbose: bool = False,
) -> "TaskResultWithValue":
    log_dir = home / "logs" / run_id / task.task_id
    log_dir.mkdir(parents=True, exist_ok=True)

    attempts_allowed = int(task.retries) + 1
    last_error: Optional[str] = None
    last_log: Optional[str] = None
    total_ms = 0

    for attempt in range(1, attempts_allowed + 1):
        log_path = log_dir / ("attempt-%d.log" % attempt)
        spec_path = log_dir / ("attempt-%d.spec.json" % attempt)
        result_path = log_dir / ("attempt-%d.result.json" % attempt)

        spec = {
            "dag_file": task.dag.source_file,
            "dag_id": task.dag.dag_id,
            "task_id": task.task_id,
            "attempt": attempt,
            "params": kwargs,
            "result_path": str(result_path),
        }

        store.record_task(run_id, task.task_id, attempt, STATE_RUNNING, log_path=str(log_path))
        payload = _spawn(spec, spec_path, result_path, log_path, task.timeout)
        total_ms += int(payload.get("duration_ms") or 0)
        last_log = str(log_path)

        if payload.get("ok"):
            store.record_task(
                run_id,
                task.task_id,
                attempt,
                STATE_SUCCESS,
                exit_code=0,
                log_path=str(log_path),
            )
            return TaskResultWithValue(
                task_id=task.task_id,
                state=STATE_SUCCESS,
                attempts=attempt,
                duration_ms=total_ms,
                log_path=str(log_path),
                value=json.loads(payload.get("return_json") or "null"),
            )

        code = payload.get("error_code") or "MF008"
        error_text = payload.get("message") or "未知错误"
        last_error = "%s %s" % (code, error_text)
        store.record_task(
            run_id,
            task.task_id,
            attempt,
            STATE_FAILED,
            exit_code=payload.get("exit_code"),
            log_path=str(log_path),
            error=last_error,
        )

        if attempt < attempts_allowed:
            if verbose:
                lead = "\r" if _IS_TTY else "  "
                print(
                    "%sRETRY %-24s attempt %d/%d 失败：%s"
                    % (lead, task.task_id, attempt, attempts_allowed, last_error),
                    flush=True,
                )
            if task.retry_delay:
                time.sleep(task.retry_delay)

    return TaskResultWithValue(
        task_id=task.task_id,
        state=STATE_FAILED,
        attempts=attempts_allowed,
        duration_ms=total_ms,
        log_path=last_log,
        error=last_error,
    )


@dataclass
class TaskResultWithValue(TaskResult):
    value: Any = None
