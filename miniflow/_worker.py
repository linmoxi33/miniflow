"""子进程入口：在独立进程里跑一个任务函数。

为什么要独立进程：
1. 任务崩溃（segfault、os._exit、死循环）不会拖垮调度器；
2. 超时能真的杀掉，而不是留一个僵尸线程；
3. 任务里的全局状态不会污染下一次运行。

这个模块只读 spec.json、写 result.json，日志直接打到 stdout。
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

# 让子进程的输出在任何终端/管道下都是 UTF-8（Windows 默认 GBK 会炸中文）
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass


def _write_atomic(path: Path, payload: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)
    os.replace(tmp, path)


def _load_dag(dag_file: Path):
    """把 DAG 文件再 import 一次，拿到里面的 DAG 对象。"""
    from miniflow.dag import reset_defined_dags, take_defined_dags

    parent = str(dag_file.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    reset_defined_dags()
    module_name = "miniflow_worker_dag_%s" % dag_file.stem
    spec = importlib.util.spec_from_file_location(module_name, str(dag_file))
    if spec is None or spec.loader is None:
        raise ImportError("无法加载 %s" % dag_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return take_defined_dags()


def run_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    started = time.time()
    params = spec.get("params") or {}
    dag_file = Path(spec["dag_file"])

    try:
        dags = _load_dag(dag_file)
    except BaseException as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_code": "MF002",
            "error_type": type(exc).__name__,
            "message": "子进程里加载 DAG 文件失败：%s" % exc,
            "traceback": traceback.format_exc(),
        }

    target = None
    for dag in dags:
        if dag.dag_id == spec["dag_id"]:
            target = dag
            break
    if target is None:
        return {
            "ok": False,
            "error_code": "MF003",
            "error_type": "LookupError",
            "message": "DAG %r 在文件里不存在（子进程重新加载时消失了）"
            % spec["dag_id"],
            "traceback": "",
        }

    try:
        task_obj = target.get_task(spec["task_id"])
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error_code": "MF003",
            "error_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }

    try:
        value = task_obj.fn(**params)
    except BaseException as exc:  # noqa: BLE001 - 包括 SystemExit
        return {
            "ok": False,
            "error_code": "MF008",
            "error_type": type(exc).__name__,
            "message": "%s: %s" % (type(exc).__name__, exc),
            "traceback": traceback.format_exc(),
        }

    try:
        return_json = json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        return {
            "ok": False,
            "error_code": "MF006",
            "error_type": type(value).__name__,
            "message": "任务返回值是 %s，不能转成 JSON：%s"
            % (type(value).__name__, exc),
            "traceback": "",
        }

    return {
        "ok": True,
        "return_json": return_json,
        "duration_ms": int((time.time() - started) * 1000),
    }


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 1:
        print("usage: python -m miniflow._worker <spec.json>", file=sys.stderr)
        return 2

    spec_path = Path(argv[0])
    try:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
    except OSError as exc:
        print("读不到 spec 文件 %s: %s" % (spec_path, exc), file=sys.stderr)
        return 2

    result_path = Path(spec["result_path"])
    try:
        payload = run_spec(spec)
    except BaseException:  # noqa: BLE001 - 兜底，保证一定有 result.json
        payload = {
            "ok": False,
            "error_code": "MF008",
            "error_type": "WorkerCrash",
            "message": "worker 进程自身异常退出",
            "traceback": traceback.format_exc(),
        }

    _write_atomic(result_path, payload)

    if not payload.get("ok"):
        # 把错误也打到 stdout，父进程会一并收进日志文件
        print(payload.get("traceback") or payload.get("message") or "", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
