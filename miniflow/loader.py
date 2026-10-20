"""加载 DAG 文件。

DAG 就是普通的 .py 文件 —— 不需要特殊的目录结构，不需要注册表，
不需要重启任何服务。改完文件下一次 run 就生效。
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .dag import DAG, reset_defined_dags, take_defined_dags
from .errors import MiniflowError


def find_dag_files(dags_dir: Path) -> List[Path]:
    dags_dir = Path(dags_dir)
    if not dags_dir.exists():
        raise MiniflowError(
            "MF001",
            "DAG 目录不存在：%s" % dags_dir,
            hint="用 --dags 指定一个存在的目录，或先跑 miniflow init 生成示例。",
            next_cmd="miniflow init",
        )
    if not dags_dir.is_dir():
        raise MiniflowError(
            "MF001",
            "指定的 DAG 路径不是目录：%s" % dags_dir,
            hint="--dags 要指向存放 .py 文件的目录。",
            next_cmd="miniflow init",
        )

    files = sorted(
        path
        for path in dags_dir.glob("*.py")
        if not path.name.startswith("_") and path.is_file()
    )
    if not files:
        raise MiniflowError(
            "MF001",
            "DAG 目录里没有任何 .py 文件：%s" % dags_dir,
            reason="以 _ 开头的文件会被当作辅助模块跳过。",
            hint="放一个定义 DAG 的 .py 进去，或跑 miniflow init 生成示例。",
            next_cmd="miniflow init",
        )
    return files


def load_file(path: Path) -> List[DAG]:
    """import 一个 DAG 文件，返回它定义的所有 DAG。"""
    path = Path(path).resolve()
    parent = str(path.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    module_name = "miniflow_user_dag_%s_%s" % (
        path.stem,
        hashlib.sha1(str(path).encode("utf-8")).hexdigest()[:10],
    )

    reset_defined_dags()
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise MiniflowError(
            "MF002",
            "无法作为 Python 模块加载：%s" % path,
            hint="确认文件后缀是 .py 且内容不是二进制。",
            next_cmd="miniflow doctor",
        )

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except MiniflowError as exc:
        _append_source(exc, path)
        raise
    except SyntaxError as exc:
        raise MiniflowError(
            "MF002",
            "DAG 文件有语法错误：%s 第 %s 行" % (path.name, exc.lineno),
            reason=str(exc.msg),
            hint="看这一行前后的缩进和括号；Python 的缩进错误也常报成 SyntaxError。",
            next_cmd="python -m py_compile %s" % _quote(path),
            detail=_short_traceback(exc),
        )
    except ModuleNotFoundError as exc:
        missing = getattr(exc, "name", None) or str(exc)
        raise MiniflowError(
            "MF002",
            "DAG 文件 import 了一个没装的包：%s" % missing,
            reason="%s 在导入 %s 时失败。" % (path.name, missing),
            hint="miniflow 自身零依赖，但你的任务可以用自己的包：pip install %s。" % missing,
            next_cmd="pip install %s" % missing,
            detail=_short_traceback(exc),
        )
    except Exception as exc:  # noqa: BLE001 - 这里要兜住用户代码的一切异常
        raise MiniflowError(
            "MF002",
            "DAG 文件执行失败：%s" % path.name,
            reason="%s: %s" % (type(exc).__name__, exc),
            hint="DAG 文件在 import 时就会执行一遍，顶层不要放需要外部服务、网络或数据库的代码。",
            next_cmd="python %s" % _quote(path),
            detail=_short_traceback(exc),
        )
    finally:
        # 父进程不需要长期持有这个模块，避免同名文件互相覆盖
        sys.modules.pop(module_name, None)

    dags = take_defined_dags()
    if not dags:
        raise MiniflowError(
            "MF002",
            "%s 里没有定义任何 DAG" % path.name,
            reason="文件能 import，但没有走 with DAG(...) 代码块。",
            hint="参考 miniflow init 生成的示例，或确认 DAG 定义没有被 if __name__ 判断挡住。",
            next_cmd="miniflow init --force",
        )

    for dag in dags:
        dag.source_file = str(path)
    return dags


def load_all(dags_dir: Path) -> Dict[str, DAG]:
    """加载目录下全部 DAG，按 dag_id 建索引。"""
    files = find_dag_files(dags_dir)
    result: Dict[str, DAG] = {}
    errors: List[MiniflowError] = []

    for path in files:
        try:
            for dag in load_file(path):
                if dag.dag_id in result:
                    other = result[dag.dag_id]
                    raise MiniflowError(
                        "MF002",
                        "dag_id 重复：%s" % dag.dag_id,
                        reason="%s 和 %s 定义了同一个 dag_id。"
                        % (Path(other.source_file or "?").name, path.name),
                        hint="多个文件里的 DAG 名字必须唯一，调度器靠它区分。",
                        next_cmd="miniflow list",
                    )
                result[dag.dag_id] = dag
        except MiniflowError as exc:
            errors.append(exc)

    if errors:
        # 只有一个文件失败时，直接把原始错误抛出去：
        # 用户能看到最具体的「怎么修」，而不是一条笼统的摘要。
        if len(errors) == 1 and not result:
            raise errors[0]
        # 多个文件失败（或部分成功）时必须汇总，避免「静默少了几个 DAG」。
        raise _CombinedLoadError(errors, result)

    return result


class _CombinedLoadError(MiniflowError):
    def __init__(self, errors: Sequence[MiniflowError], loaded: Dict[str, DAG]) -> None:
        self.errors = list(errors)
        self.loaded = dict(loaded)
        first = self.errors[0]
        hints = []
        for err in self.errors[:3]:
            if err.hint:
                hints.append("[%s] %s" % (err.code, err.hint))
        super().__init__(
            first.code,
            "%d 个 DAG 文件加载失败（已成功加载 %d 个）" % (len(self.errors), len(loaded)),
            reason="第一个失败：%s" % first.message,
            hint="\n".join(hints) if hints else "单独跑一次那个文件看完整报错。",
            next_cmd="miniflow doctor",
            detail="\n".join(
                "===== %s %s =====\n%s" % (e.code, e.spec.title, e.render(verbose=True, color=False))
                for e in self.errors
            ),
        )


def _short_traceback(exc: BaseException, limit: int = 6) -> str:
    lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    text = "".join(lines).rstrip()
    # Python 3.11+ 的 traceback 会带一长串 ~~~~^^^ 指示行，
    # 摘到错误信息里全是噪音，过滤掉。
    kept = [
        line
        for line in text.splitlines()
        if line.strip() and not set(line.strip()) <= set("~^ ")
    ]
    return "\n".join(kept[-limit:])


def _append_source(exc: MiniflowError, path: Path) -> None:
    note = "来自文件: %s" % path
    exc.detail = (exc.detail + "\n" + note) if exc.detail else note
    # 结构类错误的原 next_cmd（miniflow show）会再次踩到同一个错误，
    # 换成体检命令更实际。
    if exc.code in ("MF004", "MF005", "MF009", "MF013"):
        exc.next_cmd = "miniflow doctor"
    elif exc.next_cmd is None:
        exc.next_cmd = "miniflow doctor"


def _quote(path: Path) -> str:
    text = str(path)
    return '"%s"' % text if " " in text else text
