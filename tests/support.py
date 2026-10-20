"""测试公共设施：临时工程、DAG 生成、CLI 调用。

零第三方依赖：只用标准库 unittest + tempfile + subprocess。
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Windows 控制台与 CI runner 的默认编码常是 cp1252 / GBK，
# 中文测试名和失败信息会直接抛 UnicodeEncodeError。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass


def write_dag(directory: Path, name: str, source: str) -> Path:
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    text = textwrap.dedent(source).lstrip("\n")
    # 不用 Path.write_text(newline=...)：那是 3.10+ 才有的参数，
    # 而 miniflow 声明支持 3.9。
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    return path


class TempProject:
    """一个带 dags/ 的临时工程，用完自动清理。"""

    def __init__(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="miniflow_test_")
        self.root = Path(self._tmp.name)
        self.dags = self.root / "dags"
        self.dags.mkdir()

    def dag(self, name: str, source: str) -> Path:
        return write_dag(self.dags, name, source)

    def cleanup(self) -> None:
        self._tmp.cleanup()

    def __enter__(self) -> "TempProject":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.cleanup()


def run_cli(*args: str, cwd: Optional[Path] = None, timeout: int = 180) -> subprocess.CompletedProcess:
    """以子进程方式跑真正的 CLI，验证端到端行为而不是内部函数。"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env.pop("MINIFLOW_HOME", None)
    env.pop("MINIFLOW_CONFIG", None)

    return subprocess.run(
        [sys.executable, "-m", "miniflow"] + list(args),
        cwd=str(cwd or PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
    )


def load_dags_from(project: TempProject) -> Any:
    from miniflow.loader import load_all

    return load_all(project.dags)


def count_files(path: Path, pattern: str = "*") -> List[Path]:
    return sorted(p for p in Path(path).glob(pattern) if p.is_file())
