"""交付包自检。

解压 zip 之后先跑这个，能一次性回答：
- 文件齐不齐
- 是不是真的零第三方依赖
- 许可证与归属声明在不在
- 有没有把缓存/运行产物打进包里
- 测试能不能过

用法：
    python tools/verify_package.py              # 只做静态检查
    python tools/verify_package.py --with-tests # 额外跑一遍测试
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent

# Windows 控制台与 CI runner 的默认编码常是 cp1252 / GBK，打印中文会抛
# UnicodeEncodeError。所有面向人的输出统一走 UTF-8。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

REQUIRED = [
    "README.md",
    "LICENSE",
    "NOTICE",
    "CHANGELOG.md",
    "ANALYSIS.md",
    "pyproject.toml",
    "start.cmd",
    "miniflow/__init__.py",
    "miniflow/cli.py",
    "miniflow/cron.py",
    "miniflow/dag.py",
    "miniflow/errors.py",
    "miniflow/loader.py",
    "miniflow/runner.py",
    "miniflow/store.py",
    "miniflow/_worker.py",
    "miniflow/templates.py",
    "tests/test_cron.py",
    "tests/test_dag.py",
    "tests/test_runner.py",
    "tests/test_store.py",
    "tests/test_cli.py",
    "docs/DEPLOY.md",
    "docs/TROUBLESHOOTING.md",
    "docs/MIGRATION-FROM-AIRFLOW.md",
    "docs/FEATURE-MATRIX.md",
    "docs/assets/cover.png",
    ".github/workflows/ci.yml",
    "examples/dags/etl_dag.py",
    "examples/data/sales.csv",
    "tools/mutation_check.py",
    "tools/verify_package.py",
    "tools/build_zip.py",
    "tools/make_cover.py",
]

STDLIB_ONLY_MARKERS = (
    "dependencies = []",
)


def check_required() -> Tuple[bool, str]:
    missing = [name for name in REQUIRED if not (ROOT / name).exists()]
    if missing:
        return False, "缺少 %d 个文件：%s" % (len(missing), ", ".join(missing))
    return True, "%d 个必需文件齐全" % len(REQUIRED)


def check_zero_deps() -> Tuple[bool, str]:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    if not any(marker in text for marker in STDLIB_ONLY_MARKERS):
        return False, "pyproject.toml 里没有找到 `dependencies = []`"
    block = re.search(r"dependencies\s*=\s*(\[[^\]]*\])", text, re.S)
    if block and block.group(1).strip() != "[]":
        return False, "dependencies 不是空的：%s" % block.group(1)
    return True, "dependencies = []（零第三方运行时依赖）"


def check_license() -> Tuple[bool, str]:
    text = (ROOT / "LICENSE").read_text(encoding="utf-8")
    if "Apache License" not in text or "Version 2.0" not in text:
        return False, "LICENSE 不是 Apache-2.0 全文"
    if "Copyright 2026 Vespera" not in text:
        return False, "LICENSE 里缺少本项目的版权行"
    if "APACHE AIRFLOW SUBCOMPONENTS" in text:
        return False, "LICENSE 混入了上游的 subcomponents 声明"
    return True, "Apache-2.0 全文 + 本项目版权行（%d 字节）" % len(text.encode("utf-8"))


def check_notice() -> Tuple[bool, str]:
    text = (ROOT / "NOTICE").read_text(encoding="utf-8")
    needed = ["Apache Airflow", "Apache Software Foundation", "no source code copied"]
    missing = [token for token in needed if token.lower() not in text.lower()]
    if missing:
        return False, "NOTICE 缺少归属要素：%s" % ", ".join(missing)
    return True, "NOTICE 含上游归属与「未复制源码」声明"


def check_clean() -> Tuple[bool, str]:
    """只查不会被自动清理、也不该出现在交付物里的运行产物。

    `__pycache__` / `*.pyc` 不算问题：跑一次测试就会重新生成，
    而且 tools/build_zip.py 明确排除它们。真正要盯的是
    `.miniflow`（状态库与日志）和 `*.egg-info`（构建残留）。
    """
    junk: List[str] = []
    for pattern in ("**/.miniflow", "**/*.egg-info", "dist", "build", "examples/output"):
        junk.extend(str(p.relative_to(ROOT)) for p in ROOT.glob(pattern))
    if junk:
        return False, "包里有运行/构建产物：%s" % ", ".join(sorted(junk)[:5])
    return True, "没有 .miniflow / egg-info / dist 等产物（Python 缓存由打包脚本排除）"


def check_text_encoding() -> Tuple[bool, str]:
    bad = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix.lower() in (".zip", ".png", ".jpg", ".db"):
            continue
        if path.read_bytes()[:3] == b"\xef\xbb\xbf":
            bad.append(str(path.relative_to(ROOT)))
    if bad:
        return False, "带 BOM 的文件：%s" % ", ".join(bad)
    return True, "所有文本文件无 BOM"


def check_start_cmd() -> Tuple[bool, str]:
    path = ROOT / "start.cmd"
    raw = path.read_bytes()
    if b"\r\n" not in raw:
        return False, "start.cmd 不是 CRLF 换行，cmd.exe 可能解析异常"
    if raw[:3] == b"\xef\xbb\xbf":
        return False, "start.cmd 带 BOM"
    return True, "start.cmd 为 CRLF、无 BOM（%d 字节）" % len(raw)


def check_version_consistency() -> Tuple[bool, str]:
    init_text = (ROOT / "miniflow" / "__init__.py").read_text(encoding="utf-8")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    v1 = re.search(r'__version__\s*=\s*"([^"]+)"', init_text)
    v2 = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.M)
    if not v1 or not v2:
        return False, "找不到版本号"
    if v1.group(1) != v2.group(1):
        return False, "版本不一致：__init__.py=%s, pyproject=%s" % (v1.group(1), v2.group(1))
    return True, "版本号一致：%s" % v1.group(1)


def run_tests() -> Tuple[bool, str]:
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    tail = result.stderr.strip().splitlines()
    summary = tail[-3:] if len(tail) >= 3 else tail
    if result.returncode != 0:
        return False, "测试失败：%s" % " | ".join(summary)
    return True, " | ".join(summary)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="miniflow 交付包自检")
    parser.add_argument("--with-tests", action="store_true", help="额外跑一遍 unittest")
    args = parser.parse_args(argv)

    checks = [
        ("文件完整性", check_required),
        ("零依赖", check_zero_deps),
        ("许可证", check_license),
        ("归属声明", check_notice),
        ("无缓存产物", check_clean),
        ("编码", check_text_encoding),
        ("启动脚本", check_start_cmd),
        ("版本一致", check_version_consistency),
    ]
    if args.with_tests:
        checks.append(("测试", run_tests))

    print("miniflow 交付包自检  (%s)" % ROOT)
    print()
    failures = 0
    for name, func in checks:
        try:
            ok, detail = func()
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, "检查本身出错：%s" % exc
        print("  %-4s %-10s %s" % ("OK" if ok else "FAIL", name, detail))
        if not ok:
            failures += 1

    print()
    if failures:
        print("%d 项未通过。" % failures)
        return 1
    print("全部通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
