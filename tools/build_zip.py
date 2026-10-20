"""打交付 ZIP。

排除规则只有三类：Python 缓存、运行时产物、版本控制/构建目录。
其余一律进包——交付包要能独立跑起来，不能依赖包外的任何东西。

用法：
    python tools/build_zip.py                       # 输出到 ../dist/miniflow-<version>.zip
    python tools/build_zip.py --output D:/out.zip
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parent.parent

EXCLUDE_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".miniflow",
    ".smoke",
    ".smoke2",
    ".smoke3",
    ".demo-test",
    "dist",
    "build",
    ".pytest_cache",
    ".venv",
}

# 只排除「目录名恰好是这些」的情况：examples/output 是运行产物，但
# 代码里可能有别的 output 目录，所以按相对路径精确匹配。
EXCLUDE_RELATIVE = {
    "examples/output",
}


def read_version() -> str:
    text = (ROOT / "miniflow" / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r'__version__\s*=\s*"([^"]+)"', text)
    if not match:
        raise SystemExit("找不到 __version__")
    return match.group(1)


def collect() -> List[Path]:
    files: List[Path] = []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        if any(part in EXCLUDE_DIR_NAMES for part in rel.parts[:-1]):
            continue
        if rel.parts and rel.parts[0] in EXCLUDE_DIR_NAMES:
            continue
        if str(rel).replace("\\", "/") in EXCLUDE_RELATIVE:
            continue
        if path.suffix.lower() in (".pyc", ".pyo"):
            continue
        files.append(path)
    return files


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="打 miniflow 交付 zip")
    parser.add_argument("--output", help="输出路径")
    args = parser.parse_args(argv)

    version = read_version()
    root_name = "miniflow-%s" % version
    output = Path(args.output) if args.output else (ROOT.parent / "dist" / ("%s.zip" % root_name))
    output.parent.mkdir(parents=True, exist_ok=True)

    files = collect()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in files:
            arcname = "%s/%s" % (root_name, path.relative_to(ROOT).as_posix())
            archive.write(path, arcname)

    size_kb = output.stat().st_size / 1024
    print("已生成 %s" % output)
    print("版本 %s  |  %d 个文件  |  %.1f KB" % (version, len(files), size_kb))

    # 立刻回读一遍，确认 zip 没坏
    with zipfile.ZipFile(output) as archive:
        bad = archive.testzip()
        count = len(archive.namelist())
    if bad:
        print("zip 自检失败，损坏条目：%s" % bad)
        return 1
    print("zip 自检通过（%d 个条目）" % count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
