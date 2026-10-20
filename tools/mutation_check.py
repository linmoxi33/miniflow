"""变异敏感度检查。

为什么需要这个：测试全绿只能说明「代码没坏」，不能说明「测试有用」。
如果删掉一行关键逻辑测试依然全绿，那行逻辑等于没有被测试保护。

做法：往源码里注入一个已知缺陷 -> 跑相关测试 -> 期望测试**失败**。
失败 = 缺陷被检测到（DETECTED）；仍然全绿 = 测试有空洞（NOT DETECTED）。

用法：
    python tools/mutation_check.py            # 跑全部变异
    python tools/mutation_check.py --list     # 只列出变异清单
    python tools/mutation_check.py -k timeout # 只跑名字含 timeout 的

安全：
- 每个变异都在 try/finally 里恢复，并在恢复后做 sha256 校验；
- Ctrl+C 也会走 finally；
- 任何一次恢复校验失败，脚本立即中止并打印原文件路径。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent

# Windows 控制台与 CI runner 的默认编码常是 cp1252 / GBK，打印中文会抛
# UnicodeEncodeError。所有面向人的输出统一走 UTF-8。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass


def write_lf(path: Path, text: str) -> None:
    """按 LF 写文件。不用 Path.write_text(newline=)，那是 3.10+ 才有的参数。"""
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)

MUTATIONS: List[Dict[str, Any]] = [
    {
        "name": "cron: 日 与 星期 的 OR 语义改成 AND",
        "file": "miniflow/cron.py",
        "old": "        return dom_ok or dow_ok",
        "new": "        return dom_ok and dow_ok",
        "occurrences": 2,
        "tests": ["tests.test_cron"],
    },
    {
        "name": "重试: 允许的尝试次数少一次",
        "file": "miniflow/runner.py",
        "old": "attempts_allowed = int(task.retries) + 1",
        "new": "attempts_allowed = int(task.retries)",
        "tests": ["tests.test_runner"],
    },
    {
        "name": "超时: 等待子进程时丢掉 timeout 参数",
        "file": "miniflow/runner.py",
        "old": "            proc.wait(timeout=timeout)",
        "new": "            proc.wait()",
        "tests": ["tests.test_runner"],
    },
    {
        "name": "失败传播: 不再阻断下游",
        "file": "miniflow/runner.py",
        "old": "        if any(up in blocked for up in task.upstream):",
        "new": "        if any(up in blocked for up in task.upstream) and False:",
        "tests": ["tests.test_runner"],
    },
    {
        "name": "传参: 下游不再读取上游输出",
        "file": "miniflow/runner.py",
        "old": "        if name in outputs:\n            kwargs[name] = outputs[name]",
        "new": "        if name in outputs and False:\n            kwargs[name] = outputs[name]",
        "tests": ["tests.test_runner"],
    },
    {
        "name": "环检测: 校验阶段不再报环",
        "file": "miniflow/dag.py",
        "old": "        cycle = self.find_cycle()\n        if cycle:",
        "new": "        cycle = None\n        if cycle:",
        "tests": ["tests.test_dag"],
    },
]


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def run_tests(modules: List[str], timeout: int = 300) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [sys.executable, "-m", "unittest", "-v"] + modules,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
    )


def failed_cases(output: str) -> List[str]:
    names = []
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith(("FAIL:", "ERROR:")):
            names.append(stripped.split("(")[0].replace("FAIL:", "").replace("ERROR:", "").strip())
    return names


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="注入缺陷，验证测试能否抓到")
    parser.add_argument("--list", action="store_true", help="只列出变异清单")
    parser.add_argument("-k", dest="keyword", default="", help="只跑名字含该关键字的变异")
    args = parser.parse_args(argv)

    selected = [m for m in MUTATIONS if args.keyword.lower() in m["name"].lower()]
    if args.list:
        for index, item in enumerate(selected, 1):
            print("%d. %-46s -> %s" % (index, item["name"], ", ".join(item["tests"])))
        return 0

    print("miniflow 变异敏感度检查")
    print("每个变异都应该让测试失败；仍然全绿说明测试有空洞。")
    print()

    baseline_modules = sorted({mod for item in selected for mod in item["tests"]})
    baseline = run_tests(baseline_modules)
    print("基线（未注入缺陷）：%s" % ("OK" if baseline.returncode == 0 else "FAILED"))
    if baseline.returncode != 0:
        print("基线都不通过，先修代码再谈变异检查。")
        print(baseline.stderr[-2000:])
        return 2
    print()

    undetected: List[str] = []
    for index, item in enumerate(selected, 1):
        path = ROOT / item["file"]
        original = path.read_text(encoding="utf-8")
        original_hash = sha256(original)

        if item["old"] not in original:
            print("[%d/%d] SKIP  %s（源码里找不到目标片段）" % (index, len(selected), item["name"]))
            undetected.append(item["name"] + "（片段缺失）")
            continue

        mutated = original.replace(item["old"], item["new"], item.get("occurrences", 1))
        if sha256(mutated) == original_hash:
            print("[%d/%d] SKIP  %s（替换未改变内容）" % (index, len(selected), item["name"]))
            continue

        write_lf(path, mutated)
        try:
            result = run_tests(item["tests"])
        finally:
            write_lf(path, original)
            restored = path.read_text(encoding="utf-8")
            if sha256(restored) != original_hash:
                print("!! 恢复失败：%s 已被污染，请手动还原 !!" % path)
                return 3

        detected = result.returncode != 0
        if detected:
            cases = failed_cases(result.stderr)
            print(
                "[%d/%d] DETECTED  %-40s 失败用例: %s"
                % (index, len(selected), item["name"], ", ".join(cases[:3]) or "(见输出)")
            )
        else:
            print("[%d/%d] NOT DETECTED  %s  <-- 测试没有覆盖这条逻辑" % (index, len(selected), item["name"]))
            undetected.append(item["name"])

    print()
    if undetected:
        print("%d 个变异没被检测到：" % len(undetected))
        for name in undetected:
            print("  - %s" % name)
        return 1

    print("全部 %d 个变异都被测试检测到。" % len(selected))
    return 0


if __name__ == "__main__":
    sys.exit(main())
