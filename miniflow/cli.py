"""命令行入口。

设计取向：每个子命令都能单独跑，不需要先起服务、不需要先建用户、
不需要先迁移数据库。`miniflow run <dag>` 就是全部。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import __version__
from .cron import parse_cron
from .dag import DAG
from .errors import (
    EXIT_OK,
    EXIT_TASK,
    EXIT_USAGE,
    MiniflowError,
    check_python_version,
    die,
)
from .loader import _CombinedLoadError, find_dag_files, load_all
from .runner import RunResult, run_dag
from .store import Store, resolve_home
from .templates import EXAMPLE_CONFIG, EXAMPLE_DAG, EXAMPLE_HELPERS

CONFIG_NAME = "miniflow.json"


# ---------------------------------------------------------------- 输出工具
def _setup_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass


def _human_ms(ms: int) -> str:
    if ms < 1000:
        return "%dms" % ms
    if ms < 60000:
        return "%.1fs" % (ms / 1000.0)
    return "%dm%02ds" % (ms // 60000, (ms % 60000) // 1000)


def _print_kv(key: str, value: str) -> None:
    print("  %-10s %s" % (key, value))


# ---------------------------------------------------------------- 配置
def load_config(explicit: Optional[str]) -> Tuple[Dict[str, Any], Optional[Path]]:
    candidates: List[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    elif os.environ.get("MINIFLOW_CONFIG"):
        candidates.append(Path(os.environ["MINIFLOW_CONFIG"]))
    else:
        candidates.append(Path.cwd() / CONFIG_NAME)

    for path in candidates:
        if not path.exists():
            if explicit:
                raise MiniflowError(
                    "MF011",
                    "配置文件不存在：%s" % path,
                    hint="去掉 --config 用默认查找，或先跑 miniflow init 生成一份。",
                    next_cmd="miniflow init",
                )
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise MiniflowError(
                "MF011",
                "配置文件读不出来：%s" % path,
                reason=str(exc),
                hint="配置文件必须是 UTF-8 的 JSON，不能有注释和尾逗号。",
                next_cmd="python -m json.tool %s" % path,
                detail=str(exc),
            )
        if not isinstance(data, dict):
            raise MiniflowError(
                "MF011",
                "配置文件顶层必须是 JSON 对象：%s" % path,
                hint='内容形如 {"dags_dir": "./dags", "home": "./.miniflow"}。',
            )
        return data, path.resolve()

    return {}, None


def resolve_paths(args: argparse.Namespace) -> Tuple[Path, Path, Dict[str, Any]]:
    config, config_path = load_config(getattr(args, "config", None))
    base = config_path.parent if config_path else Path.cwd()

    dags_dir = getattr(args, "dags", None) or config.get("dags_dir") or "dags"
    dags_path = Path(dags_dir).expanduser()
    if not dags_path.is_absolute():
        dags_path = (base / dags_path).resolve()

    home_arg = getattr(args, "home", None) or config.get("home")
    home_path = resolve_home(str((base / home_arg).resolve()) if home_arg else None)

    return dags_path, home_path, config


def open_store(home: Path) -> Store:
    return Store(home / "miniflow.db")


def get_dag(dags_dir: Path, dag_id: str) -> DAG:
    dags = load_all(dags_dir)
    if dag_id not in dags:
        raise MiniflowError(
            "MF003",
            "找不到 DAG %r" % dag_id,
            hint="现有 DAG：%s" % (", ".join(sorted(dags)) or "（无）"),
            next_cmd="miniflow list",
        )
    return dags[dag_id]


# ---------------------------------------------------------------- 子命令
def _scaffold(target: Path, force: bool) -> Path:
    """在 target 下生成 dags/、示例 DAG、配置。返回 target。"""
    dags_dir = target / "dags"
    example = dags_dir / "hello_dag.py"
    config_file = target / CONFIG_NAME

    if example.exists() and not force:
        raise MiniflowError(
            "MF011",
            "目标目录已经有示例了：%s" % example,
            hint="要覆盖就加 --force；要在别处新建就指定另一个目录。",
            next_cmd="miniflow list",
        )

    dags_dir.mkdir(parents=True, exist_ok=True)
    example.write_text(EXAMPLE_DAG, encoding="utf-8")
    (dags_dir / "_helpers.py").write_text(EXAMPLE_HELPERS, encoding="utf-8")
    config_file.write_text(EXAMPLE_CONFIG, encoding="utf-8")
    return target


def cmd_init(args: argparse.Namespace) -> int:
    target = Path(args.directory).expanduser().resolve()
    _scaffold(target, args.force)

    print("已生成：")
    print("  %s" % (target / "dags" / "hello_dag.py"))
    print("  %s" % (target / "dags" / "_helpers.py"))
    print("  %s" % (target / CONFIG_NAME))
    print()
    print("下一步：")
    print("  miniflow list                 # 看看这个 DAG")
    print("  miniflow show hello_dag       # 看依赖图")
    print("  miniflow run hello_dag        # 跑一次")
    return EXIT_OK


def cmd_demo(args: argparse.Namespace) -> int:
    """一键体验：生成示例 -> 立刻跑一次 -> 告诉你下一步。

    把流程放在 CLI 里而不是批处理里，是因为这里可以被自动化测试覆盖。
    """
    import tempfile

    if args.directory:
        target = Path(args.directory).expanduser().resolve()
    else:
        target = Path(tempfile.gettempdir()) / "miniflow-demo"

    print("miniflow %s  一键体验" % __version__)
    print("Python  %s" % ("%d.%d.%d" % sys.version_info[:3]))
    print("示例目录 %s" % target)
    print()

    _scaffold(target, force=True)

    dags = load_all(target / "dags")
    if "hello_dag" not in dags:
        raise MiniflowError(
            "MF003",
            "示例 DAG 生成后没有被加载到",
            hint="目标目录可能被其它进程占用，换一个 --directory 试试。",
        )

    home = target / ".miniflow"
    store = open_store(home)
    result = run_dag(dags["hello_dag"], store, home, echo=True)

    print()
    if not result.ok:
        print("演示失败（退出码 %d）。先跑体检：" % EXIT_TASK)
        print("  miniflow --dags \"%s\" --home \"%s\" doctor" % (target / "dags", home))
        return EXIT_TASK

    print("成功。这次运行的输出也写进了日志：")
    print("  miniflow --dags \"%s\" --home \"%s\" logs %s" % (target / "dags", home, result.run_id))
    print()
    print("接下来可以试：")
    print("  show      看依赖图")
    print("  runs      看运行历史")
    print("  doctor    做环境体检")
    print("  schedule  常驻定时（Ctrl+C 退出）")
    return EXIT_OK


def cmd_list(args: argparse.Namespace) -> int:
    dags_dir, home, _ = resolve_paths(args)
    store = open_store(home)

    try:
        dags = load_all(dags_dir)
    except _CombinedLoadError as exc:
        for item in exc.loaded.values():
            _print_dag_row(item, store)
        print()
        for err in exc.errors:
            print(err.render(verbose=args.verbose), file=sys.stderr)
        return err.exit_code

    if args.json:
        payload = []
        for dag in dags.values():
            last = store.latest_run_for_dag(dag.dag_id)
            item = dag.to_dict()
            item["last_run"] = (
                {"run_id": last["run_id"], "state": last["state"], "started_at": last["started_at"]}
                if last
                else None
            )
            payload.append(item)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return EXIT_OK

    if not dags:
        print("没有找到任何 DAG。")
        return EXIT_OK

    print("%-24s %-5s %-22s %s" % ("DAG", "任务", "调度", "上次运行"))
    print("-" * 78)
    for dag in sorted(dags.values(), key=lambda d: d.dag_id):
        _print_dag_row(dag, store)
    return EXIT_OK


def _print_dag_row(dag: DAG, store: Store) -> None:
    last = store.latest_run_for_dag(dag.dag_id)
    last_text = "从未运行"
    if last:
        last_text = "%s %s" % (
            last["state"],
            (last["started_at"] or "").replace("T", " ")[:19],
        )
    print(
        "%-24s %-5d %-22s %s"
        % (dag.dag_id, len(dag.tasks), _clip(dag.schedule_text(), 20), last_text)
    )


def _clip(text: str, width: int) -> str:
    return text if len(text) <= width else text[: width - 1] + "…"


def cmd_show(args: argparse.Namespace) -> int:
    dags_dir, home, _ = resolve_paths(args)
    dag = get_dag(dags_dir, args.dag_id)
    store = open_store(home)

    if args.json:
        print(json.dumps(dag.to_dict(), ensure_ascii=False, indent=2))
        return EXIT_OK

    print("DAG  %s" % dag.dag_id)
    if dag.description:
        _print_kv("描述", dag.description)
    _print_kv("调度", dag.schedule_text())
    if dag.schedule:
        now = datetime.now()
        upcoming = dag.cron().upcoming(now, 3)
        _print_kv("下次", ", ".join(t.strftime("%Y-%m-%d %H:%M") for t in upcoming))
    _print_kv("重试", "每个任务 %d 次" % dag.retries if dag.retries else "不重试")
    _print_kv("文件", dag.source_file or "?")

    roots_need = [
        (t.task_id, t.required_params())
        for t in dag.tasks.values()
        if t.required_params()
    ]
    if roots_need:
        print()
        print("需要 --param 的任务：")
        for task_id, names in roots_need:
            print("  %-16s %s" % (task_id, ", ".join(names)))

    last = store.latest_run_for_dag(dag.dag_id)
    if last:
        print()
        print("上次运行  %s  %s" % (last["run_id"], last["state"]))
        print("          miniflow logs %s" % last["run_id"])

    print()
    print("依赖图")
    print(dag.ascii_graph())
    return EXIT_OK


def cmd_run(args: argparse.Namespace) -> int:
    dags_dir, home, _ = resolve_paths(args)
    dag = get_dag(dags_dir, args.dag_id)
    store = open_store(home)
    params = _parse_params(args.param)

    if args.dry_run:
        order = dag.ancestors(args.task) if args.task else dag.topological_order()
        print("将按以下顺序执行（dry-run，不真的跑）：")
        for index, task_id in enumerate(order, 1):
            print("  %2d. %s" % (index, task_id))
        return EXIT_OK

    print("运行 %s" % dag.dag_id)
    result = run_dag(
        dag,
        store,
        home,
        params=params,
        only_task=args.task,
        verbose=args.verbose,
        echo=True,
    )

    print()
    _print_run_summary(result, home)
    return EXIT_OK if result.ok else EXIT_TASK


def _print_run_summary(result: RunResult, home: Path) -> None:
    counts = result.counts()
    print(
        "%s  %s  %s"
        % (
            "成功" if result.ok else "失败",
            result.run_id,
            _human_ms(result.duration_ms),
        )
    )
    print(
        "  成功 %d / 失败 %d / 上游失败 %d"
        % (
            counts.get("success", 0),
            counts.get("failed", 0),
            counts.get("upstream_failed", 0),
        )
    )
    if not result.ok:
        print()
        print("排查：")
        print("  miniflow logs %s              # 看所有任务日志路径" % result.run_id)
        print("  miniflow logs %s --task <id>  # 直接看某个任务的输出" % result.run_id)
    print()
    print("状态库 %s" % (home / "miniflow.db"))


def cmd_runs(args: argparse.Namespace) -> int:
    dags_dir, home, _ = resolve_paths(args)
    store = open_store(home)
    runs = store.list_runs(dag_id=args.dag, limit=args.limit)

    if args.json:
        print(json.dumps(runs, ensure_ascii=False, indent=2))
        return EXIT_OK

    if not runs:
        print("还没有运行记录。先跑一次：miniflow run <dag_id>")
        return EXIT_OK

    print("%-42s %-10s %-20s %s" % ("RUN_ID", "状态", "开始时间", "耗时"))
    print("-" * 96)
    for run in runs:
        started = (run.get("started_at") or "").replace("T", " ")[:19]
        finished = run.get("finished_at")
        duration = "-"
        if finished:
            try:
                delta = datetime.fromisoformat(finished) - datetime.fromisoformat(run["started_at"])
                duration = _human_ms(int(delta.total_seconds() * 1000))
            except (ValueError, TypeError):
                pass
        print("%-42s %-10s %-20s %s" % (run["run_id"], run["state"], started, duration))
    return EXIT_OK


def cmd_logs(args: argparse.Namespace) -> int:
    dags_dir, home, _ = resolve_paths(args)
    store = open_store(home)

    run = store.get_run(args.run_id)
    if run is None:
        raise MiniflowError(
            "MF015",
            "找不到运行记录 %r" % args.run_id,
            hint="列出最近的运行：miniflow runs -n 10",
            next_cmd="miniflow runs",
        )

    task_rows = store.task_runs(args.run_id)
    if not task_rows:
        print("这次运行没有任何任务记录。")
        return EXIT_OK

    if args.task:
        rows = [r for r in task_rows if r["task_id"] == args.task]
        if not rows:
            raise MiniflowError(
                "MF003",
                "运行 %s 里没有任务 %r" % (args.run_id, args.task),
                hint="这次运行的任务：%s"
                % ", ".join(sorted({r["task_id"] for r in task_rows})),
                next_cmd="miniflow logs %s" % args.run_id,
            )
        latest = sorted(rows, key=lambda r: r["attempt"])[-1]
        _dump_log(latest.get("log_path"))
        return EXIT_OK

    print("运行 %s  (%s)" % (args.run_id, run["state"]))
    print()
    for row in task_rows:
        print(
            "  %-22s attempt %d  %-8s %s"
            % (row["task_id"], row["attempt"], row["state"], row.get("log_path") or "")
        )
    print()
    print("看具体输出：miniflow logs %s --task <task_id>" % args.run_id)
    return EXIT_OK


def _dump_log(path_text: Optional[str]) -> None:
    if not path_text:
        print("这条记录没有日志文件。")
        return
    path = Path(path_text)
    if not path.exists():
        print("日志文件已不存在：%s" % path)
        return
    print("# %s" % path)
    print(path.read_text(encoding="utf-8", errors="replace"))


def cmd_schedule(args: argparse.Namespace) -> int:
    dags_dir, home, _ = resolve_paths(args)
    store = open_store(home)
    dags = load_all(dags_dir)

    scheduled = {k: v for k, v in dags.items() if v.schedule}
    if not scheduled:
        raise MiniflowError(
            "MF011",
            "没有任何 DAG 配置了 schedule",
            hint='在 with DAG(..., schedule="0 3 * * *") 里加一个 cron 表达式。',
            next_cmd="miniflow list",
        )

    print("调度器已启动（单进程，Ctrl+C 退出）")
    for dag in scheduled.values():
        last = store.get_last_logical_date(dag.dag_id)
        nxt = dag.cron().next_after(last or datetime.now())
        mark = "" if last else "  [首次登记，不补历史]"
        print("  %-24s %-18s 下次 %s%s" % (dag.dag_id, dag.schedule, nxt.strftime("%Y-%m-%d %H:%M"), mark))
    print()

    if args.dry_run:
        print("--dry-run：只显示计划，不执行。")
        return EXIT_OK

    try:
        while True:
            now = datetime.now()
            for dag in scheduled.values():
                last = store.get_last_logical_date(dag.dag_id)
                if last is None:
                    store.set_last_logical_date(dag.dag_id, now - timedelta(seconds=1))
                    continue
                nxt = dag.cron().next_after(last)
                if nxt > now:
                    continue

                store.set_last_logical_date(dag.dag_id, nxt)
                print("[%s] 触发 %s（计划时刻 %s）" % (now.strftime("%H:%M:%S"), dag.dag_id, nxt))
                try:
                    result = run_dag(dag, store, home, logical_date=nxt, echo=True)
                    print("         -> %s  %s" % (result.state, result.run_id))
                except MiniflowError as exc:
                    print(exc.render(verbose=args.verbose))

            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n调度器已停止。")
        return EXIT_OK

    return EXIT_OK


def cmd_cron(args: argparse.Namespace) -> int:
    schedule = parse_cron(args.expression)
    now = datetime.now()
    upcoming = schedule.upcoming(now, args.count)

    if args.json:
        print(
            json.dumps(
                {
                    "expression": schedule.expr,
                    "description": schedule.describe(),
                    "next": [t.isoformat() for t in upcoming],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return EXIT_OK

    print("表达式  %s" % schedule.expr)
    print("含义    %s" % schedule.describe())
    print("从现在起接下来 %d 次：" % len(upcoming))
    for index, moment in enumerate(upcoming, 1):
        print("  %2d. %s  (%s)" % (index, moment.strftime("%Y-%m-%d %H:%M"), _weekday_cn(moment)))
    return EXIT_OK


def _weekday_cn(moment: datetime) -> str:
    return "周" + "一二三四五六日"[moment.weekday()]


def cmd_doctor(args: argparse.Namespace) -> int:
    dags_dir, home, config = resolve_paths(args)
    checks: List[Tuple[str, str, str]] = []  # (级别, 项目, 说明)

    py = "%d.%d.%d" % sys.version_info[:3]
    if sys.version_info >= (3, 9):
        checks.append(("OK", "Python", py))
    else:
        checks.append(("FAIL", "Python", "%s 太低，需要 3.9+" % py))

    third_party = _third_party_imports()
    if third_party:
        checks.append(("WARN", "第三方依赖", "检测到 %s（miniflow 自身不需要）" % ", ".join(third_party)))
    else:
        checks.append(("OK", "第三方依赖", "0 个，全部走标准库"))

    try:
        probe = home / ".write-probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        checks.append(("OK", "MINIFLOW_HOME", str(home)))
    except OSError as exc:
        checks.append(("FAIL", "MINIFLOW_HOME", "不可写：%s" % exc))

    db_path = home / "miniflow.db"
    try:
        store = Store(db_path)
        integrity = store.integrity_check()
        stats = store.stats()
        checks.append(
            (
                "OK" if integrity == "ok" else "FAIL",
                "状态库",
                "%s（%s，%d 次运行）" % (integrity, db_path.name, stats["runs"]),
            )
        )
    except MiniflowError as exc:
        checks.append(("FAIL", "状态库", exc.message))

    try:
        files = find_dag_files(dags_dir)
        try:
            dags = load_all(dags_dir)
            checks.append(("OK", "DAG 目录", "%s（%d 个文件 / %d 个 DAG）" % (dags_dir, len(files), len(dags))))
        except _CombinedLoadError as exc:
            checks.append(
                ("FAIL", "DAG 目录", "%d 个文件加载失败：%s" % (len(exc.errors), exc.errors[0].message))
            )
    except MiniflowError as exc:
        checks.append(("FAIL", "DAG 目录", exc.message))

    if config:
        checks.append(("OK", "配置文件", "已读取 %d 个键" % len(config)))
    else:
        checks.append(("WARN", "配置文件", "没找到 %s，使用默认值" % CONFIG_NAME))

    encoding = (getattr(sys.stdout, "encoding", "") or "").lower()
    if "utf" in encoding:
        checks.append(("OK", "终端编码", encoding))
    else:
        checks.append(("WARN", "终端编码", "%s（中文可能乱码，建议 chcp 65001）" % (encoding or "unknown")))

    print("miniflow %s 环境体检" % __version__)
    print()
    for level, name, detail in checks:
        print("  %-4s %-14s %s" % (level, name, detail))

    failures = [c for c in checks if c[0] == "FAIL"]
    warnings = [c for c in checks if c[0] == "WARN"]
    print()
    if failures:
        print("%d 项需要处理，其余可用。" % len(failures))
        return 5
    if warnings:
        print("没有阻塞问题（%d 条提醒，不影响运行）。下一步：miniflow list" % len(warnings))
        return EXIT_OK
    print("一切正常。下一步：miniflow list")
    return EXIT_OK


def _third_party_imports() -> List[str]:
    """miniflow 的核心模块只允许标准库。这里做一次白名单检查。

    用 ast 而不是按行 grep：示例 DAG 的模板是字符串常量，
    里面写着 `from _helpers import ...`，按文本扫会误报成第三方依赖。
    """
    import ast

    allowed = {
        "argparse", "ast", "contextlib", "contextvars", "dataclasses", "datetime",
        "functools", "hashlib", "importlib", "inspect", "json", "os", "pathlib",
        "re", "shutil", "sqlite3", "subprocess", "sys", "threading", "time",
        "traceback", "typing", "uuid", "textwrap", "collections", "itertools",
        "math", "random", "shlex", "tempfile", "enum", "io", "signal", "platform",
        "locale", "unicodedata", "logging", "warnings",
    }
    found = set()
    package_dir = Path(__file__).resolve().parent

    for path in sorted(package_dir.glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    found.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.level:  # 相对导入，属于本包
                    continue
                if node.module:
                    found.add(node.module.split(".")[0])

    own_package = package_dir.name
    return sorted(
        name
        for name in found
        if name not in allowed and name != own_package and not _is_stdlib(name)
    )


def _is_stdlib(name: str) -> bool:
    return name in getattr(sys, "stdlib_module_names", set())


def cmd_clean(args: argparse.Namespace) -> int:
    dags_dir, home, _ = resolve_paths(args)
    store = open_store(home)

    if not args.force:
        stats = store.stats()
        print("将删除除最近 %d 次以外的全部运行记录（当前共 %d 次）。" % (args.keep, stats["runs"]))
        print("确认请加 --force 再跑一次。")
        return EXIT_OK

    removed = store.prune(keep=args.keep)
    log_root = (home / "logs").resolve()
    cleared = 0
    if log_root.exists():
        keep_ids = {r["run_id"] for r in store.list_runs(limit=args.keep)}
        for child in log_root.iterdir():
            if not child.is_dir() or child.name in keep_ids:
                continue
            target = child.resolve()
            if log_root not in target.parents:
                continue
            shutil.rmtree(target, ignore_errors=True)
            cleared += 1

    print("已清理 %d 条运行记录、%d 个日志目录。" % (removed, cleared))
    return EXIT_OK


# ---------------------------------------------------------------- 参数解析
def _parse_params(pairs: Optional[List[str]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for item in pairs or []:
        if "=" not in item:
            raise MiniflowError(
                "MF014",
                "--param 需要 key=value 形式，收到 %r" % item,
                hint='例如 --param rows=10 或 --param name=alice。',
                next_cmd="miniflow run <dag_id> --param rows=10",
            )
        key, _, raw = item.partition("=")
        key = key.strip()
        if not key:
            raise MiniflowError(
                "MF014",
                "--param 的 key 不能为空：%r" % item,
                hint="写成 name=value。",
            )
        try:
            out[key] = json.loads(raw)
        except ValueError:
            out[key] = raw
    return out


def _add_common_args(parser: argparse.ArgumentParser, suppress: bool = False) -> None:
    """全局参数同时挂在主命令和每个子命令上。

    子命令上用 SUPPRESS 作默认值，这样「miniflow run x --dags y」和
    「miniflow --dags y run x」两种写法都成立，且后者不会被前者覆盖成 None。
    """
    default: Any = argparse.SUPPRESS if suppress else None
    kwargs: Dict[str, Any] = {"default": default}
    parser.add_argument("--dags", help="DAG 目录（默认 ./dags，或配置文件里的 dags_dir）", **kwargs)
    parser.add_argument("--home", help="状态与日志目录（默认 ./.miniflow）", **kwargs)
    parser.add_argument("--config", help="配置文件路径（默认 ./miniflow.json）", **kwargs)
    if suppress:
        parser.add_argument(
            "--verbose", "-v", action="store_true", default=argparse.SUPPRESS,
            help="显示完整错误详情与重试过程",
        )
    else:
        parser.add_argument("--verbose", "-v", action="store_true", help="显示完整错误详情与重试过程")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="miniflow",
        description="单进程、零第三方依赖的 DAG 调度器。改一个 .py 文件就是改一个工作流。",
        epilog="第一次用：miniflow init && miniflow run hello_dag",
    )
    parser.add_argument("--version", action="version", version="miniflow %s" % __version__)
    _add_common_args(parser, suppress=False)

    sub = parser.add_subparsers(dest="command", metavar="<命令>")

    def make(name: str, help_text: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_text)
        _add_common_args(p, suppress=True)
        return p

    p_init = make("init", "生成一份能直接跑的示例")
    p_init.add_argument("directory", nargs="?", default=".", help="生成到哪个目录（默认当前目录）")
    p_init.add_argument("--force", action="store_true", help="覆盖已存在的示例文件")
    p_init.set_defaults(func=cmd_init)

    p_demo = make("demo", "一键体验：生成示例并立刻跑一次")
    p_demo.add_argument("--directory", help="示例放哪（默认系统临时目录下的 miniflow-demo）")
    p_demo.set_defaults(func=cmd_demo)

    p_list = make("list", "列出所有 DAG")
    p_list.add_argument("--json", action="store_true", help="输出 JSON")
    p_list.set_defaults(func=cmd_list)

    p_show = make("show", "查看某个 DAG 的结构与依赖图")
    p_show.add_argument("dag_id")
    p_show.add_argument("--json", action="store_true", help="输出 JSON")
    p_show.set_defaults(func=cmd_show)

    p_run = make("run", "运行一次 DAG")
    p_run.add_argument("dag_id")
    p_run.add_argument("--task", help="只跑到这个任务（含它的上游）")
    p_run.add_argument("--param", action="append", help="运行时参数 key=value，可重复")
    p_run.add_argument("--dry-run", action="store_true", help="只打印执行顺序，不真的跑")
    p_run.set_defaults(func=cmd_run)

    p_runs = make("runs", "查看运行历史")
    p_runs.add_argument("--dag", help="只看某个 DAG")
    p_runs.add_argument("-n", "--limit", type=int, default=20, help="显示条数（默认 20）")
    p_runs.add_argument("--json", action="store_true", help="输出 JSON")
    p_runs.set_defaults(func=cmd_runs)

    p_logs = make("logs", "查看某次运行的任务日志")
    p_logs.add_argument("run_id")
    p_logs.add_argument("--task", help="直接看这个任务的日志")
    p_logs.set_defaults(func=cmd_logs)

    p_sched = make("schedule", "启动常驻调度循环")
    p_sched.add_argument("--once", action="store_true", help="只检查一轮就退出")
    p_sched.add_argument("--dry-run", action="store_true", help="只显示计划，不执行")
    p_sched.add_argument("--interval", type=int, default=20, help="轮询间隔秒数（默认 20）")
    p_sched.set_defaults(func=cmd_schedule)

    p_cron = make("cron", "校验 cron 表达式并看未来触发时间")
    p_cron_sub = p_cron.add_subparsers(dest="cron_command", metavar="<动作>")
    p_cron_next = p_cron_sub.add_parser("next", help="打印未来 N 次触发时间")
    _add_common_args(p_cron_next, suppress=True)
    p_cron_next.add_argument("expression", help='5 字段 cron，例如 "0 3 * * *"')
    p_cron_next.add_argument("-n", "--count", type=int, default=5, help="打印几次（默认 5）")
    p_cron_next.add_argument("--json", action="store_true", help="输出 JSON")
    p_cron_next.set_defaults(func=cmd_cron)
    p_cron.set_defaults(func=lambda a: cmd_cron_help(p_cron))

    p_doc = make("doctor", "检查环境、配置、状态库、DAG 目录")
    p_doc.set_defaults(func=cmd_doctor)

    p_clean = make("clean", "清理旧的运行记录与日志")
    p_clean.add_argument("--keep", type=int, default=50, help="保留最近多少次（默认 50）")
    p_clean.add_argument("--force", action="store_true", help="真的执行删除")
    p_clean.set_defaults(func=cmd_clean)

    return parser


def cmd_cron_help(parser: argparse.ArgumentParser) -> int:
    parser.print_help()
    return EXIT_USAGE


# ---------------------------------------------------------------- 入口
def main(argv: Optional[List[str]] = None) -> int:
    _setup_stdio()
    parser = build_parser()
    args = parser.parse_args(argv)

    if not getattr(args, "command", None):
        parser.print_help()
        return EXIT_USAGE

    try:
        check_python_version()
        return int(args.func(args))
    except MiniflowError as exc:
        return die(exc, verbose=getattr(args, "verbose", False))
    except KeyboardInterrupt:
        print("\n已中断。", file=sys.stderr)
        return 130
    except BrokenPipeError:
        return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
