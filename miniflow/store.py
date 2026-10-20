"""运行状态持久化（SQLite，标准库自带）。

只存三样东西：每次 DAG 运行、每个任务的尝试、每个 DAG 上次调度到哪一刻。
不存执行历史里的业务数据——那些应该由任务自己写到自己该在的地方。
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .errors import MiniflowError

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dag_run (
    run_id       TEXT PRIMARY KEY,
    dag_id       TEXT NOT NULL,
    logical_date TEXT,
    state        TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    params_json  TEXT
);
CREATE INDEX IF NOT EXISTS idx_dag_run_dag ON dag_run(dag_id, started_at DESC);

CREATE TABLE IF NOT EXISTS task_run (
    run_id      TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    state       TEXT NOT NULL,
    started_at  TEXT,
    finished_at TEXT,
    exit_code   INTEGER,
    log_path    TEXT,
    error       TEXT,
    PRIMARY KEY (run_id, task_id, attempt)
);

CREATE TABLE IF NOT EXISTS dag_state (
    dag_id            TEXT PRIMARY KEY,
    last_logical_date TEXT
);
"""


def now_iso() -> str:
    # 保留微秒：运行耗时统计需要，展示时再截断到秒
    return datetime.now().isoformat(timespec="microseconds")


class Store:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._conn() as conn:
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
        except sqlite3.Error as exc:
            raise MiniflowError(
                "MF012",
                "初始化状态库失败：%s" % exc,
                reason="状态库是一个 SQLite 文件：%s" % self.db_path,
                hint="确认这个目录可写；若文件损坏，可以删掉它重新开始（运行历史会丢）。",
                next_cmd="miniflow doctor",
                detail=str(exc),
            )

    # ------------------------------------------------------------ 连接管理
    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=15)
        except sqlite3.Error as exc:
            raise MiniflowError(
                "MF012",
                "打不开状态库：%s" % exc,
                hint="检查文件权限；Windows 上如果被别的进程独占，先关掉那个进程。",
                next_cmd="miniflow doctor",
            )
        try:
            conn.execute("PRAGMA busy_timeout=15000")
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except sqlite3.Error as exc:
            conn.rollback()
            raise MiniflowError(
                "MF012",
                "状态库读写失败：%s" % exc,
                hint="确认磁盘没满、文件没被占用。",
                next_cmd="miniflow doctor",
                detail=str(exc),
            )
        finally:
            conn.close()

    # ------------------------------------------------------------ 运行记录
    def start_run(
        self,
        run_id: str,
        dag_id: str,
        logical_date: Optional[datetime],
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO dag_run"
                "(run_id, dag_id, logical_date, state, started_at, finished_at, params_json)"
                " VALUES(?,?,?,?,?,NULL,?)",
                (
                    run_id,
                    dag_id,
                    logical_date.isoformat() if logical_date else None,
                    "running",
                    now_iso(),
                    json.dumps(params or {}, ensure_ascii=False, default=str),
                ),
            )

    def finish_run(self, run_id: str, state: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE dag_run SET state=?, finished_at=? WHERE run_id=?",
                (state, now_iso(), run_id),
            )

    def record_task(
        self,
        run_id: str,
        task_id: str,
        attempt: int,
        state: str,
        exit_code: Optional[int] = None,
        log_path: Optional[str] = None,
        error: Optional[str] = None,
        started_at: Optional[str] = None,
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO task_run"
                "(run_id, task_id, attempt, state, started_at, finished_at, exit_code, log_path, error)"
                " VALUES(?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(run_id, task_id, attempt) DO UPDATE SET"
                " state=excluded.state, finished_at=excluded.finished_at,"
                " exit_code=excluded.exit_code, log_path=excluded.log_path,"
                " error=excluded.error",
                (
                    run_id,
                    task_id,
                    attempt,
                    state,
                    started_at or now_iso(),
                    now_iso(),
                    exit_code,
                    log_path,
                    error,
                ),
            )

    # ------------------------------------------------------------ 查询
    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM dag_run WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None

    def list_runs(self, dag_id: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM dag_run"
        args: List[Any] = []
        if dag_id:
            sql += " WHERE dag_id=?"
            args.append(dag_id)
        sql += " ORDER BY started_at DESC, run_id DESC LIMIT ?"
        args.append(int(limit))
        with self._conn() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]

    def task_runs(self, run_id: str) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM task_run WHERE run_id=? ORDER BY started_at, task_id, attempt",
                (run_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def latest_run_for_dag(self, dag_id: str) -> Optional[Dict[str, Any]]:
        runs = self.list_runs(dag_id=dag_id, limit=1)
        return runs[0] if runs else None

    # ------------------------------------------------------------ 调度游标
    def get_last_logical_date(self, dag_id: str) -> Optional[datetime]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT last_logical_date FROM dag_state WHERE dag_id=?", (dag_id,)
            ).fetchone()
        if not row or not row["last_logical_date"]:
            return None
        try:
            return datetime.fromisoformat(row["last_logical_date"])
        except ValueError:
            return None

    def set_last_logical_date(self, dag_id: str, moment: datetime) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO dag_state(dag_id, last_logical_date) VALUES(?,?)"
                " ON CONFLICT(dag_id) DO UPDATE SET last_logical_date=excluded.last_logical_date",
                (dag_id, moment.isoformat()),
            )

    # ------------------------------------------------------------ 维护
    def prune(self, keep: int = 50) -> int:
        """只保留最近 keep 次运行，返回删掉的运行条数。"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT run_id FROM dag_run ORDER BY started_at DESC LIMIT -1 OFFSET ?",
                (int(keep),),
            ).fetchall()
            old = [r["run_id"] for r in rows]
            for run_id in old:
                conn.execute("DELETE FROM task_run WHERE run_id=?", (run_id,))
                conn.execute("DELETE FROM dag_run WHERE run_id=?", (run_id,))
            return len(old)

    def stats(self) -> Dict[str, int]:
        with self._conn() as conn:
            return {
                "runs": conn.execute("SELECT COUNT(*) c FROM dag_run").fetchone()["c"],
                "task_runs": conn.execute("SELECT COUNT(*) c FROM task_run").fetchone()["c"],
            }

    def integrity_check(self) -> str:
        with self._conn() as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            return row[0] if row else "unknown"


def resolve_home(explicit: Optional[str] = None) -> Path:
    """确定 MINIFLOW_HOME：命令行 > 环境变量 > 当前目录下的 .miniflow。"""
    candidate = explicit or os.environ.get("MINIFLOW_HOME") or ".miniflow"
    path = Path(candidate).expanduser()
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise MiniflowError(
            "MF012",
            "无法创建 MINIFLOW_HOME 目录：%s" % path,
            reason=str(exc),
            hint="换一个可写目录，或用 --home 指定，例如 --home ./data。",
            next_cmd="miniflow doctor",
        )
    return path.resolve()
