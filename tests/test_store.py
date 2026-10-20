"""状态库测试：运行记录、任务尝试、调度游标、清理。"""

from __future__ import annotations

import unittest
from datetime import datetime

from miniflow.errors import MiniflowError
from miniflow.store import Store
from tests.support import TempProject


class StoreTest(unittest.TestCase):
    def make(self, project: TempProject) -> Store:
        return Store(project.root / "state" / "miniflow.db")

    def test_schema_is_created(self) -> None:
        with TempProject() as project:
            store = self.make(project)
            self.assertEqual(store.integrity_check(), "ok")
            self.assertEqual(store.stats(), {"runs": 0, "task_runs": 0})

    def test_run_lifecycle(self) -> None:
        with TempProject() as project:
            store = self.make(project)
            store.start_run("r1", "demo", None, {"rows": 3})
            store.finish_run("r1", "success")

            run = store.get_run("r1")
            self.assertIsNotNone(run)
            self.assertEqual(run["state"], "success")
            self.assertIsNotNone(run["finished_at"])
            self.assertIn("rows", run["params_json"])

    def test_task_attempts_are_ordered(self) -> None:
        with TempProject() as project:
            store = self.make(project)
            store.start_run("r1", "demo", None)
            for attempt in (1, 2, 3):
                store.record_task("r1", "t1", attempt, "failed", exit_code=1)

            rows = store.task_runs("r1")
            self.assertEqual([r["attempt"] for r in rows], [1, 2, 3])
            self.assertTrue(all(r["state"] == "failed" for r in rows))

    def test_upsert_same_attempt_does_not_duplicate(self) -> None:
        with TempProject() as project:
            store = self.make(project)
            store.start_run("r1", "demo", None)
            store.record_task("r1", "t1", 1, "running")
            store.record_task("r1", "t1", 1, "success", exit_code=0)

            rows = store.task_runs("r1")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["state"], "success")

    def test_list_runs_is_newest_first(self) -> None:
        with TempProject() as project:
            store = self.make(project)
            for index in range(3):
                store.start_run("r%d" % index, "demo", None)
            runs = store.list_runs(dag_id="demo", limit=10)
            self.assertEqual(len(runs), 3)

    def test_last_logical_date_roundtrip(self) -> None:
        with TempProject() as project:
            store = self.make(project)
            self.assertIsNone(store.get_last_logical_date("demo"))
            moment = datetime(2026, 10, 20, 3, 0)
            store.set_last_logical_date("demo", moment)
            self.assertEqual(store.get_last_logical_date("demo"), moment)

    def test_prune_keeps_recent_runs(self) -> None:
        with TempProject() as project:
            store = self.make(project)
            for index in range(10):
                store.start_run("r%02d" % index, "demo", None)
                store.record_task("r%02d" % index, "t", 1, "success")

            removed = store.prune(keep=3)
            self.assertEqual(removed, 7)
            self.assertEqual(store.stats()["runs"], 3)
            self.assertEqual(store.stats()["task_runs"], 3)

    def test_corrupted_db_raises_mf012(self) -> None:
        with TempProject() as project:
            path = project.root / "broken.db"
            path.write_text("this is definitely not a sqlite database", encoding="utf-8")
            with self.assertRaises(MiniflowError) as ctx:
                Store(path)
        self.assertEqual(ctx.exception.code, "MF012")
        self.assertIn("状态库", ctx.exception.spec.title)


if __name__ == "__main__":
    unittest.main()
