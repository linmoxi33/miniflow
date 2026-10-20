"""执行器测试：真的起子进程、真的超时、真的重试。

这些用例故意不做 mock —— 子进程隔离、超时杀进程、跨进程传值
正是 miniflow 的核心卖点，mock 掉就等于没测。
"""

from __future__ import annotations

import time
import unittest

from miniflow.errors import MiniflowError
from miniflow.loader import load_file
from miniflow.runner import STATE_FAILED, STATE_SUCCESS, STATE_UPSTREAM_FAILED, run_dag
from miniflow.store import Store
from tests.support import TempProject


class RunnerTestBase(unittest.TestCase):
    def build(self, project: TempProject, source: str, name: str = "flow"):
        path = project.dag(name + ".py", source)
        return load_file(path)[0]

    def store(self, project: TempProject) -> Store:
        return Store(project.root / "state" / "miniflow.db")

    def home(self, project: TempProject):
        return project.root / "home"


class RunnerSuccessTest(RunnerTestBase):
    def test_chain_passes_values_between_processes(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("chain") as dag:
                    @task
                    def a():
                        return {"rows": 3}

                    @task
                    def b(a):
                        return {"rows": a["rows"] * 2}

                    @task
                    def c(b):
                        return {"final": b["rows"]}

                    a >> b >> c
                """,
            )
            result = run_dag(dag, self.store(project), self.home(project), echo=False)

        self.assertEqual(result.state, STATE_SUCCESS)
        self.assertEqual(result.outputs["a"], {"rows": 3})
        self.assertEqual(result.outputs["b"], {"rows": 6})
        self.assertEqual(result.outputs["c"], {"final": 6})
        self.assertEqual([t.state for t in result.tasks], [STATE_SUCCESS] * 3)

    def test_runtime_params_reach_root_task(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("params") as dag:
                    @task
                    def root(rows):
                        return {"rows": rows}
                """,
            )
            result = run_dag(
                dag, self.store(project), self.home(project), params={"rows": 42}, echo=False
            )

        self.assertEqual(result.outputs["root"], {"rows": 42})

    def test_only_task_runs_ancestors_only(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("partial") as dag:
                    @task
                    def a():
                        return 1

                    @task
                    def b(a):
                        return a + 1

                    @task
                    def unrelated():
                        return 99

                    a >> b
                """,
            )
            result = run_dag(
                dag, self.store(project), self.home(project), only_task="b", echo=False
            )

        self.assertEqual(sorted(result.outputs), ["a", "b"])
        self.assertNotIn("unrelated", result.outputs)

    def test_logs_are_written_per_attempt(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("logging") as dag:
                    @task
                    def talk():
                        print("hello from worker")
                        return 1
                """,
            )
            result = run_dag(dag, self.store(project), self.home(project), echo=False)
            log_path = result.tasks[0].log_path
            self.assertIsNotNone(log_path)
            with open(log_path, encoding="utf-8") as handle:
                content = handle.read()

        self.assertIn("hello from worker", content)
        self.assertIn("exit_code=0", content)


class RunnerFailureTest(RunnerTestBase):
    def test_retry_really_reruns_the_function(self) -> None:
        with TempProject() as project:
            counter = project.root / "counter.txt"
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("retry") as dag:
                    @task(retries=2, retry_delay=0)
                    def flaky():
                        import pathlib
                        path = pathlib.Path(r"%s")
                        count = int(path.read_text()) if path.exists() else 0
                        count += 1
                        path.write_text(str(count))
                        if count < 3:
                            raise RuntimeError("第 %%d 次故意失败" %% count)
                        return {"attempts": count}
                """
                % counter.as_posix(),
            )
            result = run_dag(dag, self.store(project), self.home(project), echo=False)
            attempts_on_disk = counter.read_text()

        self.assertEqual(result.state, STATE_SUCCESS)
        self.assertEqual(result.tasks[0].attempts, 3)
        self.assertEqual(result.outputs["flaky"], {"attempts": 3})
        # 磁盘上的计数证明函数体真的执行了三次，不是只改了状态字段
        self.assertEqual(attempts_on_disk, "3")

    def test_retry_exhausted_reports_failure(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("always_fail") as dag:
                    @task(retries=1, retry_delay=0)
                    def broken():
                        raise RuntimeError("永远失败")
                """,
            )
            result = run_dag(dag, self.store(project), self.home(project), echo=False)

        self.assertEqual(result.state, STATE_FAILED)
        self.assertEqual(result.tasks[0].attempts, 2)
        self.assertIn("MF008", result.tasks[0].error or "")

    def test_timeout_kills_the_process(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("slow") as dag:
                    @task(timeout=1)
                    def slow():
                        import time
                        time.sleep(10)
                        return "never"
                """,
            )
            started = time.monotonic()
            result = run_dag(dag, self.store(project), self.home(project), echo=False)
            elapsed = time.monotonic() - started

        self.assertEqual(result.state, STATE_FAILED)
        self.assertIn("MF007", result.tasks[0].error or "")
        # 关键：真的被杀了，而不是老实等完 10 秒。
        # 阈值取 8 秒：正常情况 1~2 秒，kill 失效时必然 >= 10 秒。
        self.assertLess(elapsed, 8.0, "超时没有生效，实际耗时 %.1fs" % elapsed)

    def test_nonserializable_return_is_explained(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("baddata") as dag:
                    @task
                    def make_set():
                        return {1, 2, 3}
                """,
            )
            result = run_dag(dag, self.store(project), self.home(project), echo=False)

        self.assertEqual(result.state, STATE_FAILED)
        self.assertIn("MF006", result.tasks[0].error or "")
        self.assertIn("set", result.tasks[0].error or "")

    def test_failed_upstream_blocks_downstream_but_not_siblings(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("branch") as dag:
                    @task
                    def boom():
                        raise ValueError("炸")

                    @task
                    def downstream(boom):
                        return boom

                    @task
                    def independent():
                        return "还能跑"

                    boom >> downstream
                """,
            )
            result = run_dag(dag, self.store(project), self.home(project), echo=False)

        states = {task.task_id: task.state for task in result.tasks}
        self.assertEqual(states["boom"], STATE_FAILED)
        self.assertEqual(states["downstream"], STATE_UPSTREAM_FAILED)
        self.assertEqual(states["independent"], STATE_SUCCESS)
        self.assertNotIn("downstream", result.outputs)

    def test_missing_runtime_param_fails_before_spawning(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("needparam") as dag:
                    @task
                    def root(rows):
                        return rows
                """,
            )
            result = run_dag(dag, self.store(project), self.home(project), echo=False)

        self.assertEqual(result.state, STATE_FAILED)
        self.assertIn("rows", result.tasks[0].error or "")

    def test_validate_rejects_broken_dag_before_running(self) -> None:
        with TempProject() as project:
            dag = self.build(
                project,
                """
                from miniflow import DAG, task

                with DAG("ok") as dag:
                    @task
                    def a():
                        return 1
                """,
            )
            # 手动制造一个非法结构，确认 run_dag 自己会再校验一次
            dag.tasks["a"].upstream = {"ghost"}
            with self.assertRaises(MiniflowError) as ctx:
                run_dag(dag, self.store(project), self.home(project), echo=False)

        self.assertEqual(ctx.exception.code, "MF013")


if __name__ == "__main__":
    unittest.main()
