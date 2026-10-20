"""CLI 端到端测试：全部通过子进程调用真正的命令行。"""

from __future__ import annotations

import json
import unittest

from tests.support import TempProject, run_cli

HELLO = """
from miniflow import DAG, task

with DAG("hello", schedule="0 3 * * *") as dag:
    @task
    def fetch(rows: int = 2):
        print("fetch rows=%s" % rows)
        return {"rows": rows}

    @task
    def report(fetch):
        print("report got %s" % fetch["rows"])
        return fetch["rows"]

    fetch >> report
"""

BROKEN = """
from miniflow import DAG, task

with DAG("broken") as dag:
    @task
    def boom():
        raise RuntimeError("故意失败")
"""


class CliTest(unittest.TestCase):
    def test_init_then_run_works_out_of_the_box(self) -> None:
        """一键启动的硬标准：init 之后不改任何东西就能 run 成功。"""
        with TempProject() as project:
            init = run_cli("init", ".", cwd=project.root)
            self.assertEqual(init.returncode, 0, init.stderr)
            self.assertTrue((project.root / "dags" / "hello_dag.py").exists())
            self.assertTrue((project.root / "miniflow.json").exists())

            run = run_cli("run", "hello_dag", cwd=project.root)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertIn("成功", run.stdout)

    def test_init_refuses_to_overwrite_without_force(self) -> None:
        with TempProject() as project:
            self.assertEqual(run_cli("init", ".", cwd=project.root).returncode, 0)
            second = run_cli("init", ".", cwd=project.root)
            self.assertEqual(second.returncode, 5)
            self.assertIn("MF011", second.stderr)

    def test_list_and_show(self) -> None:
        with TempProject() as project:
            project.dag("hello.py", HELLO)
            listed = run_cli("list", cwd=project.root)
            self.assertEqual(listed.returncode, 0, listed.stderr)
            self.assertIn("hello", listed.stdout)

            shown = run_cli("show", "hello", cwd=project.root)
            self.assertEqual(shown.returncode, 0, shown.stderr)
            self.assertIn("fetch", shown.stdout)
            self.assertIn("report", shown.stdout)
            self.assertIn("每天 03:00", shown.stdout)

    def test_list_json_is_machine_readable(self) -> None:
        with TempProject() as project:
            project.dag("hello.py", HELLO)
            result = run_cli("list", "--json", cwd=project.root)
            payload = json.loads(result.stdout)
            self.assertEqual(len(payload), 1)
            self.assertEqual(payload[0]["dag_id"], "hello")
            self.assertEqual([t["task_id"] for t in payload[0]["tasks"]], ["fetch", "report"])

    def test_run_passes_params(self) -> None:
        with TempProject() as project:
            project.dag("hello.py", HELLO)
            result = run_cli("run", "hello", "--param", "rows=7", cwd=project.root)
            self.assertEqual(result.returncode, 0, result.stderr)
            runs = json.loads(run_cli("runs", "--json", cwd=project.root).stdout)
            run_id = runs[0]["run_id"]
            log = run_cli("logs", run_id, "--task", "fetch", cwd=project.root)
            self.assertIn("fetch rows=7", log.stdout)

    def test_run_failure_exit_code_is_four(self) -> None:
        with TempProject() as project:
            project.dag("broken.py", BROKEN)
            result = run_cli("run", "broken", cwd=project.root)
            self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
            self.assertIn("MF008", result.stdout)

    def test_unknown_dag_exit_code_is_two(self) -> None:
        with TempProject() as project:
            project.dag("hello.py", HELLO)
            result = run_cli("run", "nope", cwd=project.root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("MF003", result.stderr)
            self.assertIn("hello", result.stderr)  # 提示里要列出可用的 DAG

    def test_invalid_cron_exit_code_is_three(self) -> None:
        result = run_cli("cron", "next", "0 99 * * *")
        self.assertEqual(result.returncode, 3)
        self.assertIn("MF009", result.stderr)

    def test_cron_next_prints_five_slots(self) -> None:
        result = run_cli("cron", "next", "0 3 * * *", "-n", "5", "--json")
        payload = json.loads(result.stdout)
        self.assertEqual(len(payload["next"]), 5)
        self.assertEqual(payload["description"], "每天 03:00")

    def test_demo_generates_and_runs_in_one_command(self) -> None:
        """start.cmd 背后就是这一条命令，必须端到端可用。"""
        with TempProject() as project:
            target = project.root / "demo-target"
            result = run_cli("demo", "--directory", str(target), cwd=project.root)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("成功", result.stdout)
            self.assertTrue((target / "dags" / "hello_dag.py").exists())
            self.assertTrue((target / ".miniflow" / "miniflow.db").exists())

    def test_demo_is_rerunnable(self) -> None:
        with TempProject() as project:
            target = project.root / "demo-target"
            first = run_cli("demo", "--directory", str(target), cwd=project.root)
            second = run_cli("demo", "--directory", str(target), cwd=project.root)
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertEqual(second.returncode, 0, second.stderr)

    def test_dags_flag_works_after_subcommand(self) -> None:
        with TempProject() as project:
            other = project.root / "elsewhere"
            other.mkdir()
            (other / "hello.py").write_text(HELLO.strip() + "\n", encoding="utf-8")

            result = run_cli("run", "hello", "--dags", str(other), cwd=project.root)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_dags_flag_works_before_subcommand(self) -> None:
        with TempProject() as project:
            other = project.root / "elsewhere"
            other.mkdir()
            (other / "hello.py").write_text(HELLO.strip() + "\n", encoding="utf-8")

            result = run_cli("--dags", str(other), "list", cwd=project.root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("hello", result.stdout)

    def test_doctor_reports_environment(self) -> None:
        with TempProject() as project:
            project.dag("hello.py", HELLO)
            result = run_cli("doctor", cwd=project.root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Python", result.stdout)
            self.assertIn("状态库", result.stdout)
            self.assertIn("0 个", result.stdout)  # 第三方依赖 0 个

    def test_runs_and_clean(self) -> None:
        with TempProject() as project:
            project.dag("hello.py", HELLO)
            for _ in range(3):
                run_cli("run", "hello", cwd=project.root)

            listed = run_cli("runs", cwd=project.root)
            self.assertEqual(listed.returncode, 0, listed.stderr)

            cleaned = run_cli("clean", "--keep", "1", "--force", cwd=project.root)
            self.assertEqual(cleaned.returncode, 0, cleaned.stderr)

            remaining = json.loads(run_cli("runs", "--json", cwd=project.root).stdout)
            self.assertEqual(len(remaining), 1)

    def test_broken_file_is_reported_not_silently_skipped(self) -> None:
        with TempProject() as project:
            project.dag("hello.py", HELLO)
            project.dag("bad.py", "import nothing_here_at_all\n")
            result = run_cli("list", cwd=project.root)
            self.assertEqual(result.returncode, 3)
            self.assertIn("MF002", result.stderr)
            # 好的那个 DAG 仍然要被列出来
            self.assertIn("hello", result.stdout)

    def test_verbose_shows_traceback(self) -> None:
        with TempProject() as project:
            project.dag("broken.py", BROKEN)
            result = run_cli("run", "broken", "--verbose", cwd=project.root)
            self.assertEqual(result.returncode, 4)
            self.assertIn("RuntimeError", result.stdout)


if __name__ == "__main__":
    unittest.main()
