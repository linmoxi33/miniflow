"""DAG 模型：结构、拓扑序、环检测、参数绑定校验。"""

from __future__ import annotations

import unittest

from miniflow.errors import MiniflowError
from miniflow.loader import load_file
from tests.support import TempProject


def load_one(project: TempProject, source: str):
    path = project.dag("dag.py", source)
    dags = load_file(path)
    return dags[0]


class DagStructureTest(unittest.TestCase):
    def test_basic_chain(self) -> None:
        with TempProject() as project:
            dag = load_one(
                project,
                """
                from miniflow import DAG, task

                with DAG("chain") as dag:
                    @task
                    def a():
                        return 1

                    @task
                    def b(a):
                        return a

                    a >> b
                """,
            )
            self.assertEqual(dag.dag_id, "chain")
            self.assertEqual(sorted(dag.tasks), ["a", "b"])
            self.assertEqual(dag.topological_order(), ["a", "b"])
            self.assertEqual(dag.roots(), ["a"])
            self.assertEqual(dag.leaves(), ["b"])

    def test_diamond_topological_order_is_stable(self) -> None:
        source = """
            from miniflow import DAG, task

            with DAG("diamond") as dag:
                @task
                def a():
                    return 1

                @task
                def b(a):
                    return a

                @task
                def c(a):
                    return a

                @task
                def d(c):
                    return c

                a >> b
                a >> c
                b >> d
                c >> d
            """
        with TempProject() as project:
            first = load_one(project, source).topological_order()
        with TempProject() as project:
            second = load_one(project, source).topological_order()
        self.assertEqual(first, second)
        self.assertEqual(first.index("a"), 0)
        self.assertLess(first.index("c"), first.index("d"))
        self.assertEqual(first.index("b"), 1)

    def test_ancestors_are_minimal(self) -> None:
        with TempProject() as project:
            dag = load_one(
                project,
                """
                from miniflow import DAG, task

                with DAG("anc") as dag:
                    @task
                    def a():
                        return 1

                    @task
                    def b(a):
                        return a

                    @task
                    def side():
                        return 9

                    @task
                    def c(b):
                        return b

                    a >> b >> c
                """,
            )
            self.assertEqual(dag.ancestors("c"), ["a", "b", "c"])
            self.assertNotIn("side", dag.ancestors("c"))

    def test_ascii_graph_lists_every_task(self) -> None:
        with TempProject() as project:
            dag = load_one(
                project,
                """
                from miniflow import DAG, task

                with DAG("g") as dag:
                    @task
                    def a():
                        return 1

                    @task(retries=2, timeout=30)
                    def b(a):
                        return a

                    a >> b
                """,
            )
            graph = dag.ascii_graph()
            self.assertIn("a", graph)
            self.assertIn("b", graph)
            self.assertIn("retries=2", graph)
            self.assertIn("timeout=30s", graph)

    def test_runtime_params_detected(self) -> None:
        with TempProject() as project:
            dag = load_one(
                project,
                """
                from miniflow import DAG, task

                with DAG("p") as dag:
                    @task
                    def need(x, y=2):
                        return x

                    @task
                    def use(need):
                        return need
                    need >> use
                """,
            )
            self.assertEqual(dag.tasks["need"].required_params(), ["x"])
            self.assertEqual(dag.tasks["use"].required_params(), [])


class DagValidationTest(unittest.TestCase):
    def test_cycle_reports_arrow_direction(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG, task

                with DAG("cycle") as dag:
                    @task
                    def a(b):
                        return b

                    @task
                    def b(c):
                        return c

                    @task
                    def c(a):
                        return a

                    a >> b >> c >> a
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        err = ctx.exception
        self.assertEqual(err.code, "MF004")
        # 报出来的顺序要和用户写的 >> 一致，不能是反的
        self.assertIn("a -> b -> c -> a", err.message)

    def test_missing_edge_gives_actionable_hint(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG, task

                with DAG("noedge") as dag:
                    @task
                    def boom():
                        raise ValueError("x")

                    @task
                    def downstream(boom):
                        return boom
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        err = ctx.exception
        self.assertEqual(err.code, "MF005")
        self.assertIn("boom >> downstream", err.render(verbose=True, color=False))

    def test_wrong_param_name_suggests_upstream(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG, task

                with DAG("wrong") as dag:
                    @task
                    def fetch():
                        return 1

                    @task
                    def use(nope):
                        return nope

                    fetch >> use
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        err = ctx.exception
        self.assertEqual(err.code, "MF005")
        self.assertIn("'fetch'", err.render(verbose=True, color=False))

    def test_duplicate_task_id(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG, task

                with DAG("dup") as dag:
                    @task
                    def a():
                        return 1

                    @task(task_id="a")
                    def b():
                        return 2
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        self.assertEqual(ctx.exception.code, "MF013")
        self.assertIn("同名任务", ctx.exception.message)

    def test_task_outside_dag(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import task

                @task
                def orphan():
                    return 1
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        self.assertEqual(ctx.exception.code, "MF013")
        self.assertIn("with DAG", ctx.exception.message)

    def test_nested_dag_rejected(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG, task

                with DAG("outer") as outer:
                    @task
                    def a():
                        return 1

                    with DAG("inner") as inner:
                        @task
                        def b():
                            return 2
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        self.assertEqual(ctx.exception.code, "MF013")
        self.assertIn("嵌套", ctx.exception.message)

    def test_calling_task_with_args_rejected(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG, task

                with DAG("call") as dag:
                    @task
                    def a(x):
                        return x

                    a(1)
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        self.assertEqual(ctx.exception.code, "MF013")
        self.assertIn("参数", ctx.exception.message)

    def test_bad_schedule_surfaces_as_mf009(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG, task

                with DAG("badcron", schedule="0 99 * * *") as dag:
                    @task
                    def a():
                        return 1
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        self.assertEqual(ctx.exception.code, "MF009")

    def test_empty_dag_rejected(self) -> None:
        with TempProject() as project:
            path = project.dag(
                "dag.py",
                """
                from miniflow import DAG

                with DAG("empty") as dag:
                    pass
                """,
            )
            with self.assertRaises(MiniflowError) as ctx:
                load_file(path)
        self.assertEqual(ctx.exception.code, "MF013")


if __name__ == "__main__":
    unittest.main()
