"""miniflow：单进程、零第三方依赖的 DAG 调度器。

最常用的那 20% 长这样：

    from miniflow import DAG, task

    with DAG("daily", schedule="0 3 * * *") as dag:
        @task
        def extract():
            return {"rows": 3}

        @task(retries=1, timeout=60)
        def load(extract):          # 参数名 = 上游 task_id
            print(extract["rows"])

        extract >> load

其余交给 `miniflow run daily`。
"""

from .dag import DAG, Task, get_current_dag, task

__version__ = "0.1.0"
__author__ = "Vespera (linmoxi33)"
__license__ = "Apache-2.0"

__all__ = ["DAG", "Task", "task", "get_current_dag", "__version__"]
