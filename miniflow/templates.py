"""miniflow init 生成的文件内容。

刻意写成「能直接跑」而不是「最小骨架」：
新用户第一条命令就该看到 OK，而不是先读三页文档。
"""

EXAMPLE_DAG = '''"""最小可跑的 DAG：取数 -> 转换 -> 汇报。

跑它：
    miniflow run hello_dag
    miniflow run hello_dag --param rows=12
"""

from miniflow import DAG, task

from _helpers import summarize  # 同目录的辅助模块，下划线开头不会被当成 DAG

with DAG(
    "hello_dag",
    schedule=None,          # None = 只手动触发；要定时就写 cron，例如 "0 3 * * *"
    description="最小示例：三步流水线",
    retries=0,
) as dag:

    @task
    def fetch(rows: int = 5):
        """模拟取数。返回值会按 JSON 传给下游。"""
        print("fetch rows=%s" % rows)
        return {"rows": rows}

    @task(retries=1, retry_delay=1, timeout=30)
    def transform(fetch):
        """参数名 fetch 等于上游 task_id，miniflow 据此注入上游的返回值。"""
        doubled = fetch["rows"] * 2
        print("transform: %s -> %s" % (fetch["rows"], doubled))
        return {"rows": doubled}

    @task
    def report(transform):
        print(summarize(transform["rows"]))

    fetch >> transform >> report
'''

EXAMPLE_HELPERS = '''"""辅助模块：以 _ 开头，不会被 miniflow 当成 DAG 文件加载。"""


def summarize(rows):
    return "最终共 %d 行" % rows
'''

EXAMPLE_CONFIG = '''{
  "_comment": "miniflow 配置。删掉这一行也没关系。",
  "dags_dir": "./dags",
  "home": "./.miniflow"
}
'''
