# 示例

两个可以直接跑的 DAG，覆盖「最小可用」和「像真实工作」两档。

## 从项目根目录运行

```bash
cd miniflow

# 1. 最小示例：init 生成的那个（三步流水线）
python -m miniflow init /tmp/demo && cd /tmp/demo && python -m miniflow run hello_dag

# 2. 真实一点的示例：CSV -> 聚合 -> Markdown 日报
python -m miniflow --dags examples/dags list
python -m miniflow --dags examples/dags show daily_sales
python -m miniflow --dags examples/dags run daily_sales --param day=2026-10-20
```

跑完会在 `examples/output/` 看到 `sales-2026-10-20.md`。

## 这个示例演示了什么

| 文件 | 演示的能力 |
|---|---|
| `dags/etl_dag.py` | 根任务接收 `--param`、三个任务串联、跨进程传 dict、写文件、`retries` + `timeout` |
| `data/sales.csv` | 任务读取与 DAG 文件同目录的资源（用 `__file__` 定位，不依赖 cwd） |

`etl_dag.py` 里的 `read_rows` 有个细节值得注意：

```python
with DATA_FILE.open(encoding="utf-8-sig", newline="") as handle:
```

用 `utf-8-sig` 而不是 `utf-8`，是因为 Excel 导出的 CSV 常常带 BOM。
带 BOM 时第一列的列名会变成 `\ufeffdate`，`row["date"]` 直接 `KeyError`。
这是个很常见的坑，示例里直接处理掉了。

## 自己加一个 DAG

在 `examples/dags/` 下新建 `.py`：

```python
from miniflow import DAG, task

with DAG("my_flow", schedule=None) as dag:

    @task
    def step_one():
        return {"ok": True}

    @task
    def step_two(step_one):          # 参数名 = 上游 task_id
        print(step_one)

    step_one >> step_two
```

然后：

```bash
python -m miniflow --dags examples/dags run my_flow
```

文件名以 `_` 开头的不会被当成 DAG（用来放辅助模块）。
