# 从 Airflow 迁移到 miniflow

这份文档只对一件事负责：**如果你的 DAG 属于 miniflow 的能力范围，怎么改最快。**

先看第 1 节判断「该不该迁」，别看完代码才发现能力不够。

---

## 1. 先做判断

### 适合迁移

- DAG 数量在几十个以内，跑在同一台机器上
- 任务类型以「跑 Python 函数 / 调 HTTP / 读写文件」为主
- 依赖关系用 `>>` 表达，参数靠 XCom 传
- 需要的是「按时跑 + 别重复跑 + 失败能重试 + 留下日志」
- 团队里只有 1~3 个人看这些 DAG

### 不适合迁移

| 你正在用 | 原因 |
|---|---|
| Web UI 看任务状态、手工点重跑 | miniflow 没有 UI |
| Celery / Kubernetes Executor | miniflow 单进程串行 |
| Connections / Variables 管理界面 | miniflow 用环境变量 |
| 云厂商 Operator（S3、BigQuery、EMR…） | miniflow 没有 Operator 生态 |
| RBAC、多人协作、审计 | miniflow 没有权限模型 |
| `catchup=True` 自动补历史 | miniflow 刻意不补 |
| Datasets / 事件驱动调度 | 没有实现 |
| SLA、告警、回调 | 只能从外部读状态 |

**混合方案**：核心链路留在 Airflow，把「几个小脚本按顺序跑」这类边角需求挪到 miniflow。
两者可以共存，互不干扰。

---

## 2. 概念对照

| Airflow | miniflow | 说明 |
|---|---|---|
| `DAG(dag_id=..., schedule=...)` | `DAG("id", schedule="...")` | 参数名更短，其余相同 |
| `PythonOperator` | `@task` 装饰函数 | 不需要 import operator |
| `@task` (TaskFlow) | `@task` | 语义基本一致 |
| `task_a >> task_b` | `task_a >> task_b` | 相同 |
| XCom `ti.xcom_pull(...)` | 参数名 = 上游 task_id | 自动注入，不用手写 pull |
| `retries` / `retry_delay` | `retries` / `retry_delay` | `retry_delay` 用秒，不是 `timedelta` |
| `execution_timeout` | `timeout` | 秒 |
| `schedule_interval` / `schedule` | `schedule` | 只支持 5 字段 cron 或 `None` |
| `start_date` | `start_date` | 保留但仅作元数据，不影响补跑 |
| `catchup` | —— | 不补历史，参数被忽略 |
| `max_active_runs` | —— | 单进程串行，无需此参数 |
| `default_args` | `DAG(...)` 上的 `retries` / `timeout` | 全局默认写在 DAG 上 |
| `airflow dags list` | `miniflow list` | |
| `airflow tasks test` | `miniflow run <dag> --task <id>` | 会真的写状态库 |
| `airflow dags backfill` | —— | 手动 `run` 多次 |
| Connections | 环境变量 / `.env` | |
| Variables | 配置文件 / `--param` | |
| Pools / Priority | —— | 无并发，无需 |
| Plugins | —— | 直接 import 自己的代码 |

---

## 3. 改写示例

### 改写前（Airflow）

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook


def extract(**context):
    hook = HttpHook(method="GET", http_conn_id="my_api")
    resp = hook.run("/rows")
    return resp.json()["rows"]


def transform(**context):
    rows = context["ti"].xcom_pull(task_ids="extract")
    return [r for r in rows if r["ok"]]


def load(**context):
    rows = context["ti"].xcom_pull(task_ids="transform")
    print("loaded", len(rows))


default_args = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="daily_etl",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)
    t1 >> t2 >> t3
```

### 改写后（miniflow）

```python
import urllib.request
import json

from miniflow import DAG, task

with DAG(
    "daily_etl",
    schedule="0 3 * * *",
    start_date="2026-01-01",
    retries=2,
    retry_delay=300,      # 秒，不是 timedelta
    timeout=600,
) as dag:

    @task
    def extract():
        url = "https://api.example.com/rows"
        with urllib.request.urlopen(url, timeout=30) as resp:
            return {"rows": json.load(resp)["rows"]}

    @task
    def transform(extract):          # 参数名 = 上游 task_id，不用 xcom_pull
        return {"rows": [r for r in extract["rows"] if r["ok"]]}

    @task
    def load(transform):
        print("loaded", len(transform["rows"]))
        return {"loaded": len(transform["rows"])}

    extract >> transform >> load
```

改动点：

1. 去掉 `default_args` 字典，`retries` / `retry_delay` / `timeout` 直接写在 `DAG(...)` 上。
2. `timedelta` 换成秒数。
3. `PythonOperator` 换成 `@task`。
4. `xcom_pull` 换成参数名绑定。
5. `HttpHook` + Connection 换成直接 `urllib`（或你自己 `pip install requests`）。
6. `catchup` 删掉。

---

## 4. 逐项改写的坑

### 4.1 XCom 换成参数名

Airflow：

```python
def transform(**context):
    rows = context["ti"].xcom_pull(task_ids="extract")
```

miniflow：

```python
@task
def transform(extract):
    rows = extract
```

**函数参数名必须等于上游的 `task_id`。** 写错会在加载时报 MF005，并告诉你该改成什么。

多个上游：

```python
@task
def merge(extract_a, extract_b):
    return extract_a + extract_b

extract_a >> merge
extract_b >> merge
```

只需要其中一个：

```python
@task
def only_a(extract_a):
    return extract_a
```

（另一个上游仍然会执行，只是它的输出没被用到。这符合依赖语义。）

### 4.2 XCom 的 JSON 限制变严格

Airflow 的 XCom 在某些后端能塞更多类型，miniflow 一律走 JSON。
`datetime`、`Decimal`、`set`、`bytes`、自定义类都不行。

```python
# 错
return {"ts": datetime.now()}

# 对
return {"ts": datetime.now().isoformat()}
```

### 4.3 `start_date` 不再驱动补跑

Airflow 的 `start_date` + `catchup=True` 会从 `start_date` 开始补跑。
miniflow 里 `start_date` 只是元数据，调度游标从**首次启动时刻**开始。

想要历史数据，自己写循环：

```bash
for d in 2026-10-01 2026-10-02 2026-10-03; do
  python -m miniflow run daily_etl --param date=$d
done
```

### 4.4 Connections 换成环境变量

Airflow：

```python
hook = HttpHook(http_conn_id="my_api")
```

miniflow：

```python
import os
token = os.environ["MY_API_TOKEN"]        # 从环境变量读
url = os.environ.get("MY_API_URL", "https://api.example.com")
```

在 systemd 里注入：

```ini
Environment=MY_API_TOKEN=xxxxx
```

Windows 计划任务里，在「操作」里包一层 `cmd /c set MY_API_TOKEN=xxx && python -m ...`，
或者用用户级环境变量（`setx MY_API_TOKEN xxx`，需重开终端）。

**不要**把密钥写进 DAG 文件——它会被 `list`、`show`、每个子进程反复读取，
而且很可能被提交进 git。

### 4.5 依赖 `airflow.models` 的代码要改

`Variable.get()`、`Connection.get()`、`ti.xcom_push()`、`dag_run.conf` 都没有对应物。

替代方案：

| Airflow | miniflow |
|---|---|
| `Variable.get("k")` | 配置文件 / 环境变量 |
| `dag_run.conf["x"]` | `--param x=...` |
| `ti.xcom_push(key=..., value=...)` | 直接 `return`，下游按名字接 |
| `ti.try_number` | 任务函数里拿不到；需要幂等就用外部状态 |

### 4.6 时区

miniflow 全部使用**本机本地时间**（`datetime.now()`），不做时区转换。

如果你的 Airflow DAG 用了 `timezone("Asia/Shanghai")` 而机器在 UTC，
迁移后触发时间会变。解决办法：把机器时区设对，或把 cron 换算成 UTC 再写。

### 4.7 `schedule_interval` 的 preset

| Airflow | miniflow |
|---|---|
| `@daily` | `"@daily"` 或 `"0 0 * * *"` |
| `@hourly` | `"@hourly"` 或 `"0 * * * *"` |
| `timedelta(hours=6)` | `"0 */6 * * *"`（不是所有间隔都能用 cron 表达） |
| `None` | `None` |

`timedelta` 类间隔（比如「每 90 分钟」）cron 表达不了，需要拆成两条或用外部驱动。

---

## 5. 迁移检查清单

- [ ] 统计现有 DAG 数量与任务数，确认在能力范围内
- [ ] 确认没有用 Connection / Variable / Hook / Operator 生态
- [ ] 逐个 DAG 改写，先只改结构不改逻辑
- [ ] `miniflow list` 能加载全部 DAG（一条 MF002 都不能有）
- [ ] `miniflow show <每个dag>` 检查依赖图与原来一致
- [ ] 手工 `run` 一遍，对比输出与 Airflow 那次的结果
- [ ] 检查所有返回值都能 JSON 序列化
- [ ] 确认时区与触发时间
- [ ] 观察一周，确认没有「Airflow 会自动做但 miniflow 不做」的隐性依赖
- [ ] 确认告警/通知链路已迁走（miniflow 自己不发通知）

---

## 6. 迁移后仍然需要 Airflow 的信号

出现以下任一情况，说明选错了工具，早退回早省事：

- 开始有人问「能不能在网页上点一下重跑」
- 开始需要「这个 DAG 只能 A 组看」
- 开始需要「同时跑 20 个任务」
- 开始需要「任务跑在另一台有 GPU 的机器上」
- 开始写「给 miniflow 加个插件」的代码
