# 排错手册

miniflow 的每个可预期错误都有编号。**先按编号查这张表**，再往下看「没有编号的问题」。

给脚本用的时候，直接判退出码比解析文本可靠：

| 退出码 | 含义 |
|---|---|
| 0 | 成功 |
| 1 | 一般性失败（极少出现） |
| 2 | 命令行用法错 |
| 3 | DAG 校验失败 |
| 4 | 任务本身跑失败 |
| 5 | 环境 / 配置 / 状态库问题 |

---

## 1. 错误码表

### MF001 找不到 DAG 目录

```
[MF001] 找不到 DAG 目录  DAG 目录不存在：E:\work\dags
```

三种可能：路径写错、目录里没有 `.py`、文件全以 `_` 开头（被当作辅助模块跳过）。

```bash
ls dags/                     # 确认真的有 .py
miniflow list --dags ./dags  # 显式指定
miniflow init                # 或者先生成示例
```

### MF002 DAG 文件加载失败

文件能读到，但 `import` 时抛了异常。`详情` 里会给出**具体的哪一种**：

| 提示 | 原因 | 怎么办 |
|---|---|---|
| `import 了一个没装的包：xxx` | 你的任务用了第三方库 | `pip install xxx` |
| `有语法错误：xx.py 第 N 行` | 语法/缩进错 | `python -m py_compile dags/xx.py` |
| `执行失败：xx.py` | 顶层代码抛异常 | 见下 |

最常见的原因是**在 DAG 文件顶层放了副作用代码**：

```python
# 错：import 这个文件时就会连数据库
conn = connect_db()

with DAG("x") as dag:
    @task
    def a():
        ...
```

```python
# 对：把连接放到任务函数里
with DAG("x") as dag:
    @task
    def a():
        conn = connect_db()
        ...
```

DAG 文件在**每次加载时都会被执行一遍**（`list`、`show`、`run`、每个子进程都会执行），
所以顶层只应该放「定义」。

### MF003 找不到指定的 DAG 或任务

```
[MF003] 找不到 DAG 'daily'  
       怎么修  现有 DAG：daily_report, etl
```

名字打错了，或者 `--dags` 指到了别的目录。注意 `dag_id` 是 `DAG("...")` 里的字符串，
不一定等于文件名。

### MF004 依赖关系成环

```
[MF004] 依赖关系成环  依赖关系成环：a -> b -> c -> a
```

报出来的路径方向和你在代码里写的 `>>` 一致。删掉环上任意一条边即可。

```python
a >> b >> c >> a     # 环
a >> b >> c          # 去掉最后一条
```

### MF005 任务参数无法绑定

三种子情形，消息会区分：

| 消息 | 原因 | 修法 |
|---|---|---|
| `参数 'x' 是同一个 DAG 里的任务，但它们之间没有依赖` | 忘了写 `>>` | 补 `x >> 你的任务` |
| `参数 'x' 绑不到任何上游输出` | 参数名和上游 id 不一致 | 改成上游的 task_id，或给默认值 |
| `需要运行时参数 'x'，但没提供` | 根任务的必填参数 | `--param x=值` |

记住唯一规则：**参数名 = 上游 task_id**。

```python
@task
def fetch():
    return {"rows": 1}

@task
def use(fetch):          # 对：参数名就是上游 id
    return fetch["rows"]

fetch >> use
```

### MF006 任务返回值无法序列化

```
[MF006] 任务返回值无法序列化  任务返回值是 set，不能转成 JSON
```

任务之间靠 JSON 传值。返回值必须是：`dict / list / str / int / float / bool / None`。

```python
# 错
return {1, 2, 3}

# 对
return [1, 2, 3]
```

`datetime`、`Decimal`、自定义对象都不行——**显式转成字符串或数字**，别指望自动转换
（自动转换会在下游拿到一个类型不对的值，比报错更难查）。

### MF007 任务执行超时

```
[MF007] 任务执行超时  任务执行超过 60.0s 被终止
```

`timeout` 到期后子进程被 `kill`。注意：

- 超时会被算作**一次失败**，如果配了 `retries` 会重试。
- Windows 上 `kill` 只杀直接子进程，如果任务里又起了子进程（比如 `subprocess` 调外部程序），
  那个孙进程可能残留。需要严格管控时用进程组或外部工具兜底。

### MF008 任务执行失败

任务函数自己抛了异常。日志里有完整 traceback：

```bash
miniflow logs <run_id> --task <task_id>
```

下游任务会变成 `upstream_failed`（不会执行），**无关分支继续跑**。

### MF009 cron 表达式非法

```
[MF009] cron 表达式非法  cron 表达式 '0 25 * * *' 的第 2 个字段（小时）取值 25 超出范围 0-23
```

字段顺序：`分 时 日 月 星期`。常见坑：

- 月份是 `1-12`，不是 `0-11`。
- 星期 `0` 和 `7` 都表示周日。
- 不支持 `L` / `W` / `#` / `?`（Quartz 扩展），也不支持 6 字段（带秒）。
- 当「日」和「星期」都不是 `*` 时，两者是**或**关系（Vixie cron 标准）。

验证：

```bash
miniflow cron next "0 25 * * *"     # 立刻报错并指出哪个字段
miniflow cron next "0 3 * * *" -n 5
```

### MF010 Python 版本过低

需要 3.9+。用完整路径指定解释器，或升级。

### MF011 配置文件非法

`miniflow.json` 必须是**纯 JSON**：不能有注释、不能有尾逗号、必须是对象。

```bash
python -m json.tool miniflow.json    # 会指出第几行出错
```

### MF012 状态库不可用

三种情况：

1. **目录不可写** → `miniflow doctor` 会实测，换 `--home`。
2. **文件损坏**（不是 SQLite 文件）→ 挪走旧文件，下次运行会自动重建（历史会丢）。
3. **被占用**（Windows 上另一个进程正开着它）→ 关掉那个进程，或换 `home`。

### MF013 DAG 定义写法有误

| 消息 | 修法 |
|---|---|
| `@task 必须写在 with DAG(...) 代码块里面` | 把 `@task` 函数缩进到 `with` 块内 |
| `with DAG(...) 不能嵌套` | 拆成两个 `with` 块，或拆到两个文件 |
| `DAG x 里有两个同名任务` | 给其中一个加 `@task(task_id="别的名字")` |
| `任务 x 被调用时传了参数` | 别写 `fetch(1)`，依赖用 `>>` 连 |
| `DAG x 里一个任务都没有` | `with` 块里至少要有一个 `@task` |
| `依赖了不存在的任务 'ghost'` | 检查 `>>` 两边是不是同一个 DAG 的任务 |

### MF014 `--param` 格式不对

必须是 `key=value`：

```bash
miniflow run x --param rows=10
miniflow run x --param name=alice --param debug=true
```

值会先按 JSON 解析：`10` 是整数，`true` 是布尔，`alice` 解析失败所以是字符串。
想强制字符串就加引号：`--param 'code="007"'`。

### MF015 找不到这次运行记录

`run_id` 抄错了，或者已经被 `miniflow clean` 清理掉。

```bash
miniflow runs -n 20
```

---

## 2. 没有错误码的问题

### 中文输出乱码

miniflow 启动时会把 stdout/stderr 强制切成 UTF-8，但**终端本身**要支持：

```bat
chcp 65001
```

Linux/macOS 确认 `LANG` 含 `UTF-8`。重定向到文件时不会乱码（文件永远是 UTF-8）。

### 任务里读不到相对路径的文件

子进程的**工作目录 = 你敲 `miniflow` 命令时所在的目录**，不是 DAG 文件所在目录。

```python
# 不稳：依赖 cwd
data = open("data.csv").read()

# 稳：相对 DAG 文件定位
import pathlib
data = (pathlib.Path(__file__).parent / "data.csv").read_text(encoding="utf-8")
```

### 调度器不触发

按顺序排查：

```bash
miniflow list                    # DAG 的「调度」列是手动触发吗？
miniflow show <dag_id>           # 「下次」是什么时间？
miniflow cron next "你的表达式"    # 表达式本身的语义对不对？
```

还要注意：**首次启动只登记当前时刻，不补跑历史**。如果你期望「启动就补跑一次」，
手动 `miniflow run` 一次即可。这是刻意设计——Airflow 的 `catchup=True` 默认值
让很多人一上线就被几百个历史任务淹没。

### 日志文件里没有任务的 print

Python 默认缓冲 stdout。任务结束时缓冲区会 flush，但如果任务被强杀（超时），
最后的输出可能丢。需要实时看到输出就显式 flush：

```python
print("progress...", flush=True)
```

### 每次运行都有 100~200ms 的固定开销

每个任务起一个独立 Python 子进程，启动解释器本身就要这么久。
这是「隔离」的代价：换来了崩溃不互相影响、超时能真杀、全局状态不串。

任务本身耗时（读文件、调接口、算数据）通常在秒级以上，这点开销可以忽略。
如果你的任务全是微秒级的纯计算，那不适合用任何调度器，直接写个脚本串起来更快。

### 路径含空格或中文

miniflow 内部全部用 `pathlib`，本身没问题。但你在 DAG 里拼 shell 命令时要注意引号：

```python
subprocess.run([sys.executable, script_path], check=True)   # 用列表，别用字符串
```

### 两台机器同时跑同一个 home

不要。SQLite 不是为跨机器共享设计的（网络文件系统上的文件锁尤其不可靠）。
每台机器用自己的 `--home`。

### 任务里想用虚拟环境里的包

子进程用的是**当前解释器**（`sys.executable`）。所以：

```bash
# 用哪个 python 启动 miniflow，任务就用哪个环境
/opt/venv/bin/python -m miniflow run etl
```

如果你的任务需要另一个环境的包，在任务函数里自己 `subprocess` 调那个环境的解释器。

---

## 3. 收集诊断信息

报 issue 或找人帮忙时，把这些贴出来：

```bash
python --version
python -m miniflow --version
python -m miniflow doctor
python -m miniflow list
python -m miniflow show <dag_id>
python -m unittest discover -s tests -t . 2>&1 | tail -20
```

出错的运行：

```bash
python -m miniflow runs -n 5
python -m miniflow logs <run_id>
python -m miniflow logs <run_id> --task <task_id>
```

**贴日志前先检查里面有没有密钥、密码、内网地址。**
