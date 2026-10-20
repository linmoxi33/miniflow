# 改造分析：从 Apache Airflow 到 miniflow

> 日期：2026-10-20　目标仓库：`apache/airflow`　产物：`miniflow 0.1.0`

---

## 0. 一句话结论

Airflow 的**编排内核**（DAG 声明、依赖解析、跨任务传值、定时、重试、状态、日志）只占它代码量与部署成本的很小一部分，
但决定了它 90% 的日常价值。把这部分独立重写，可以做到**零第三方依赖、单进程、Windows 原生**，
代价是放弃 Web UI、分布式执行与 Provider 生态——这些恰好是单人/小团队场景里成本最高、收益最低的部分。

---

## 1. 选题：为什么是 Airflow

### 1.1 候选对比（2026-10-20 实测）

| 仓库 | Stars | GitHub 识别的许可证 | 是否适合做衍生 | 结论 |
|---|---:|---|---|---|
| `apache/airflow` | 47,227 | **Apache-2.0** | ✅ OSI 认可，允许衍生与再分发，只需保留声明 | **选中** |
| `langgenius/dify` | 159,014 | NOASSERTION | ⚠️ 实际为 Apache-2.0 **附加限制条款**（多租户 SaaS、品牌等） | 排除 |
| `n8n-io/n8n` | 208,121 | NOASSERTION | ❌ Sustainable Use License，非 OSI 开源 | 排除 |
| `opendatalab/MinerU` | 82,120 | NOASSERTION | ⚠️ AGPL-3.0，衍生必须整体 AGPL，传染性强 | 排除 |

数据来自 GitHub REST API `repos/{owner}/{repo}` 的 `stargazers_count` 与 `license.spdx_id` 字段。

选 Airflow 的两个理由：

1. **许可证干净**。Apache-2.0 是少数可以放心做「独立实现 + 再分发」的许可证，义务只有保留声明与变更说明。
2. **它的复杂度是结构性的，不是偶然的**。130 个依赖、5 个常驻进程、必须 WSL2——这些不是为了炫技，
   而是「分布式 + 多租户 + Web UI + Provider 生态」这套设计的必然成本。砍掉那些目标，成本就塌下来了。

### 1.2 「部署复杂」的量化证据

**依赖规模**

```bash
pip install --dry-run --ignore-installed --report dep_report.json "apache-airflow==3.3.2"
# -> 130 个待安装包（不含任何 extras）
```

拆开看：

- `apache-airflow==3.3.2` 本身是个 meta 包，`requires_dist` 有 251 条（绝大多数带 extra 条件），非 extra 只有 2 条：
  `apache-airflow-core==3.3.2`、`apache-airflow-task-sdk==1.3.2`
- `apache-airflow-core==3.3.2` 的 `requires_dist` 有 88 条，其中 **68 条是非 extra 的直接依赖**：
  `alembic`、`sqlalchemy[asyncio]`、`fastapi`、`uvicorn`、`uvloop`（非 Windows）、`cryptography`、
  `libcst`、`opentelemetry-*`、`pendulum`、`pydantic`、`croniter`、`rich`、`structlog`……
- 传递依赖把 68 推高到 130

**部署步骤**（摘自 Airflow 3.3.2 官方文档）

官方 Quick Start 明确指出 Windows 用户必须：

```bash
wsl --install
```

手动模式（不用 `standalone` 魔法命令）需要：

```bash
airflow db migrate
airflow users create --username admin --firstname ... --lastname ... --role Admin --email ...
airflow api-server --port 8080
airflow scheduler
airflow dag-processor
airflow triggerer
```

即 **6 条命令 + 5 个常驻进程 + 一个必须提前准备好的元数据库**。
官方 Installation 页面自己也写了：*"installation of Airflow might be quite complex"*。

这些都不是「文档没写好」，而是架构决定的：元数据库要迁移、调度器与 API 服务要分离、
任务解析要独立进程、延迟任务要 triggerer。

---

## 2. 许可证合规判断

### 2.1 上游许可证事实

- `LICENSE`：标准 Apache License 2.0 全文（11,752 字节），末尾带 Airflow 的 subcomponents 声明
  （JetBrains Mono、Plus Jakarta Sans 等字体走 OFL-1.1）
- `NOTICE`：`Apache Airflow / Copyright 2016-2026 The Apache Software Foundation`
- GitHub 识别：`Apache-2.0`

### 2.2 本项目的合规策略

miniflow 是**独立实现（clean-room）**，不复制 Airflow 源码。这样做有两个好处：

1. 避免 Apache-2.0 第 4(b) 条「修改文件必须显著标注」的义务落到每个文件上；
2. 避免把 Airflow 的 subcomponents（字体等）义务误带进来。

但**仍然**履行了归属义务，因为「概念与 API 形状的借鉴」是事实：

| 义务 | 落点 |
|---|---|
| 保留许可证 | `LICENSE`（标准 Apache-2.0 全文，版权行改为本项目） |
| 声明归属 | `NOTICE`：明确写出灵感来源、上游版权、以及「未复制源码」 |
| 商标不混淆 | `NOTICE`：声明 Apache/Airflow 是 ASF 商标，本项目与 ASF 无隶属关系，`miniflow` 不是 Airflow 改名 |
| 不暗示背书 | `README` 第 9 节与 `NOTICE` 双重声明 |

**没有做的事**（也不该做）：把 Airflow 的 LICENSE 原样拷过来当自己的——那会把 ASF 的版权声明变成对本项目的授权声明，是错的。

### 2.3 为什么排除另外三个候选

- **Dify**：附加条款限制多租户商用与品牌使用，做「衍生轻量版」会把限制继承过来，且条款解释权不在我这。
- **n8n**：Sustainable Use License 明确限制「转售/托管给第三方」，不是开源许可，无法合规再分发。
- **MinerU**：AGPL-3.0 要求网络服务也必须开源全部源码。做本地 CLI 工具虽然不触发网络条款，
  但一旦有人包一层 Web 服务就踩线，风险传导给下游用户。

---

## 3. 功能解剖：什么必须留，什么可以砍

判断标准只有一条：**「单人/小团队在这台机器上跑几个脚本」这个场景，缺了它还能不能用？**

### 3.1 不可缺少（核心）

| 能力 | 为什么不能砍 | miniflow 的实现 |
|---|---|---|
| 声明式 DAG | 没有它只是 shell 脚本 | `with DAG(...)` + `@task` |
| 依赖解析 | 手写顺序易错，且无法并行判断 | 拓扑排序（Kahn）+ 环检测 |
| 跨任务传值 | 否则任务只能靠临时文件/全局变量串联 | 参数名 = 上游 task_id，JSON 传递 |
| 定时触发 | 用户第一诉求就是「每天跑」 | 自研 5 字段 cron |
| 重试 | 无人值守的前提 | `retries` + `retry_delay` |
| 超时 | 一个卡死的任务会堵住整条链 | 子进程 + `kill` |
| 运行状态 | 否则无法回答「昨天跑成功了吗」 | SQLite（run / task_run / dag_state） |
| 日志 | 失败后唯一线索 | 每次尝试一个文件，含 stdout+stderr |
| 错误可诊断 | 报错看不懂等于不可用 | 15 个错误码 + 原因 + 修法 + 下一步命令 |

### 3.2 可以裁剪（成本高、场景里用不上）

| 上游能力 | 处理 | 理由 |
|---|---|---|
| Web UI | 不做 | 单人场景 CLI 更快；成本极高（前端 + REST + 认证 + 实时刷新） |
| 多 Executor（Celery/K8s） | 不做 | 分布式是另一个量级的复杂度，且引入 broker 依赖 |
| Provider 生态（数百个算子） | 不做 | 用户在自己的任务函数里 `import boto3` 就行，不需要中间层 |
| Connections / Variables 加密后端 | 不做 | 环境变量 + 配置文件足够；加密后端需要主密钥管理 |
| 插件系统 | 不做 | 直接 import 自己的模块，比写插件协议简单 |
| REST API | 用 `--json` 代替 | 脚本化需求 90% 是「读状态」，输出 JSON 就够 |
| SLA / 告警 | 不做 | 外部监控读 `runs --json` 或看退出码 |
| backfill | 不做 | 反而避开了 `catchup=True` 一上线跑几百个历史任务的坑 |
| 元数据库（Postgres/MySQL） | 换成 SQLite | 单机场景 SQLite 足够，且免部署、免迁移 |
| `L`/`W`/`#` cron 扩展 | 不做 | 支持一半不如明确拒绝，静默算错更难查 |

### 3.3 直接删除（与目标场景正交）

- **RBAC / 多租户**：没有服务端，没有用户概念
- **Dataset / 事件驱动调度**：和「定时」是两个正交模型
- **TaskGroup / 动态任务映射**：几十个任务的规模用不上
- **多调度器高可用**：单机场景 HA 无意义

---

## 4. 轻量版的关键设计决策

### 4.1 任务用子进程，不用线程

| 方案 | 优点 | 缺点 | 结论 |
|---|---|---|---|
| 同进程直接调用 | 最快（0 开销） | 任务崩溃拖垮调度器；超时无法真杀；全局状态污染 | 否 |
| 线程 | 快 | GIL 限制；`threading` 无法强杀；异常难隔离 | 否 |
| **子进程** | 崩溃隔离；超时可靠；状态干净 | 每任务 100~200ms 启动开销 | **采用** |

代价是真实的：3 个空任务跑 450ms，其中 420ms 是解释器启动。但真实任务通常以秒计，
这个固定成本可以接受；而「一个任务 OOM 把调度器干掉」是不可接受的。

### 4.2 传值用「参数名 = 上游 task_id」，不用 XCom

Airflow 的 `xcom_pull(task_ids="x")` 需要：

- 知道 XCom 这个概念
- 手动写 pull
- 类型受后端限制
- 错了要到运行时才发现

miniflow 的做法：

```python
@task
def transform(extract):   # 参数名即上游 id
    ...
```

好处是**加载时就能校验**：参数名对不上，`miniflow list` 就直接报 MF005 并告诉你该改成什么名字。
代价是不够灵活（不能按 key 取部分数据），但可以靠「任务返回什么就传什么」规避。

### 4.3 状态用 SQLite，不用 JSON 文件

JSON 方案需要自己处理：并发写、部分写入、原子替换、查询排序。
SQLite 是标准库，一行 `sqlite3.connect` 就有事务和索引，代价只是多一个 `.db` 文件。

### 4.4 cron 自研，不用 croniter

`croniter` 是 Airflow 的依赖之一。引入它，miniflow 就失去了「零依赖」这个最硬的卖点。
自己实现 5 字段解析约 200 行，还能做一件 croniter 不做的事：**明确拒绝不支持的语法**（MF009 会指出哪个字段错）。

### 4.5 默认不补历史

Airflow 的 `catchup=True` 是新手第一大坑：`start_date` 设在半年前，一上线就补跑 180 次。
miniflow 的调度游标从**首次启动时刻**开始，历史要靠手动 `run` 补。

这只是个默认值的选择，但它决定了「第一次部署会不会把数据库写爆」。

---

## 5. 实现结果与验证证据

### 5.1 规模

| 指标 | 数值 |
|---|---|
| 运行时依赖 | **0** |
| Python 源码 | 9 个模块，约 2,300 行（含注释与文档字符串） |
| 测试 | 72 个用例，约 15 秒跑完 |
| 文档 | README + 4 篇 docs + ANALYSIS |

### 5.2 真实可用的证据（不是"应该能跑"）

| 验证项 | 方法 | 结果 |
|---|---|---|
| 跨进程传值 | 3 任务链，下游拿上游 dict | `{"rows":12}` → `24` |
| 重试真的重跑 | 计数文件，前两次抛异常 | 文件内容 `3`，attempts=3 |
| 超时真的杀 | `time.sleep(10)` + `timeout=1` | 1~2 秒返回，MF007 |
| 失败阻断 | boom 失败 + 下游 + 无关分支 | 下游 `upstream_failed`，无关分支 `success` |
| 环检测 | `a >> b >> c >> a` | MF004 报 `a -> b -> c -> a`（方向与代码一致） |
| 调度器补跑 | 游标回退 5 分钟 | 触发一次，run_id 用计划时刻 |
| 一键体验 | `miniflow demo` | 生成 + 运行 + 打印下一步 |
| 真实示例 | CSV → 聚合 → Markdown | `合计 6402.00`（手工核对一致） |

### 5.3 测试有效性（变异检查）

只跑测试会陷入「全绿幻觉」。用 `tools/mutation_check.py` 注入 6 类缺陷：

| 注入的缺陷 | 检测结果 | 抓到它的用例 |
|---|---|---|
| cron 的 OR 改成 AND | DETECTED | `test_dom_and_dow_are_or` |
| 重试次数少一次 | DETECTED | 多个 |
| 超时丢掉 `timeout` 参数 | DETECTED | `test_timeout_kills_the_process` |
| 失败不再阻断下游 | DETECTED | `test_failed_upstream_blocks_downstream_but_not_siblings` |
| 下游不读上游输出 | DETECTED | `test_chain_passes_values_between_processes` |
| 校验阶段不报环 | DETECTED | `test_cycle_reports_arrow_direction` |

这个检查不是形式主义——开发过程中它**真的抓到一个 bug**：变异脚本被中断后，
`proc.wait()` 没有被恢复，`timeout` 参数丢失，任务不再超时。
是 `test_timeout_kills_the_process` 把它挡在了交付之前。

---

## 6. 已知风险与边界

| 风险 | 影响 | 缓解 |
|---|---|---|
| 单进程串行 | 多 DAG 同时到期会排队 | 文档写明；需要并发就用系统计划任务拆进程 |
| Windows `kill` 不递归 | 任务内的孙进程可能残留 | `TROUBLESHOOTING` 明确说明 |
| DAG 文件每次加载都执行 | 顶层副作用会重复触发 | 文档强调「顶层只放定义」；`doctor` 能暴露加载失败 |
| SQLite 不适合多机共享 | 网络文件系统上锁不可靠 | 文档写明「每台机器用自己的 home」 |
| 无 UI | 排障全靠 CLI | 错误码 + `logs` 子命令 + `--json` |
| 项目年轻 | 边界情况未覆盖 | 72 个测试 + 变异检查；CHANGELOG 列明已知限制 |

---

## 7. 复现本文所有数字

```bash
# 1. 依赖规模
pip install --dry-run --ignore-installed --report dep_report.json "apache-airflow==3.3.2"
python -c "import json;print(len(json.load(open('dep_report.json'))['install']))"

# 2. 核心包直接依赖
python -c "import json,urllib.request;d=json.load(urllib.request.urlopen('https://pypi.org/pypi/apache-airflow-core/json'));print(len([r for r in d['info']['requires_dist'] if 'extra ==' not in r]))"

# 3. Stars 与许可证
curl -s https://api.github.com/repos/apache/airflow | python -c "import json,sys;d=json.load(sys.stdin);print(d['stargazers_count'], d['license']['spdx_id'])"

# 4. 本项目测试
cd miniflow
python -m unittest discover -s tests -t .

# 5. 测试有效性
python tools/mutation_check.py
```
