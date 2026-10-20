# 更新日志

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [0.1.0] - 2026-10-20

首个可用版本。目标不是「替代 Airflow」，而是把单机小规模编排这件事
从「装 130 个包 + 5 个常驻进程」压缩到「一条命令」。

### 新增

**核心运行时**

- `DAG` / `@task` 声明式定义，`>>` 表达依赖，链式与分叉均支持
- 拓扑排序（Kahn，同层按定义顺序，结果稳定可复现）
- 环检测，报出的路径方向与 `>>` 一致
- 参数按「参数名 = 上游 task_id」自动绑定；绑不上时给出可执行的修改建议
- 每个任务在独立子进程中执行，跨进程用 JSON 传值
- 重试（`retries` / `retry_delay`）、超时（`timeout`，真 kill 子进程）
- 失败任务的下游标记 `upstream_failed`，无关分支继续执行
- 只跑某任务及其上游（`--task`）

**调度**

- 自研 5 字段 cron 解析器：`*` / `a-b` / `*/n` / `a-b/n` / 枚举，6 个 `@` 别名
- Vixie cron 的 day-of-month 与 day-of-week **或**语义
- 闰年、月末缺日自动跳过
- 明确拒绝 Quartz 扩展（`L` / `W` / `#` / `?`）与 6 字段写法，不静默算错
- 常驻调度循环（`schedule`），轮询间隔可调
- `schedule --once`：交给系统计划任务，每次 < 1 秒
- 调度游标持久化，重启不重复触发；首次启动不补跑历史

**状态与日志**

- SQLite 状态库（标准库 `sqlite3`），自动建表，无迁移步骤
- 运行记录、逐次尝试、调度游标三张表
- 每次尝试一个日志文件（stdout + stderr + 元信息）
- 参数快照（`attempt-N.spec.json`）与结果文件，便于复现问题
- `clean --keep N` 按保留数清理记录与日志

**CLI**

- `init` / `list` / `show` / `run` / `runs` / `logs` / `schedule` / `cron` / `doctor` / `clean`
- `--dags` / `--home` / `--config` 放子命令前后都生效
- `list` / `show` / `runs` / `cron next` 支持 `--json`

**错误体系**

- 15 个稳定错误编号（MF001–MF015），每条含：发生了什么、为什么、怎么修、下一步命令
- 退出码区分「用法错 / DAG 校验失败 / 任务失败 / 环境问题」
- 单文件加载失败时直接给出该文件的具体建议，不被汇总信息覆盖
- 坏文件不会被静默跳过：能用的 DAG 照常列出，同时报错

**工程**

- 零第三方运行时依赖（`dependencies = []`），仅用标准库
- 72 个 `unittest` 用例，含真实子进程、真实超时、真实重试（原 68 个，补了 demo 与 3.9 兼容用例）
- 变异敏感度检查脚本（`tools/mutation_check.py`），验证测试真的能抓到缺陷
- 一键启动脚本（`start.cmd`，Windows）+ `miniflow demo` 子命令
- 交付包自检（`tools/verify_package.py`）与打包脚本（`tools/build_zip.py`）
- 封面图生成脚本（`tools/make_cover.py`，需要 Pillow，不属于运行时）
- GitHub Actions CI：Python 3.9 / 3.12 / 3.13 × Ubuntu / Windows 六组合，
  外加一个独立的变异检查 job
- Apache-2.0 许可证 + NOTICE 归属与商标说明

### 已知限制

- 单进程串行，无并发 / 分布式
- 无 Web UI、无 REST API
- 仅本机时区，无时区转换
- Windows 上超时 kill 不递归杀孙进程
- `timedelta` 类间隔无法用 cron 表达
- 每个任务固定 100~200ms 子进程启动开销

### 未实现（也不打算）

Web UI、RBAC、Connection 加密后端、Provider 插件生态、Dataset 事件调度、
SLA 告警、多调度器高可用。需要这些请用 Apache Airflow。
