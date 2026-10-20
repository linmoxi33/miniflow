# 部署文档

miniflow 只有一种依赖：Python 3.9+。没有数据库服务、没有消息队列、没有前端构建。
所以「部署」这件事只剩三步：放文件、跑起来、让它按时间跑。

---

## 1. 前提检查

```bash
python --version
```

要求 3.9 或更高。检查不通过时 `miniflow` 会直接报 **MF010** 并退出。

如果同时装了多个 Python，用完整路径调用，别依赖 PATH 顺序：

```bash
# Windows
C:\Python313\python.exe -m miniflow --version

# Linux / macOS
/usr/bin/python3.12 -m miniflow --version
```

---

## 2. 三种部署形态

### 形态 A：手动跑（最简）

适合：每天想起来就点一下、或者用别人的调度器调你。

```bash
cd /opt/myflow
python -m miniflow run daily_report
```

退出码直接可用在 shell 的 `&&` / `||` 里：

```bash
python -m miniflow run daily_report || echo "失败了，去看日志"
```

### 形态 B：常驻调度进程

适合：一台长期开着的机器，DAG 不多（几十个以内）。

```bash
cd /opt/myflow
python -m miniflow schedule --interval 20
```

- `--interval` 是轮询间隔（秒），默认 20。DAG 的 cron 最小粒度是 1 分钟，20 秒足够。
- 首次启动只**登记**当前时刻，不会补跑历史（没有 `catchup` 的坑）。
- 每个 DAG 的调度游标存在 SQLite 里，进程重启不会重复触发同一时刻。
- `Ctrl+C` graceful 退出，正在跑的任务会等它跑完当前这一步。

> 单进程串行执行：同一时刻多个 DAG 到期时会排队。任务本身耗时长的场景，
> 请评估队列延迟；需要并发请用形态 C 拆到多个进程/机器。

### 形态 C：交给系统计划任务（最稳）

适合：不想维护常驻进程，或者机器会重启。

原理：让系统每分钟调一次 `schedule --once`，miniflow 只做「判断该不该跑」，
不该跑就立刻退出（耗时 < 1 秒）。

#### Windows（任务计划程序）

先用记事本确认路径，然后在**管理员命令提示符**里：

```bat
schtasks /Create /SC MINUTE /MO 1 /TN "miniflow tick" ^
  /TR "cmd /c cd /d E:\myflow && C:\Python313\python.exe -m miniflow schedule --once" ^
  /F
```

查看与删除：

```bat
schtasks /Query /TN "miniflow tick" /V /FO LIST
schtasks /Delete /TN "miniflow tick" /F
```

注意：

- `cd /d` 不能省，否则相对路径 `./dags` 会指到 `C:\Windows\System32`。
- Python 路径写绝对路径，任务计划的环境变量和你的终端不一样。
- 默认以当前用户运行；需要「不登录也跑」时，在任务属性里改成「不管用户是否登录都要运行」。

#### Linux / macOS（cron）

```cron
* * * * * cd /opt/myflow && /usr/bin/python3 -m miniflow schedule --once >> /var/log/miniflow.log 2>&1
```

#### systemd（长期常驻，推荐形态 B）

`/etc/systemd/system/miniflow.service`：

```ini
[Unit]
Description=miniflow DAG scheduler
After=network-online.target

[Service]
Type=simple
User=myflow
WorkingDirectory=/opt/myflow
ExecStart=/usr/bin/python3 -m miniflow schedule --interval 20
Restart=on-failure
RestartSec=10
# 让中文日志不乱码
Environment=PYTHONUTF8=1
Environment=PYTHONIOENCODING=utf-8

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now miniflow
sudo systemctl status miniflow
journalctl -u miniflow -f
```

---

## 3. 目录布局

推荐把「代码」和「状态」分开，这样更新代码不会碰到运行记录：

```
/opt/myflow/                 # 代码 + DAG
├── dags/
│   ├── daily_report.py
│   └── _helpers.py          # 下划线开头 = 辅助模块，不当 DAG 加载
├── miniflow.json
├── miniflow/                # 包源码（用 pip 安装时可不存在）
└── .miniflow/               # 状态与日志（建议单独备份）
    ├── miniflow.db
    └── logs/

/var/backups/miniflow/       # 状态备份目标
```

用 `--home` 把状态放到数据盘：

```bash
python -m miniflow --home /data/miniflow-state run daily_report
```

或在 `miniflow.json` 里写：

```json
{
  "dags_dir": "./dags",
  "home": "/data/miniflow-state"
}
```

---

## 4. 权限

只有一个要求：`home` 目录可写（SQLite 要建库、写日志）。

```bash
mkdir -p /data/miniflow-state
chown -R myflow:myflow /data/miniflow-state
```

Windows 上如果 `home` 放在 `C:\Program Files` 下面会失败（需要管理员权限），
换成 `%LOCALAPPDATA%\miniflow` 或 `D:\miniflow-state`。

`miniflow doctor` 会实际写一个探针文件来验证，不靠猜。

---

## 5. 日志与状态

| 位置 | 内容 | 是否可删 |
|---|---|---|
| `.miniflow/miniflow.db` | 运行记录、任务尝试、调度游标 | 删了会丢历史，且调度游标重置（重启后不补历史） |
| `.miniflow/logs/<run_id>/<task_id>/attempt-N.log` | 任务 stdout + stderr | 可删，`miniflow clean` 会按保留数清理 |
| `.miniflow/logs/.../attempt-N.spec.json` | 传给子进程的完整参数 | 排查「到底传了什么」时有用 |

清理：

```bash
python -m miniflow clean --keep 50 --force
```

备份（SQLite 建议用 `.backup` 而不是直接 `cp`，避免拷到写了一半的页）：

```bash
python -c "import sqlite3;s=sqlite3.connect('.miniflow/miniflow.db');d=sqlite3.connect('/var/backups/miniflow.db');s.backup(d);d.close();s.close()"
```

---

## 6. 升级

miniflow 的升级就是替换代码，**没有数据库迁移步骤**（schema 变更只做加列，不做破坏性修改）。

```bash
# 1. 先备份状态
python -c "import sqlite3;s=sqlite3.connect('.miniflow/miniflow.db');d=sqlite3.connect('/var/backups/miniflow-pre-upgrade.db');s.backup(d);d.close();s.close()"

# 2. 替换代码
unzip -o miniflow-0.2.0.zip -d /opt/myflow-new
mv /opt/myflow /opt/myflow-old
mv /opt/myflow-new /opt/myflow

# 3. 体检 + 回归测试
cd /opt/myflow
python -m miniflow doctor
python -m unittest discover -s tests -t .

# 4. 确认无误后删除旧版本
rm -rf /opt/myflow-old
```

---

## 7. 离线 / 内网分发

因为没有第三方依赖，分发就是「拷贝一个目录」：

```bash
cd miniflow
python -m unittest discover -s tests -t .     # 先在能跑测试的机器上验证
zip -r miniflow-0.1.0.zip . -x "*.pyc" -x "__pycache__/*" -x ".miniflow/*" -x ".smoke*/*"
```

目标机器上：

```bash
unzip miniflow-0.1.0.zip -d /opt/myflow
cd /opt/myflow
python -m miniflow doctor
python -m unittest discover -s tests -t .
```

不需要 pip、不需要联网、不需要 root。

---

## 8. 复现 README 里的上游数据

想自己核对「130 个包」「68 个直接依赖」这些数字：

```bash
# 完整依赖树（不需要真的安装）
pip install --dry-run --ignore-installed --report dep_report.json "apache-airflow==3.3.2"
python -c "import json;d=json.load(open('dep_report.json'));print(len(d['install']))"

# 核心包的直接依赖（非 extra）
pip download apache-airflow-core==3.3.2 --no-deps -d /tmp/af
python -c "import json,urllib.request;d=json.load(urllib.request.urlopen('https://pypi.org/pypi/apache-airflow-core/json'));print(len([r for r in d['info']['requires_dist'] if 'extra ==' not in r]))"

# Stars / 许可证
curl -s https://api.github.com/repos/apache/airflow | python -c "import json,sys;d=json.load(sys.stdin);print(d['stargazers_count'], d['license']['spdx_id'])"
```

---

## 9. 部署检查清单

- [ ] `python --version` ≥ 3.9
- [ ] `python -m miniflow doctor` 全部 OK（WARN 可接受）
- [ ] `python -m unittest discover -s tests -t .` 全绿
- [ ] `miniflow run <你的dag>` 手工跑通一次
- [ ] 日志文件里能看到任务的 print 输出
- [ ] 调度形态选定（手动 / 常驻 / 计划任务）并已生效
- [ ] `home` 目录已纳入备份
- [ ] 失败时的通知方式已确定（读 `miniflow runs --json` 或看退出码）
