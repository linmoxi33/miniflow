"""错误体系。

设计原则（这是 miniflow 与重型调度器最主要的体验差异之一）：

1. 每个错误有**稳定编号**，可以拿去搜索、也可以写进脚本判断。
2. 每条消息回答四个问题：发生了什么、为什么、怎么修、下一步敲什么命令。
3. 退出码稳定，方便 CI 区分「用法错」「DAG 有问题」「任务本身失败」。

python 3.9+ / 只用标准库。
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Optional

# ---------------------------------------------------------------- 退出码约定
EXIT_OK = 0
EXIT_FAIL = 1        # 一般性失败
EXIT_USAGE = 2       # 命令行用法错
EXIT_VALIDATION = 3  # DAG 校验没过（成环、参数绑定、cron 非法）
EXIT_TASK = 4        # DAG 结构没问题，是任务本身跑失败了
EXIT_ENV = 5         # 环境/配置/状态库问题


@dataclass(frozen=True)
class ErrorSpec:
    code: str
    title: str
    exit_code: int


# 编号一旦发布就不再改变含义，只新增、不复用。
ERROR_SPECS = {
    "MF001": ErrorSpec("MF001", "找不到 DAG 目录", EXIT_ENV),
    "MF002": ErrorSpec("MF002", "DAG 文件加载失败", EXIT_VALIDATION),
    "MF003": ErrorSpec("MF003", "找不到指定的 DAG 或任务", EXIT_USAGE),
    "MF004": ErrorSpec("MF004", "依赖关系成环", EXIT_VALIDATION),
    "MF005": ErrorSpec("MF005", "任务参数无法绑定", EXIT_VALIDATION),
    "MF006": ErrorSpec("MF006", "任务返回值无法序列化", EXIT_TASK),
    "MF007": ErrorSpec("MF007", "任务执行超时", EXIT_TASK),
    "MF008": ErrorSpec("MF008", "任务执行失败", EXIT_TASK),
    "MF009": ErrorSpec("MF009", "cron 表达式非法", EXIT_VALIDATION),
    "MF010": ErrorSpec("MF010", "Python 版本过低", EXIT_ENV),
    "MF011": ErrorSpec("MF011", "配置文件非法", EXIT_ENV),
    "MF012": ErrorSpec("MF012", "状态库不可用", EXIT_ENV),
    "MF013": ErrorSpec("MF013", "DAG 定义写法有误", EXIT_VALIDATION),
    "MF014": ErrorSpec("MF014", "--param 参数格式不对", EXIT_USAGE),
    "MF015": ErrorSpec("MF015", "找不到这次运行记录", EXIT_USAGE),
}

MIN_PYTHON = (3, 9)


class MiniflowError(Exception):
    """所有可预期错误的统一载体。

    参数
    ----
    code:    错误编号，必须是 ERROR_SPECS 里的键
    message: 发生了什么（一句话，具体到值）
    reason:  为什么会这样（可省略）
    hint:    怎么修（可省略，但强烈建议给）
    next_cmd:下一步可以直接敲的命令（可省略）
    detail:  原始细节（traceback 尾部等），只在 --verbose 或日志里展开
    """

    def __init__(
        self,
        code: str,
        message: str,
        reason: Optional[str] = None,
        hint: Optional[str] = None,
        next_cmd: Optional[str] = None,
        detail: Optional[str] = None,
        exit_code: Optional[int] = None,
    ) -> None:
        if code not in ERROR_SPECS:
            raise KeyError("未登记的错误编号: %s" % code)
        self.code = code
        self.message = message
        self.reason = reason
        self.hint = hint
        self.next_cmd = next_cmd
        self.detail = detail
        self.spec = ERROR_SPECS[code]
        self.exit_code = exit_code if exit_code is not None else self.spec.exit_code
        super().__init__("%s %s: %s" % (self.code, self.spec.title, message))

    # ------------------------------------------------------------------ 渲染
    def render(self, verbose: bool = False, color: Optional[bool] = None) -> str:
        if color is None:
            color = _supports_color()
        red = "\033[31m" if color else ""
        dim = "\033[2m" if color else ""
        bold = "\033[1m" if color else ""
        reset = "\033[0m" if color else ""

        lines = []
        lines.append(
            "%s[%s]%s %s%s%s  %s"
            % (red, self.code, reset, bold, self.spec.title, reset, self.message)
        )
        if self.reason:
            lines.append("       原因    %s" % _indent(self.reason))
        if self.hint:
            lines.append("       怎么修  %s" % _indent(self.hint))
        if self.next_cmd:
            lines.append("       下一步  %s%s%s" % (dim, self.next_cmd, reset))
        if self.detail:
            if verbose:
                lines.append("       详情    %s" % _indent(self.detail, first_line_newline=True))
            else:
                first = str(self.detail).strip().splitlines()[0][:160]
                lines.append("       详情    %s   (加 --verbose 看完整内容)" % first)
        lines.append("       退出码  %d" % self.exit_code)
        return "\n".join(lines)


def _indent(text: str, first_line_newline: bool = False) -> str:
    text = str(text).rstrip()
    if first_line_newline:
        return "\n" + "\n".join("               " + ln for ln in text.splitlines())
    return text.replace("\n", "\n               ")


def _supports_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("MF_NO_COLOR"):
        return False
    try:
        return bool(sys.stderr.isatty())
    except Exception:
        return False


def die(err: MiniflowError, verbose: bool = False) -> "int":
    """打印错误并返回进程退出码。"""
    print(err.render(verbose=verbose), file=sys.stderr)
    return err.exit_code


def check_python_version() -> None:
    if sys.version_info < MIN_PYTHON:
        raise MiniflowError(
            "MF010",
            "当前 Python 是 %d.%d，miniflow 需要 %d.%d 或更高"
            % (
                sys.version_info[0],
                sys.version_info[1],
                MIN_PYTHON[0],
                MIN_PYTHON[1],
            ),
            reason="用到了 f-string 之外的标准库新行为（dataclasses / contextvars / sqlite3 的组合）。",
            hint="装一个 3.9+ 的 Python，或用 py -3.12 -m miniflow 指定解释器。",
            next_cmd="python --version",
        )
