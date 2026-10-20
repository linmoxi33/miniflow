"""5 字段 cron 解析器（零依赖自研）。

支持
----
    *       任意值
    */5     步长
    1-5     区间
    1,3,5   枚举
    1-20/2  区间步长
    @hourly / @daily / @midnight / @weekly / @monthly / @yearly

不支持（并会明确报错，而不是静默算错）
--------------------------------------
    L / W / # / ?   这些是 Quartz 扩展，不属于标准 5 字段 cron
    6 字段（带秒）   miniflow 的最小调度粒度是 1 分钟

日与周的语义按 Vixie cron 标准：当 day-of-month 和 day-of-week 都**不是** *
时，两者是 **或** 关系（满足其一即触发），不是且。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Sequence, Set, Tuple

from .errors import MiniflowError

ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

# (字段名, 最小值, 最大值, 中文名)
FIELDS: Sequence[Tuple[str, int, int, str]] = (
    ("minute", 0, 59, "分钟"),
    ("hour", 0, 23, "小时"),
    ("day_of_month", 1, 31, "日"),
    ("month", 1, 12, "月"),
    ("day_of_week", 0, 7, "星期"),
)


@dataclass(frozen=True)
class _Field:
    values: Set[int]
    wildcard: bool

    def matches(self, value: int) -> bool:
        return value in self.values


@dataclass(frozen=True)
class CronSchedule:
    expr: str
    minute: _Field
    hour: _Field
    day_of_month: _Field
    month: _Field
    day_of_week: _Field

    # ------------------------------------------------------------ 匹配
    def matches(self, moment: datetime) -> bool:
        if not self.minute.matches(moment.minute):
            return False
        if not self.hour.matches(moment.hour):
            return False
        if not self.month.matches(moment.month):
            return False

        dom_ok = self.day_of_month.matches(moment.day)
        # cron 的星期是 0=周日；Python 的 weekday() 是 0=周一
        cron_dow = (moment.weekday() + 1) % 7
        dow_ok = self.day_of_week.matches(cron_dow)

        if self.day_of_month.wildcard and self.day_of_week.wildcard:
            return True
        if self.day_of_month.wildcard:
            return dow_ok
        if self.day_of_week.wildcard:
            return dom_ok
        # 两个都限定时是「或」，与 Vixie cron 一致
        return dom_ok or dow_ok

    # ------------------------------------------------------------ 下一次
    def next_after(self, after: datetime) -> datetime:
        """返回严格晚于 after 的下一个触发时刻（精确到分钟）。"""
        start = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        day = start.date()
        first_day = True
        hours = sorted(self.hour.values)
        minutes = sorted(self.minute.values)

        # 上限 8 年：足以覆盖 2 月 29 日这类稀疏表达式，又不会死循环
        for _ in range(366 * 8):
            if self._date_matches(day):
                if first_day:
                    day_start = start
                else:
                    day_start = datetime(day.year, day.month, day.day, 0, 0)
                for h in hours:
                    # 快速跳过：当天里已经过去的整点
                    for m in minutes:
                        candidate = datetime(day.year, day.month, day.day, h, m)
                        if candidate >= day_start:
                            return candidate
            day = day + timedelta(days=1)
            first_day = False

        raise MiniflowError(
            "MF009",
            "cron 表达式 %r 在 8 年内没有任何匹配时刻" % self.expr,
            reason="日期条件互相冲突，例如「2 月 30 日」。",
            hint="检查 day-of-month 与 month 的组合是否真实存在。",
            next_cmd='miniflow cron next "%s"' % self.expr,
        )

    def _date_matches(self, day) -> bool:
        if not self.month.matches(day.month):
            return False
        dom_ok = self.day_of_month.matches(day.day)
        cron_dow = (day.weekday() + 1) % 7
        dow_ok = self.day_of_week.matches(cron_dow)
        if self.day_of_month.wildcard and self.day_of_week.wildcard:
            return True
        if self.day_of_month.wildcard:
            return dow_ok
        if self.day_of_week.wildcard:
            return dom_ok
        return dom_ok or dow_ok

    def upcoming(self, after: datetime, count: int = 5) -> List[datetime]:
        out: List[datetime] = []
        cursor = after
        for _ in range(count):
            cursor = self.next_after(cursor)
            out.append(cursor)
        return out

    def describe(self) -> str:
        """把 cron 翻成一句人话，出现在 miniflow list / show 里。"""
        if self.expr == "* * * * *":
            return "每分钟"
        if self.expr == "0 * * * *":
            return "每小时整点"

        hours = sorted(self.hour.values)
        minutes = sorted(self.minute.values)
        date_is_any = self.day_of_month.wildcard and self.day_of_week.wildcard

        if date_is_any and len(hours) * len(minutes) <= 6:
            times = ["%02d:%02d" % (h, m) for h in hours for m in minutes]
            if len(times) == 1:
                return "每天 %s" % times[0]
            return "每天 " + "、".join(times)

        return "cron(%s)" % self.expr


def parse_cron(expr: str) -> CronSchedule:
    if expr is None or not str(expr).strip():
        raise MiniflowError(
            "MF009",
            "cron 表达式为空",
            hint="要手动触发就写 schedule=None；要定时就写 5 个字段，例如 \"0 3 * * *\"。",
            next_cmd='miniflow cron next "0 3 * * *"',
        )

    text = str(expr).strip()
    lowered = text.lower()
    if lowered in ALIASES:
        text = ALIASES[lowered]

    parts = text.split()
    if len(parts) != 5:
        raise MiniflowError(
            "MF009",
            "cron 表达式 %r 有 %d 个字段，标准 cron 需要 5 个" % (expr, len(parts)),
            reason="5 个字段依次是：分 时 日 月 星期。",
            hint='正确写法示例："0 3 * * *"（每天 03:00）；miniflow 不支持带秒的 6 字段写法。',
            next_cmd='miniflow cron next "0 3 * * *"',
        )

    parsed = {}
    for index, (name, lo, hi, cn) in enumerate(FIELDS):
        parsed[name] = _parse_field(parts[index], name, lo, hi, cn, index + 1, expr)

    # 星期 7 等价于 0（周日）
    dow = parsed["day_of_week"]
    if 7 in dow.values:
        dow = _Field(values=(dow.values - {7}) | {0}, wildcard=dow.wildcard)
        parsed["day_of_week"] = dow

    for name, _, _, _ in FIELDS:
        if not parsed[name].values:
            raise MiniflowError(
                "MF009",
                "cron 表达式 %r 的 %s 字段没有任何有效取值" % (expr, name),
                hint="检查步长是否大于区间长度，例如 5-3 或 */0。",
                next_cmd='miniflow cron next "0 3 * * *"',
            )

    return CronSchedule(
        expr=text,
        minute=parsed["minute"],
        hour=parsed["hour"],
        day_of_month=parsed["day_of_month"],
        month=parsed["month"],
        day_of_week=parsed["day_of_week"],
    )


def _parse_field(
    text: str,
    name: str,
    lo: int,
    hi: int,
    cn_name: str,
    position: int,
    full_expr: str,
) -> _Field:
    forbidden = set("LW#?")
    if any(ch in forbidden for ch in text.upper()):
        raise MiniflowError(
            "MF009",
            "cron 表达式 %r 的第 %d 个字段（%s）用了不支持的字符：%s"
            % (full_expr, position, cn_name, text),
            reason="L / W / # / ? 是 Quartz 的扩展，标准 5 字段 cron 没有这些语义。",
            hint="改用具体数字或区间，例如把「每月最后一个周五」写成明确日期。",
            next_cmd='miniflow cron next "0 3 * * *"',
        )

    wildcard = text == "*"
    values: Set[int] = set()

    for chunk in text.split(","):
        chunk = chunk.strip()
        if not chunk:
            raise MiniflowError(
                "MF009",
                "cron 表达式 %r 的第 %d 个字段（%s）里有空的枚举项"
                % (full_expr, position, cn_name),
                hint="逗号之间不能为空，去掉多余的逗号。",
                next_cmd='miniflow cron next "0 3 * * *"',
            )

        step = 1
        if "/" in chunk:
            base, _, step_text = chunk.partition("/")
            if not step_text.isdigit():
                raise MiniflowError(
                    "MF009",
                    "cron 表达式 %r 的第 %d 个字段（%s）步长不是整数：%r"
                    % (full_expr, position, cn_name, step_text),
                    hint="步长写成正整数，例如 */5 或 1-20/2。",
                    next_cmd='miniflow cron next "0 3 * * *"',
                )
            step = int(step_text)
            if step <= 0:
                raise MiniflowError(
                    "MF009",
                    "cron 表达式 %r 的第 %d 个字段（%s）步长必须大于 0，现在是 %d"
                    % (full_expr, position, cn_name, step),
                    hint="用 */1 表示不跳步，或直接去掉 /步长。",
                    next_cmd='miniflow cron next "0 3 * * *"',
                )
        else:
            base = chunk

        if base == "*":
            span_lo, span_hi = lo, hi
        elif "-" in base.lstrip("-"):
            a, _, b = base.partition("-")
            try:
                span_lo, span_hi = int(a), int(b)
            except ValueError:
                raise MiniflowError(
                    "MF009",
                    "cron 表达式 %r 的第 %d 个字段（%s）区间写法不对：%r"
                    % (full_expr, position, cn_name, base),
                    hint="区间写成 起始-结束，例如 1-5。",
                    next_cmd='miniflow cron next "0 3 * * *"',
                )
            if span_lo > span_hi:
                raise MiniflowError(
                    "MF009",
                    "cron 表达式 %r 的第 %d 个字段（%s）区间起点大于终点：%s-%s"
                    % (full_expr, position, cn_name, span_lo, span_hi),
                    hint="把小的写前面，例如 1-5；跨天需求请拆成两条枚举。",
                    next_cmd='miniflow cron next "0 3 * * *"',
                )
        else:
            if not base.lstrip("+").isdigit():
                raise MiniflowError(
                    "MF009",
                    "cron 表达式 %r 的第 %d 个字段（%s）不是数字：%r"
                    % (full_expr, position, cn_name, base),
                    hint="每个字段只能是 *, 数字, 区间, 步长四类写法。",
                    next_cmd='miniflow cron next "0 3 * * *"',
                )
            span_lo = span_hi = int(base)

        for value in (span_lo, span_hi):
            if value < lo or value > hi:
                raise MiniflowError(
                    "MF009",
                    "cron 表达式 %r 的第 %d 个字段（%s）取值 %d 超出范围 %d-%d"
                    % (full_expr, position, cn_name, value, lo, hi),
                    hint=_range_hint(name),
                    next_cmd='miniflow cron next "0 3 * * *"',
                )

        for value in range(span_lo, span_hi + 1, step):
            values.add(value)

    return _Field(values=frozenset(values), wildcard=wildcard)


def _range_hint(name: str) -> str:
    return {
        "minute": "分钟取 0-59；每 5 分钟写 */5。",
        "hour": "小时取 0-23，24 点要写成第二天的 0 点。",
        "day_of_month": "日取 1-31；注意月份没有 30/31 号时会自动跳过。",
        "month": "月取 1-12，不是 0-11。",
        "day_of_week": "星期取 0-7，0 和 7 都表示周日。",
    }.get(name, "检查取值范围。")


def next_runs(expr: str, after: datetime, count: int = 5) -> List[datetime]:
    """给 CLI 用的便捷函数。"""
    return parse_cron(expr).upcoming(after, count)


def _iter_values(field: _Field) -> Iterable[int]:
    return sorted(field.values)
