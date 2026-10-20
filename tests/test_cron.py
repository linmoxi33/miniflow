"""cron 解析与推进的正确性测试。

这里只钉「可验证的事实」：给定一个时刻，下一次触发必须落在具体某天某分。
不测「格式化得好不好看」。
"""

from __future__ import annotations

import unittest
from datetime import datetime

from miniflow.cron import parse_cron
from miniflow.errors import MiniflowError


class CronNextTest(unittest.TestCase):
    def test_daily_three_am(self) -> None:
        schedule = parse_cron("0 3 * * *")
        self.assertEqual(
            schedule.next_after(datetime(2026, 1, 1, 4, 0)),
            datetime(2026, 1, 2, 3, 0),
        )

    def test_same_day_later_hour(self) -> None:
        schedule = parse_cron("30 22 * * *")
        self.assertEqual(
            schedule.next_after(datetime(2026, 6, 1, 10, 0)),
            datetime(2026, 6, 1, 22, 30),
        )

    def test_every_two_minutes(self) -> None:
        schedule = parse_cron("*/2 * * * *")
        self.assertEqual(
            schedule.next_after(datetime(2026, 10, 20, 20, 53)),
            datetime(2026, 10, 20, 20, 54),
        )

    def test_every_fifteen_minutes_crosses_hour(self) -> None:
        schedule = parse_cron("*/15 * * * *")
        self.assertEqual(
            schedule.next_after(datetime(2026, 10, 20, 20, 50)),
            datetime(2026, 10, 20, 21, 0),
        )

    def test_weekday_only(self) -> None:
        # 2026-10-23 是周五，下一个工作日是 10-26 周一
        schedule = parse_cron("0 9 * * 1-5")
        self.assertEqual(
            schedule.next_after(datetime(2026, 10, 23, 10, 0)),
            datetime(2026, 10, 26, 9, 0),
        )

    def test_sunday_zero_and_seven_are_same(self) -> None:
        a = parse_cron("0 0 * * 0")
        b = parse_cron("0 0 * * 7")
        start = datetime(2026, 10, 20, 12, 0)  # 周二
        self.assertEqual(a.next_after(start), b.next_after(start))
        self.assertEqual(a.next_after(start), datetime(2026, 10, 25, 0, 0))

    def test_leap_day(self) -> None:
        schedule = parse_cron("0 0 29 2 *")
        # 2026 不是闰年，下一个 2 月 29 日是 2028
        self.assertEqual(
            schedule.next_after(datetime(2026, 3, 1)),
            datetime(2028, 2, 29, 0, 0),
        )

    def test_month_end_skips_missing_days(self) -> None:
        schedule = parse_cron("0 0 31 * *")
        # 4 月没有 31 号，应该跳到 5 月 31
        self.assertEqual(
            schedule.next_after(datetime(2026, 4, 1)),
            datetime(2026, 5, 31, 0, 0),
        )

    def test_dom_and_dow_are_or(self) -> None:
        # 每月 1 号 或 每周一。从 2026-10-20（周二）起，先遇到 10-26（周一）
        schedule = parse_cron("0 0 1 * 1")
        self.assertEqual(
            schedule.next_after(datetime(2026, 10, 20, 12, 0)),
            datetime(2026, 10, 26, 0, 0),
        )

    def test_alias_daily(self) -> None:
        self.assertEqual(
            parse_cron("@daily").next_after(datetime(2026, 5, 5, 12, 0)),
            datetime(2026, 5, 6, 0, 0),
        )

    def test_upcoming_is_strictly_increasing(self) -> None:
        schedule = parse_cron("0 */6 * * *")
        runs = schedule.upcoming(datetime(2026, 10, 20, 7, 0), 4)
        self.assertEqual(len(runs), 4)
        for earlier, later in zip(runs, runs[1:]):
            self.assertLess(earlier, later)
        self.assertEqual(runs[0], datetime(2026, 10, 20, 12, 0))


class CronMatchTest(unittest.TestCase):
    def test_matches_exact_minute(self) -> None:
        schedule = parse_cron("0 3 * * *")
        self.assertTrue(schedule.matches(datetime(2026, 1, 1, 3, 0)))
        self.assertFalse(schedule.matches(datetime(2026, 1, 1, 3, 1)))

    def test_matches_respects_second_irrelevance(self) -> None:
        schedule = parse_cron("0 3 * * *")
        self.assertTrue(schedule.matches(datetime(2026, 1, 1, 3, 0, 59)))


class CronErrorTest(unittest.TestCase):
    def assert_cron_error(self, expr: str, *fragments: str) -> None:
        with self.assertRaises(MiniflowError) as ctx:
            parse_cron(expr)
        err = ctx.exception
        self.assertEqual(err.code, "MF009")
        for fragment in fragments:
            self.assertIn(fragment, err.render(verbose=True, color=False))

    def test_hour_out_of_range(self) -> None:
        self.assert_cron_error("0 25 * * *", "25", "小时")

    def test_month_zero(self) -> None:
        self.assert_cron_error("0 0 * 0 *", "0", "月")

    def test_wrong_field_count(self) -> None:
        self.assert_cron_error("0 3 * *", "5 个字段")

    def test_quartz_extension_rejected(self) -> None:
        self.assert_cron_error("0 3 L * *", "Quartz")

    def test_zero_step_rejected(self) -> None:
        self.assert_cron_error("*/0 * * * *", "步长")

    def test_reversed_range_rejected(self) -> None:
        self.assert_cron_error("0 5-1 * * *", "起点大于终点")

    def test_empty_expression(self) -> None:
        self.assert_cron_error("   ", "为空")

    def test_describe_is_human_readable(self) -> None:
        self.assertEqual(parse_cron("0 3 * * *").describe(), "每天 03:00")
        self.assertEqual(parse_cron("0 * * * *").describe(), "每小时整点")


if __name__ == "__main__":
    unittest.main()
