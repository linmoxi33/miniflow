"""把 CSV 汇总成日报。

比 hello_dag 更接近真实用法：读文件、聚合、写结果，全程只用标准库。

跑一次：
    cd miniflow
    python -m miniflow --dags examples/dags run daily_sales --param day=2026-10-20

看依赖图：
    python -m miniflow --dags examples/dags show daily_sales
"""

import csv
import pathlib

from miniflow import DAG, task

EXAMPLES_DIR = pathlib.Path(__file__).resolve().parent.parent
DATA_FILE = EXAMPLES_DIR / "data" / "sales.csv"
OUTPUT_DIR = EXAMPLES_DIR / "output"

with DAG(
    "daily_sales",
    schedule="0 8 * * *",
    retries=1,
    retry_delay=2,
    timeout=60,
    description="按天汇总销售 CSV，输出一份 Markdown 日报",
) as dag:

    @task
    def read_rows(day: str):
        """读取指定日期的原始行。day 由 --param day=YYYY-MM-DD 传入。"""
        if not DATA_FILE.exists():
            raise FileNotFoundError("找不到数据文件：%s" % DATA_FILE)

        # utf-8-sig 能吃掉 Excel 导出 CSV 时常见的 BOM。
        # 不加的话第一列列名会变成 '\ufeffdate'，row["date"] 直接 KeyError。
        with DATA_FILE.open(encoding="utf-8-sig", newline="") as handle:
            rows = [row for row in csv.DictReader(handle) if row["date"] == day]

        print("读取 %d 行（%s）" % (len(rows), day))
        return {"day": day, "rows": rows}

    @task
    def aggregate(read_rows):
        """按商品汇总数量与金额。"""
        totals = {}
        for row in read_rows["rows"]:
            item = row["item"]
            bucket = totals.setdefault(item, {"qty": 0, "amount": 0.0})
            bucket["qty"] += int(row["qty"])
            bucket["amount"] += float(row["unit_price"]) * int(row["qty"])

        total_amount = sum(b["amount"] for b in totals.values())
        print("聚合出 %d 个商品，合计 %.2f" % (len(totals), total_amount))
        return {
            "day": read_rows["day"],
            "items": totals,
            "total_amount": round(total_amount, 2),
        }

    @task
    def write_report(aggregate):
        """写一份 Markdown 日报。"""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        target = OUTPUT_DIR / ("sales-%s.md" % aggregate["day"])

        lines = [
            "# 销售日报 %s" % aggregate["day"],
            "",
            "| 商品 | 数量 | 金额 |",
            "|---|---:|---:|",
        ]
        for item in sorted(aggregate["items"]):
            bucket = aggregate["items"][item]
            lines.append("| %s | %d | %.2f |" % (item, bucket["qty"], bucket["amount"]))
        lines += ["", "**合计 %.2f**" % aggregate["total_amount"], ""]

        target.write_text("\n".join(lines), encoding="utf-8")
        print("已写出 %s" % target)
        return {"path": str(target)}

    read_rows >> aggregate >> write_report
