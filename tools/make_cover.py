"""生成 README 顶部的封面图（1280x640，GitHub 社交预览尺寸）。

这是构建工具，不是运行时的一部分：需要 Pillow，但 miniflow 本身仍然零依赖。

    python tools/make_cover.py

输出：docs/assets/cover.png
"""

from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

# Windows 控制台默认 cp1252 / GBK，打印中文会抛 UnicodeEncodeError。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

WIDTH, HEIGHT = 1280, 640

BG_TOP = (10, 13, 18)
BG_BOTTOM = (20, 26, 34)
CARD = (13, 17, 23)
CARD_EDGE = (38, 46, 56)
TERM = (5, 8, 11)
TERM_EDGE = (30, 38, 48)
TILE = (17, 22, 29)
TILE_EDGE = (34, 42, 52)

TEXT = (230, 237, 243)
MUTED = (125, 137, 150)
DIM = (92, 102, 114)
GREEN = (63, 185, 80)
AMBER = (210, 153, 34)
BLUE = (88, 166, 255)

FONTS = Path("C:/Windows/Fonts")


def font(name: str, size: int, index: int = 0) -> ImageFont.FreeTypeFont:
    path = FONTS / name
    if path.suffix.lower() == ".ttc":
        return ImageFont.truetype(str(path), size, index=index)
    return ImageFont.truetype(str(path), size)


def vertical_gradient(draw: ImageDraw.ImageDraw) -> None:
    for y in range(HEIGHT):
        t = y / max(HEIGHT - 1, 1)
        color = tuple(int(a + (b - a) * t) for a, b in zip(BG_TOP, BG_BOTTOM))
        draw.line([(0, y), (WIDTH, y)], fill=color)


def main() -> int:
    image = Image.new("RGB", (WIDTH, HEIGHT), BG_TOP)
    draw = ImageDraw.Draw(image)
    vertical_gradient(draw)

    # ---- 外框卡片 ----
    draw.rounded_rectangle(
        [40, 36, WIDTH - 40, HEIGHT - 36], radius=18, fill=CARD, outline=CARD_EDGE, width=1
    )

    # ---- 窗口红绿灯 ----
    for index, color in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        cx = 78 + index * 24
        draw.ellipse([cx - 6, 70 - 6, cx + 6, 70 + 6], fill=color)

    # ---- 标题 + 徽标 ----
    title_font = font("consolab.ttf", 66)
    draw.text((72, 108), "miniflow", font=title_font, fill=TEXT)

    badge_font = font("consolab.ttf", 19)
    badge_text = "0 dependencies"
    badge_w = draw.textlength(badge_text, font=badge_font)
    bx0, by0 = 72 + draw.textlength("miniflow", font=title_font) + 24, 128
    draw.rounded_rectangle(
        [bx0, by0, bx0 + badge_w + 28, by0 + 36], radius=18, fill=(22, 34, 26), outline=(45, 78, 50)
    )
    draw.text((bx0 + 14, by0 + 8), badge_text, font=badge_font, fill=GREEN)

    # ---- 副标题 ----
    draw.text(
        (74, 196),
        "单进程、零第三方依赖的 DAG 调度器",
        font=font("msyh.ttc", 27, index=0),
        fill=(178, 190, 202),
    )
    draw.text(
        (74, 236),
        "the 5% of Apache Airflow you actually use, without the 130 packages",
        font=font("consola.ttf", 18),
        fill=MUTED,
    )

    # ---- 左：终端窗口 ----
    term_box = [72, 288, 752, 520]
    draw.rounded_rectangle(term_box, radius=12, fill=TERM, outline=TERM_EDGE, width=1)

    mono = font("consolab.ttf", 20)
    mono_re = font("consola.ttf", 20)
    x = term_box[0] + 26
    y = term_box[1] + 24
    line_h = 36

    draw.text((x, y), "$", font=mono, fill=DIM)
    draw.text((x + 22, y), "miniflow run daily_report", font=mono_re, fill=BLUE)
    y += line_h + 8

    for state, task, cost in (
        ("OK", "fetch", "179ms"),
        ("OK", "transform", "146ms"),
        ("OK", "report", "121ms"),
    ):
        draw.text((x + 22, y), state, font=mono, fill=GREEN)
        draw.text((x + 66, y), task, font=mono_re, fill=TEXT)
        draw.text((x + 210, y), cost, font=mono_re, fill=MUTED)
        y += line_h

    y += 12
    draw.text((x + 22, y), "成功", font=font("msyh.ttc", 19, index=0), fill=GREEN)
    draw.text((x + 74, y + 2), "daily_report__20261020T205102", font=mono_re, fill=(150, 162, 175))
    draw.text((x + 452, y + 2), "518ms", font=mono_re, fill=DIM)

    # ---- 右：三条关键数据 ----
    # 注意：数值部分用 Consolas 渲染，所以这里不能出现中文，
    # 否则会掉成豆腐块（Consolas 没有 CJK 字形）。
    tiles = [
        ("130 → 0", "第三方依赖", GREEN),
        ("5 → 1", "常驻进程", GREEN),
        ("WSL2 → native", "Windows 支持", AMBER),
    ]
    tile_x0, tile_x1 = 782, 1208
    tile_y = 288
    for big, label, color in tiles:
        draw.rounded_rectangle(
            [tile_x0, tile_y, tile_x1, tile_y + 64], radius=10, fill=TILE, outline=TILE_EDGE, width=1
        )
        draw.text((tile_x0 + 20, tile_y + 12), big, font=font("consolab.ttf", 28), fill=color)
        label_w = draw.textlength(label, font=font("msyh.ttc", 17, index=0))
        draw.text(
            (tile_x1 - 20 - label_w, tile_y + 24),
            label,
            font=font("msyh.ttc", 17, index=0),
            fill=MUTED,
        )
        tile_y += 78

    # ---- 底部信息条 ----
    foot_font = font("consola.ttf", 17)
    foot_cn = font("msyh.ttc", 17, index=0)
    fy = HEIGHT - 68
    draw.text((74, fy), "Apache-2.0", font=foot_font, fill=AMBER)
    draw.text((194, fy), "·", font=foot_font, fill=DIM)
    draw.text((212, fy), "Python 3.9+", font=foot_font, fill=MUTED)
    draw.text((336, fy), "·", font=foot_font, fill=DIM)
    draw.text((354, fy), "Windows / macOS / Linux", font=foot_font, fill=MUTED)

    tag = "Apache Airflow 的轻量替代"
    tag_w = draw.textlength(tag, font=foot_cn)
    draw.text((WIDTH - 74 - tag_w, fy), tag, font=foot_cn, fill=(104, 114, 126))

    out = Path(__file__).resolve().parent.parent / "docs" / "assets" / "cover.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    image.save(out, "PNG", optimize=True)
    print("生成 %s" % out)
    print("尺寸 %dx%d  %.1f KB" % (image.width, image.height, out.stat().st_size / 1024))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
