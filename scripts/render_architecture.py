"""Render architecture.png from the repository root or any working directory (Pillow)."""

from math import atan2, cos, hypot, sin
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


WIDTH, HEIGHT = 1512, 822
SCALE = 2
canvas = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), "white")
draw = ImageDraw.Draw(canvas)
ink, muted = "#182e41", "#465d6c"
hot, cold, tally = "#9b4b0e", "#315a84", "#16716a"

font_pairs = (
    ("/System/Library/Fonts/Supplemental/Arial.ttf", "/System/Library/Fonts/Supplemental/Arial Bold.ttf"),
    ("/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf", "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf"),
    ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
)
regular_font, bold_font = next(
    (pair for pair in font_pairs if all(Path(file).is_file() for file in pair)),
    ("Arial.ttf", "arialbd.ttf"),
)


def label(x, y, text, size=20, color=ink, bold=False, anchor="lm"):
    font = ImageFont.truetype(bold_font if bold else regular_font, round(size * SCALE))
    draw.text((round(x * SCALE), round(y * SCALE)), text, fill=color, font=font, anchor=anchor)


def rectangle(box, fill, stroke, radius):
    draw.rounded_rectangle(
        tuple(round(value * SCALE) for value in box),
        radius=radius * SCALE, fill=fill, outline=stroke, width=2 * SCALE,
    )


def node(box, title, subtitle, stroke=muted, fill="white", large=23, small=18):
    rectangle(box, fill, stroke, 13)
    x, y = (box[0] + box[2]) / 2, (box[1] + box[3]) / 2
    label(x, y - 11, title, large, ink, True, "mm")
    label(x, y + 15, subtitle, small, muted, False, "mm")


def database(box, title, subtitle, stroke, fill="white", large=23, small=17):
    rectangle(box, fill, stroke, 10)
    left, top, right, bottom = box
    y_rule = (top + 12) * SCALE
    draw.line([(left * SCALE, y_rule), (right * SCALE, y_rule)], fill=stroke, width=2 * SCALE)
    x, y = (left + right) / 2, (top + bottom) / 2
    label(x, y - 5, title, large, ink, True, "mm")
    label(x, y + 20, subtitle, small, muted, False, "mm")


def line(points, color, width=3, dashed=False):
    for (x1, y1), (x2, y2) in zip(points, points[1:]):
        dx, dy = x2 - x1, y2 - y1
        length = hypot(dx, dy)
        ux, uy = dx / length, dy / length
        starts = range(0, int(length) + 1, 13) if dashed else [0]
        for start in starts:
            end = min(length, start + 7) if dashed else length
            segment = [
                (int((x1 + ux * distance) * SCALE), int((y1 + uy * distance) * SCALE))
                for distance in (start, end)
            ]
            draw.line(segment, fill=color, width=SCALE * width)


def arrowhead(tip, previous, color):
    x, y = tip
    angle = atan2(y - previous[1], x - previous[0])
    back_x, back_y = x - 12 * cos(angle), y - 12 * sin(angle)
    side_x, side_y = 5 * sin(angle), 5 * cos(angle)
    points = [tip, (back_x - side_x, back_y + side_y), (back_x + side_x, back_y - side_y)]
    draw.polygon([(round(px * SCALE), round(py * SCALE)) for px, py in points], fill=color)


def arrow(points, color, width=3, dashed=False, twoway=False):
    line(points, color, width, dashed)
    arrowhead(points[-1], points[-2], color)
    if twoway:
        arrowhead(points[0], points[1], color)


def group(box, fill, outline, title, subtitle, color):
    rectangle(box, fill, outline, 17)
    x, y = box[:2]
    label(x + 23, y + 35, title, 25, color, True)
    label(x + 23, y + 65, subtitle, 18, muted)


label(44, 43, 'Архитектура телеголосования', 39, ink, True)
label(45, 89, '100 млн зрителей за 60 секунд', 23, muted)
group((40, 128, 1472, 536), '#fffaf2', '#ebd3a5', 'Приём голосов', 'Расчётный пик с повторами — 5,5 млн POST/с', hot)
group((40, 563, 960, 799), '#f5f8fd', '#cbdbea', 'Управление опросом', 'До эфира редактор задаёт вопрос и окно голосования.', cold)
group((980, 563, 1472, 799), '#f3faf8', '#bdded6', 'Подсчёт после эфира', 'Цель — итог в течение 5 минут.', tally)
publication_path = [(573, 679), (573, 549), (651, 549), (651, 366)]
line(publication_path, cold, 2, True)
arrow([(651, 348), (651, 294), (555, 294), (555, 272)], cold, 2, True)
arrow([(162, 325), (162, 242), (370, 242)], cold)
arrow([(254, 358), (370, 358)], hot)
arrow([(617, 358), (685, 358)], hot)
arrow([(951, 358), (1052, 358)], hot)
arrow([(1294, 406), (1294, 454)], hot)
arrow([(255, 719), (350, 719)], cold)
arrow([(617, 719), (689, 719)], cold, 3, False, True)
arrow([(1398, 362), (1435, 362), (1435, 722), (1378, 722)], tally)
arrow([(1051, 719), (942, 719)], tally)
label(259, 223, 'GET страницы', 17, cold, True, 'mm')
label(310, 338, 'POST', 17, hot, True, 'mm')
label(1002, 337, 'по хешу', 17, hot, True, 'mm')
label(1279, 429, 'синхронная репликация', 17, hot, False, 'rm')
label(672, 503, 'публикация формы до эфира', 17, cold, False, 'lm')
label(997, 698, 'итог', 17, tally, True, 'mm')
label(1420, 654, 'итоги шардов', 17, tally, False, 'rm')
node((68, 325, 254, 393), 'Зрители', 'браузеры', cold, '#ffffff')
node((370, 211, 617, 272), 'CDN', 'страница опроса', cold, '#ffffff')
node((370, 323, 617, 393), 'Балансировщик', 'HTTP', hot, '#ffefd4')
node((685, 323, 951, 393), 'Пул Go Vote API', 'локальная карта шардов', hot, '#ffefd4')
database((1052, 310, 1398, 406), 'PostgreSQL', 'пишущие шарды 1 … N', hot, '#ffefd4', 24, 20)
database((1065, 454, 1398, 519), 'Реплика каждого шарда', 'в другой зоне доступности', hot, '#fff8eb', 20, 17)
node((68, 682, 255, 755), 'Редактор', 'админка', cold)
node((350, 679, 617, 759), 'Admin API', 'создание и результаты', cold)
database((689, 673, 942, 763), 'PostgreSQL', 'опросы и итоги', cold, '#ffffff', 22, 18)
node((1051, 679, 1378, 760), 'Агрегатор', 'ожидает все шарды', tally, '#ffffff', 24, 19)

output = Path(__file__).resolve().parents[1] / "architecture.png"
canvas.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS).save(output, optimize=True)
