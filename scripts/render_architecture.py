"""Render architecture.png from the repository root or any working directory (Pillow)."""

from math import atan2, cos, hypot, sin
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


WIDTH, HEIGHT = 1512, 802
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
label(45, 88, '100 млн зрителей за 60 секунд', 23, muted)

group((40, 128, 1472, 519), '#f9fafb', '#d5dce2', 'Публичный API во время эфира',
      'При совпадении пиков — 3,33 млн GET/с и 3,67 млн POST/с. Суммарно 7 млн запросов/с.', ink)
group((40, 544, 955, 776), '#f5f8fd', '#cbdbea', 'Управление опросом',
      'До эфира редактор задаёт вопрос и окно голосования.', cold)
group((977, 544, 1472, 776), '#f3faf8', '#bdded6', 'Подсчёт после эфира',
      'Цель — итог в течение 5 минут.', tally)

# Both handlers live inside each API instance.
rectangle((555, 214, 986, 450), '#ffffff', '#82929e', 14)
label(770, 243, 'Пул экземпляров Go API', 23, ink, True, 'mm')
arrow([(220, 343), (276, 343)], muted)
line([(485, 343), (522, 343)], muted)
arrow([(522, 343), (522, 306), (578, 306)], cold)
arrow([(522, 343), (522, 398), (578, 398)], hot)
arrow([(963, 398), (1090, 398)], hot)
arrow([(1260, 355), (1260, 304)], hot)
arrow([(534, 658), (534, 505), (767, 505), (767, 450)], cold, 2, True)

node((64, 303, 220, 383), 'Зритель', 'браузер', cold, large=24)
node((276, 303, 485, 383), 'Балансировщик', 'HTTPS', muted, large=22)
node((578, 269, 963, 343), 'GET /p/{id}', 'Форма с вопросом из памяти', cold, '#edf5fc', large=24, small=19)
node((578, 361, 963, 435), 'POST /api/polls/{id}/votes', 'Проверка и запись бюллетеня', hot, '#fff3e1', large=23, small=19)
database((1090, 355, 1401, 441), 'PostgreSQL', 'пишущие шарды 1 … N', hot, '#fff3e1', 24, 19)
database((1090, 227, 1401, 304), 'Реплика каждого шарда', 'в другой зоне доступности', hot, '#fffbf4', 20, 17)
label(1025, 376, 'INSERT', 17, hot, True, 'mm')
label(1025, 419, 'по хешу', 17, hot, False, 'mm')
label(1244, 328, 'синхронная репликация', 17, hot, False, 'rm')
label(786, 477, 'параметры и карта шардов до эфира', 17, cold)

arrow([(251, 696), (344, 696)], cold)
arrow([(610, 696), (680, 696)], cold, twoway=True)
arrow([(1401, 398), (1442, 398), (1442, 696), (1383, 696)], tally)
arrow([(1050, 696), (935, 696)], tally)
label(991, 675, 'итог', 17, tally, True, 'mm')
label(1428, 632, 'итоги шардов', 17, tally, False, 'rm')

node((66, 657, 251, 735), 'Редактор', 'админка', cold)
node((344, 658, 610, 734), 'Admin API', 'создание и результаты', cold)
database((680, 651, 935, 741), 'PostgreSQL', 'опросы и итоги', cold, '#ffffff', 22, 18)
node((1050, 656, 1383, 736), 'Агрегатор', 'ожидает все шарды', tally, '#ffffff', 24, 19)

output = Path(__file__).resolve().parents[1] / "architecture.png"
canvas.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS).save(output, optimize=True)
