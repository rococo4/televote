"""Render architecture.png from the repository root or any working directory (Pillow)."""

from math import atan2, cos, hypot, sin
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


WIDTH, HEIGHT = 1512, 810
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


label(44, 43, "Архитектура телеголосования", 39, ink, True)
label(45, 88, "100 млн голосов за 60 секунд", 23, muted)

group((40, 126, 1472, 480), "#fafbfd", "#d5dce2",
      "Приём во время эфира",
      "Расчётный пик с повторами — 3,67 млн POST/с. При совпадении с пиком GET — 7 млн HTTP-запросов/с.", ink)
group((40, 508, 875, 784), "#f5f8fd", "#cbdbea",
      "Создание опроса и просмотр результатов",
      "Редактор задаёт вопрос и границы окна голосования.", cold)
group((899, 508, 1472, 784), "#f3faf8", "#bdded6",
      "Подсчёт после закрытия",
      "Цель — опубликовать результат в течение 5 минут.", tally)

# Public route handlers are roles of the same Go application.
rectangle((526, 211, 941, 424), "#ffffff", "#82929e", 14)
label(733, 234, "Пул экземпляров Go API", 22, ink, True, "mm")
arrow([(205, 321), (253, 321)], muted)
line([(459, 321), (491, 321)], muted)
arrow([(491, 321), (491, 289), (546, 289)], cold)
arrow([(491, 321), (491, 377), (546, 377)], hot)
arrow([(921, 377), (1092, 377)], hot)

node((62, 280, 205, 361), "Зритель", "браузер", cold)
node((253, 280, 459, 361), "Балансировщик", "HTTPS", muted, large=22)
node((546, 255, 921, 325), "GET /p/{id}", "Форма опроса из памяти", cold, "#edf5fc")
node((546, 342, 921, 413), "POST /api/polls/{id}/votes",
     "202 после подтверждения Kafka", hot, "#fff3e1", large=22)
label(1007, 357, "событие", 17, hot, True, "mm")
label(1007, 399, "ключ зрителя", 17, hot, False, "mm")

rectangle((1092, 285, 1418, 416), "#fff3e1", hot, 12)
label(1255, 311, "Apache Kafka", 26, ink, True, "mm")
label(1255, 347, "Партиции журнала голосов", 19, muted, False, "mm")
label(1255, 383, "RF = 3   ·   min ISR = 2", 18, hot, False, "mm")

# Publish once before the broadcast; read bounded input only after it ends.
arrow([(467, 644), (467, 608), (725, 608), (725, 424)], cold, 2, True)
label(741, 452, "версия опроса до эфира", 16, cold)
arrow([(1418, 349), (1443, 349), (1443, 672), (1401, 672)], tally)

# Both types of aggregate are written to the same PostgreSQL store.
arrow([(227, 684), (294, 684)], cold)
arrow([(510, 684), (572, 684)], cold, twoway=True)
arrow([(963, 684), (853, 684)], tally)
label(908, 664, "итоги", 17, tally, True, "mm")

node((64, 644, 227, 725), "Редактор", "админка", cold)
node((294, 644, 510, 725), "Admin API", "опрос и отчёт", cold)
database((572, 626, 853, 741), "PostgreSQL", "опросы и агрегаты", cold, large=23, small=18)

rectangle((963, 597, 1401, 754), "#ffffff", tally, 13)
label(1182, 620, "Обработчики на Go", 23, ink, True, "mm")
label(1182, 650, "Чтение заданных диапазонов партиций", 18, muted, False, "mm")
label(1182, 678, "Последний допустимый голос по ключу зрителя", 18, muted, False, "mm")
draw.line([(982 * SCALE, 701 * SCALE), (1381 * SCALE, 701 * SCALE)],
          fill="#bdded6", width=2 * SCALE)
label(1182, 727, "Итог по каждой партиции и общий итог", 18, tally, False, "mm")

output = Path(__file__).resolve().parents[1] / "architecture.png"
canvas.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS).save(output, optimize=True)
