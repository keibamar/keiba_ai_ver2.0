import os
import csv
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

# -----------------------------
# 設定
# -----------------------------
TEMPLATE_DIR = "./templates"
OUTPUT_DIR = "../../../site/tracks/meeting"
THIS_MEETING_OUTPUT = "../../../site/tracks/meeting/this_meeting.html"

CSV_FILE = "../../../../texts/race_calendar/2025_race_calendar.csv"
CURRENT_YEAR = 2025

# 競馬場コード → 名前
TRACK_MAP = {
    1: "01_sapporo",
    2: "02_hakodate",
    3: "03_fukushima",
    4: "04_niigata",
    5: "05_tokyo",
    6: "06_nakayama",
    7: "07_chukyo",
    8: "08_kyoto",
    9: "09_hanshin",
    10: "10_kokura"
}

# -----------------------------
# Jinja2
# -----------------------------
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)
template = env.get_template("ai_meeting.html")   # ← あなたが作るテンプレート名


# -----------------------------
# CSV を開催単位でまとめる
# -----------------------------
def load_meetings():
    meetings = {}  # {(course, times): [ {date, day}, ... ]}

    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            month = int(row["month"])
            day = int(row["day"])
            course = int(row["course"])
            times = int(row["times"])
            days = int(row["days"])

            key = (course, times)

            if key not in meetings:
                meetings[key] = []

            meetings[key].append({
                "date": datetime(2025, month, day),
                "day": days
            })

    # 日付順にソート
    for key in meetings:
        meetings[key].sort(key=lambda x: x["date"])

    return meetings


# -----------------------------
# 開催ページ生成
# -----------------------------
def generate_meeting_pages():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    meetings = load_meetings()
    today = datetime.now()

    past_meetings = []
    future_meetings = []
    current_meeting = None

    # 開催を分類
    for (course, times), days in meetings.items():
        first_day = days[0]["date"]
        last_day = days[-1]["date"]

        if last_day < today:
            past_meetings.append((course, times, days))
        elif first_day <= today <= last_day:
            current_meeting = (course, times, days)
        else:
            future_meetings.append((course, times, days))

    # -----------------------------
    # 過去開催ページを生成
    # -----------------------------
    os.makedirs(os.path.join(OUTPUT_DIR, str(CURRENT_YEAR)), exist_ok=True)
    for course, times, days in past_meetings:
        track_name = TRACK_MAP[course]
        first_day = days[0]["date"]

        filename = f"{track_name}-{times}th.html"

        html = template.render(
            title=f"{track_name} {times}回 開催成績",
            track=track_name,
            times=times,
            days=days
        )

        with open(os.path.join(OUTPUT_DIR, str(CURRENT_YEAR), filename), "w", encoding="utf-8") as f:
            f.write(html)

        print(f"Generated: {filename}")

    # -----------------------------
    # 今開催ページを生成
    # -----------------------------
    if current_meeting:
        course, times, days = current_meeting
        track_name = TRACK_MAP[course]

        html = template.render(
            title=f"{track_name} {times}回（今開催）",
            track=track_name,
            times=times,
            days=days
        )

        with open(THIS_MEETING_OUTPUT, "w", encoding="utf-8") as f:
            f.write(html)

        print("Generated: this_meeting.html")


# -----------------------------
# 実行
# -----------------------------
if __name__ == "__main__":
    generate_meeting_pages()
