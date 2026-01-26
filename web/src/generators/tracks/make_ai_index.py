import os
import csv
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

# -----------------------------
# 設定
# -----------------------------
TEMPLATE_DIR = "./templates"
OUTPUT_DIR = "../../../site/tracks"
CSV_FILE = "../../../../texts/race_calendar/2025_race_calendar.csv"
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
CURRENT_YEAR = 2025
TODAY = datetime.now()


# -----------------------------
# Jinja2 環境
# -----------------------------
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)
template = env.get_template("ai_index.html")

# -----------------------------
# 開催情報を読み込む
# -----------------------------
def load_meetings():
    meetings = {}  # {(course, times): [ {date, day}, ... ]}

    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = 2025  # CSV が 2025 年のため
            month = int(row["month"])
            day = int(row["day"])
            course = int(row["course"])
            times = int(row["times"])
            days = int(row["days"])

            key = (year, course, times)

            if key not in meetings:
                meetings[key] = []

            meetings[key].append({
                "date": datetime(year, month, day),
                "day": days
            })

    # 日付順にソート
    for key in meetings:
        meetings[key].sort(key=lambda x: x["date"])

    return meetings


# -----------------------------
# 今年の開催リンクを生成
# -----------------------------
def get_current_year_meeting_links():
    meetings = load_meetings()
    links = []

    for (year, course, times), days in meetings.items():
        if year != CURRENT_YEAR:
            continue

        first_day = days[0]["date"]
        if first_day > TODAY:
            continue  # 未来の開催は除外
        
        track_name = TRACK_MAP[course]
        filename = f"{track_name}-{times}th.html"
        url = f"meeting/{year}/{filename}"

        links.append({
            "track": track_name,
            "times": times,
            "url": url,
            "period": f"{days[0]['date'].strftime('%Y/%m/%d')}〜{days[-1]['date'].strftime('%Y/%m/%d')}"
        })
    # ★ ここでソートを追加（競馬場名 → 開催回） 
    links.sort(key=lambda x: (x["track"], x["times"]))

    return links


# -----------------------------
# AIメインページ生成
# -----------------------------
def generate_ai_index():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    meeting_links = get_current_year_meeting_links()

    html = template.render(
        title="AI予想成績",
        current_year = CURRENT_YEAR,
        tracks=list(TRACK_MAP.values()),
        current_year_meetings=meeting_links
    )

    with open(os.path.join(OUTPUT_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print("Generated: ai/index.html")


# -----------------------------
# 実行
# -----------------------------
if __name__ == "__main__":
    generate_ai_index()

