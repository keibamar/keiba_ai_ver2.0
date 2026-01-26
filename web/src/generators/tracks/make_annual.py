
import os
from jinja2 import Environment, FileSystemLoader

# -----------------------------
# 設定
# -----------------------------
TEMPLATE_DIR = "./templates"
OUTPUT_DIR = "../../../site/tracks/annual"

CURRENT_YEAR = 2026

# 年別AI成績（後でAIロジックと接続）
annual_data = {
    2026: {"hit_rate": 40.2, "return_rate": 92.1},
    2025: {"hit_rate": 38.2, "return_rate": 88.1},
    2024: {"hit_rate": 36.5, "return_rate": 82.4},
    2023: {"hit_rate": 34.1, "return_rate": 79.2},
}

# 開催別成績（例）
meeting_list = {
    2026: [
        {"file": "2026-01-1st", "name": "中山1回1日〜2日", "period": "2026/01/05〜2026/01/06"},
    ],
    2025: [
        {"file": "2025-01-1st", "name": "京都1回1日〜2日", "period": "2025/01/12〜2025/01/13"},
    ],
}

# -----------------------------
# Jinja2 環境
# -----------------------------
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)
template = env.get_template("ai_annual.html")

# -----------------------------
# 年間ページ生成
# -----------------------------
def generate_annual_page(year):
    os.makedirs(os.path.join(OUTPUT_DIR), exist_ok=True)

    html = template.render(
        title=f"{year}年 AI年間成績",
        year=year,
        annual=annual_data.get(year, {"hit_rate": 0, "return_rate": 0}),
        meetings=meeting_list.get(year, []),
        all_years=sorted(annual_data.keys(), reverse=True)
    )

    output_path = os.path.join(OUTPUT_DIR, f"{year}.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Generated: ai/annual/{year}.html")

# -----------------------------
# annual.html → 今年のページとして生成
# -----------------------------
def generate_current_annual():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    html = template.render(
        title="AI年間成績",
        year=CURRENT_YEAR,
        annual=annual_data[CURRENT_YEAR],
        meetings=meeting_list.get(CURRENT_YEAR, []),
        all_years=sorted(annual_data.keys(), reverse=True)
    )

    with open(os.path.join(OUTPUT_DIR, "annual.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print("Generated: ai/annual.html")

# -----------------------------
# 実行
# -----------------------------
if __name__ == "__main__":
    generate_current_annual()

    # 全年分のページを生成
    for y in annual_data.keys():
        generate_annual_page(y)
