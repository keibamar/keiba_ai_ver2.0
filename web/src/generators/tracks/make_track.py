import os
from jinja2 import Environment, FileSystemLoader

# -----------------------------
# 設定
# -----------------------------
TRACK_NAMES = [
    "01_sapporo", "02_hakodate", "03_fukushima", "04_niigata",
    "05_tokyo", "06_nakayama", "07_chukyo", "08_kyoto",
    "09_hanshin", "10_kokura"
]

# 競馬場ごとのコース情報のリスト
COURSE_LISTS =[ [["芝","1000"],["芝","1200"],["芝","1500"],["芝","1800"],["芝","2000"],["芝","2600"],
                 ["ダート","1000"],["ダート","1700"],["ダート","2400"]],                                                      # 01_sapporo
                [["芝","1000"],["芝","1200"],["芝","1800"],["芝","2000"],["芝","2600"],
                 ["ダート","1000"],["ダート","1700"],["ダート","2400"]],                                                      # 02_hakodate
                [["芝","1200"],["芝","1800"],["芝","2000"],["芝","2600"],
                 ["ダート","1150"],["ダート","1700"],["ダート","2400"]],                                                      # 03_fukushima
                [["芝","1000"],["芝","1200"],["芝","1400"],["芝","1600"],["芝","1800"],["芝","2000"],["芝","2200"],["芝","2400"],
                 ["ダート","1200"],["ダート","1800"],["ダート","2500"]],                                                      # 04_nigata
                [["芝","1400"],["芝","1600"],["芝","1800"],["芝","2000"],["芝","2300"],["芝","2400"],["芝","2500"],["芝","3400"],
                 ["ダート","1300"],["ダート","1400"],["ダート","1600"],["ダート","2100"],["ダート","2400"]],                   # 05_tokyo
                [["芝","1200"],["芝","1600"],["芝","1800"],["芝","2000"],["芝","2200"],["芝","2500"],["芝","3600"],
                 ["ダート","1200"],["ダート","1800"],["ダート","2400"],["ダート","2500"]],                                     # 06_nakayama
                [["芝","1200"],["芝","1400"],["芝","1600"],["芝","2000"],["芝","2200"],
                 ["ダート","1200"],["ダート","1400"],["ダート","1800"],["ダート","1900"]],                                     # 07_chukyo
                [["芝","1200"],["芝","1400"],["芝","1600"],["芝","1800"],["芝","2000"],["芝","2200"],["芝","2400"],["芝","3000"],["芝","3200"],
                 ["ダート","1200"],["ダート","1400"],["ダート","1800"],["ダート","1900"]],                                     # 08_kyoto
                [["芝","1200"],["芝","1400"],["芝","1600"],["芝","1800"],["芝","2000"],["芝","2200"],["芝","2400"],["芝","2600"],["芝","3000"],
                 ["ダート","1200"],["ダート","1400"],["ダート","1800"],["ダート","2000"]],                                     # 09_hanshin
                [["芝","1200"],["芝","1700"],["芝","1800"],["芝","2000"],["芝","2600"],
                 ["ダート","1000"],["ダート","1700"],["ダート","2400"]],                                                       # 10_kokura          
              ]

TEMPLATE_DIR = "./templates"
OUTPUT_DIR = "../../../site/tracks/course"

# -----------------------------
# Jinja2 環境
# -----------------------------
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)

track_template = env.get_template("track.html")
course_template = env.get_template("course.html")

# -----------------------------
# HTML生成関数
# -----------------------------
def generate_track_pages():
    for idx, track_name in enumerate(TRACK_NAMES):
        track_dir = os.path.join(OUTPUT_DIR, track_name)
        os.makedirs(track_dir, exist_ok=True)

        course_list = COURSE_LISTS[idx]

        # -----------------------------
        # 競馬場全体ページ（index.html）
        # -----------------------------
        track_html = track_template.render(
            track_name=track_name,
            courses=course_list
        )

        with open(os.path.join(track_dir, "index.html"), "w", encoding="utf-8") as f:
            f.write(track_html)

        # -----------------------------
        # 各コースページ
        # -----------------------------
        for course in course_list:
            surface = course[0]   # 芝 or ダート
            distance = course[1]  # 1000, 1200, ...

            filename = f"{surface}-{distance}.html".replace("芝", "turf").replace("ダート", "dirt")

            course_html = course_template.render(
                track_name=track_name,
                surface=surface,
                distance=distance
            )

            with open(os.path.join(track_dir, filename), "w", encoding="utf-8") as f:
                f.write(course_html)

        print(f"Generated: {track_name}")

# -----------------------------
# 実行
# -----------------------------
if __name__ == "__main__":
    generate_track_pages()
