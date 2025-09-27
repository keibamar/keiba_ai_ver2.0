import pandas as pd
import os
import sys
from glob import glob
import re
from datetime import date,datetime, timedelta
import calendar

# pycache を生成しない
sys.dont_write_bytecode = True
sys.path.append(r"C:\keiba_ai\keiba_ai_ver2.0\libs")
import name_header


def parse_filename(filename: str):
    """
    ファイル名から各変数を抽出する。
    形式: YYYYPPKKDDRR.csv
      YYYY = 年（4桁）
      PP   = 開催場ID（2桁）
      KK   = 開催数（2桁）
      DD   = 開催日（2桁）
      RR   = レース番号（2桁）
    """
    print(filename)
    base = os.path.splitext(os.path.basename(filename))[0]  # 拡張子を除いた部分
    if not re.match(r"^\d{12}$", base):
        raise ValueError(f"ファイル名の形式が不正です: {filename}")

    year = base[0:4]
    place_id = base[4:6]
    times = base[6:8]
    days = base[8:10]
    race_num = base[10:12]

    return {
        "year": int(year),
        "place_id": int(place_id),
        "times": int(times),
        "days": int(days),
        "race_num": int(race_num)
    }

def list_files_and_parse(input_dir : str):
    """
    指定ディレクトリ内のCSVファイルを走査し、
    拡張子を除いたファイル名と分解した変数を返す。
    """
    files = glob(os.path.join(input_dir, "*.csv"))
    results = []
    for f in files:
        try:
            parsed = parse_filename(f)
            results.append({"file": os.path.splitext(os.path.basename(f))[0], **parsed})
        except ValueError as e:
            print(e)
    return results

def format_date(date_str):
    """YYMMDD → YYYY/M/D"""
    dt = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    return dt

def csv_to_html(csv_path, output_path, date_str, race_num, place_id,  max_races):
    date_display = format_date(date_str)
    print(csv_path)
    try:
        df = pd.read_csv(csv_path)
        df = df[["枠", "馬番", "馬名", "性齢", "斤量", "騎手", "score", "rank"]]
    except Exception as e:
        print(e)
        return

    # --- HTMLのtbody部分を手動で構築 ---
    table_rows = ""
    for _, row in df.iterrows():
        table_rows += f"""
        <tr>
          <td>{int(row['枠'])}</td>
          <td>{int(row['馬番'])}</td>
          <td>{row['馬名']}</td>
          <td>{row['性齢']}</td>
          <td>{row['斤量']}</td>
          <td>{row['騎手']}</td>
          <td>{row['score']:.3f}</td>
          <td>{int(row['rank'])}</td>
        </tr>
        """

    # ナビゲーション
    nav_links = []
    nav_links.append(f'<a href="index.html">この日の一覧に戻る</a>')
    if race_num > 1:
        nav_links.append(f'<a href="{name_header.PLACE_LIST[place_id - 1]}R{race_num-1}.html">前のレースへ</a>')
    if race_num < max_races:
        nav_links.append(f'<a href="{name_header.PLACE_LIST[place_id - 1]}R{race_num+1}.html">次のレースへ</a>')
    nav_html = " | ".join(nav_links)

    html_content = f"""
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <title>{date_display} {name_header.NAME_LIST[place_id - 1]}競馬場 第{race_num}レース</title>
  <style>
    body {{
      font-family: sans-serif;
    }}
    .nav {{
      margin: 10px 0;
      padding: 5px;
      background: #f9f9f9;
    }}
    .nav a {{
      margin: 0 8px;
      text-decoration: none;
      color: blue;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
    }}
    th, td {{
      border: 1px solid #ddd;
      padding: 8px;
      text-align: center;
    }}
    th {{
      background-color: #f2f2f2;
      cursor: pointer;
    }}
    .waku-1 {{ background-color: white; }}
    .waku-2 {{ background-color: black; color: white; }}
    .waku-3 {{ background-color: red; color: white; }}
    .waku-4 {{ background-color: blue; color: white; }}
    .waku-5 {{ background-color: yellow; }}
    .waku-6 {{ background-color: green; color: white; }}
    .waku-7 {{ background-color: orange; }}
    .waku-8 {{ background-color: pink; }}
    .rank-1 {{ background-color: yellow; }}
    .rank-2 {{ background-color: lightblue; }}
    .rank-3 {{ background-color: orange; }}
    .score-high {{ color: red; }}
    .score-low {{ color: blue; }}
    .score-verylow {{ color: darkblue; }}
  </style>
</head>
<body>
  <div class="nav">{nav_html}</div>

  <h2>{date_display} {name_header.NAME_LIST[place_id - 1]}競馬場 第{race_num}レース</h2>

  <table id="raceTable">
    <thead>
      <tr>
        <th>枠</th>
        <th>馬番 ▼</th>
        <th>馬名</th>
        <th>性齢</th>
        <th>斤量</th>
        <th>騎手</th>
        <th>Score</th>
        <th>Rank ▼</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>

  <script>
    // 枠色・rank色・score色を反映
    document.addEventListener("DOMContentLoaded", () => {{
      const rows = document.querySelectorAll("#raceTable tbody tr");
      rows.forEach(row => {{
        const waku = parseInt(row.children[0].innerText);
        row.children[0].classList.add(`waku-${{waku}}`);
        row.children[1].classList.add(`waku-${{waku}}`);

        const rank = parseInt(row.children[7].innerText);
        if (rank === 1) row.children[7].classList.add("rank-1");
        if (rank === 2) row.children[7].classList.add("rank-2");
        if (rank === 3) row.children[7].classList.add("rank-3");

        const score = parseFloat(row.children[6].innerText);
        if (score >= 0.1) row.children[6].classList.add("score-high");
        if (score < 0 && score >= -1) row.children[6].classList.add("score-low");
        if (score < -1) row.children[6].classList.add("score-verylow");
      }});
    }});

    // ソート関数
    function sortTable(n) {{
      const table = document.getElementById("raceTable");
      let switching = true;
      let dir = "asc";
      let switchcount = 0;
      while (switching) {{
        switching = false;
        let rows = table.rows;
        for (let i = 1; i < (rows.length - 1); i++) {{
          let shouldSwitch = false;
          let x = rows[i].getElementsByTagName("TD")[n];
          let y = rows[i + 1].getElementsByTagName("TD")[n];
          if (dir === "asc") {{
            if (Number(x.innerText) > Number(y.innerText)) {{
              shouldSwitch = true;
            }}
          }} else if (dir === "desc") {{
            if (Number(x.innerText) < Number(y.innerText)) {{
              shouldSwitch = true;
            }}
          }}
          if (shouldSwitch) {{
            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
            switching = true;
            switchcount++;
            break;
          }}
        }}
        if (!switching && switchcount === 0 && dir === "asc") {{
          dir = "desc";
          switching = true;
        }}
      }}
    }}

    // ヘッダークリックでソート
    document.querySelectorAll("th")[1].onclick = () => sortTable(1);
    document.querySelectorAll("th")[7].onclick = () => sortTable(7);
  </script>
</body>
</html>
"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)



def make_index_page(date_str, output_dir, files_info_list):
    """
    開催日の index.html を生成する。
    開催コースごとに、レース一覧を表示する。
    """
    date_display = format_date(date_str)

    # place_id ごとにレースをまとめる
    place_dict = {}
    for info in files_info_list:
        pid = info["place_id"]
        race_num = info["race_num"]
        if pid not in place_dict:
            place_dict[pid] = []
        place_dict[pid].append(race_num)

    # HTMLを組み立て
    content = f"<h1>{date_display} 開催コース一覧</h1>\n"
    for pid, races in sorted(place_dict.items()):
        place_name = name_header.PLACE_LIST[pid - 1]
        content += f"<h2>{name_header.NAME_LIST[pid - 1]}競馬場</h2>\n<ul>\n"
        for r in sorted(races):
            content += f'  <li><a href="{place_name}R{r}.html">第{r}レース</a></li>\n'
        content += "</ul>\n"

    html = f"""
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <title>{date_display} 開催コース一覧</title>
  <link rel="stylesheet" href="../css/style.css">
</head>
<body>
  {content}
  <p><a href="../index.html">開催日一覧に戻る</a></p>
</body>
</html>
"""
    with open(os.path.join(output_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)


def make_global_index(output_dir="output"):
    """
    開催日一覧をカレンダー形式で表示する index.html を作成。
    dates: ["20250922", "20250928", ...] の形式
    """

    html = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <title>開催日カレンダー</title>
  <style>
    body {
      font-family: sans-serif;
      text-align: center;
    }
    #calendar {
      border-collapse: collapse;
      margin: 20px auto;
      width: 80%;
    }
    #calendar td, #calendar th {
      border: 1px solid #ccc;
      padding: 10px;
      width: 14%;
      height: 80px;
      vertical-align: top;
    }
    #calendar a {
      text-decoration: none;
      color: blue;
      font-weight: bold;
    }
    #calendar td {
      cursor: default;
    }
    #monthYear {
      font-size: 1.5em;
      margin: 0 10px;
    }
  </style>
</head>
<body>
  <h1>開催日カレンダー</h1>

  <div>
    <button id="prevMonth">←</button>
    <span id="monthYear"></span>
    <button id="nextMonth">→</button>
  </div>

  <table id="calendar"></table>

  <!-- 開催日一覧（Pythonで自動生成される） -->
  <script src="../js/racedays.js"></script>
  <!-- カレンダー描画用（固定で配置） -->
  <script src="../js/calendar.js"></script>
</body>
</html>
"""
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ index.html を生成しました: {output_dir}")

def make_race_days_js(dates, output_dir="output"):
    """
    dates: list of 開催日文字列 (例: ["20250922", "20250923"])
    """
    # 重複削除 & ソート
    unique_dates = sorted(set(dates))
    mapping = {d: f"{d}/index.html" for d in unique_dates}

    js_content = "const raceDays = " + str(mapping).replace("'", '"') + ";"

    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "raceDays.js"), "w", encoding="utf-8") as f:
        f.write(js_content)


if __name__ == "__main__":
    race_day = date.today() - timedelta(days=(7))
    day_str =  race_day.strftime("%Y%m%d")
    print(day_str)
    input_dir = f"../data/RaceCards/{day_str}"
    output_dir = f"races/{day_str}"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    files_info_list = list_files_and_parse(input_dir)
    # print(files_info_list)
    # 各日ごとにHTML生成
    for file_info in files_info_list:
        csv_path = input_dir + "/" + str(file_info['file']) + ".csv"
        csv_to_html(csv_path, os.path.join(output_dir, f"{name_header.PLACE_LIST[file_info['place_id'] - 1]}R{file_info['race_num']}.html"), str(day_str), file_info['race_num'], file_info['place_id'] , max_races=12 )
    make_index_page(day_str, output_dir, files_info_list)

    # # 開催日全体の index.html
    global_index_dir = "races"
    make_global_index( global_index_dir )
    js_dir = "js"
    make_race_days_js(day_str,js_dir)


