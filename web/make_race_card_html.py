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
    date_display = format_date(date_str)

    # place_id ごとにレース番号をグルーピング
    place_races = {}
    for file_info in files_info_list:
        place_id = file_info['place_id']
        place_key = name_header.PLACE_LIST[place_id - 1]   # 英語キー (リンク用)
        place_name = name_header.NAME_LIST[place_id - 1]   # 日本語名 (表示用)
        race_num = file_info['race_num']

        if place_key not in place_races:
            place_races[place_key] = {"display": place_name, "races": []}
        place_races[place_key]["races"].append(race_num)

    # 横軸 = 開催場、縦軸 = レース番号 の表を構築
    place_keys = sorted(place_races.keys())
    max_races = max(len(data["races"]) for data in place_races.values())

    table_rows = ""
    for race_num in range(1, max_races + 1):
        row_cells = f"<th>{race_num}R</th>"
        for place_key in place_keys:
            races = place_races[place_key]["races"]
            if race_num in races:
                row_cells += f'<td><a href="{place_key}R{race_num}.html">{race_num}R</a></td>'
            else:
                row_cells += "<td>-</td>"
        table_rows += f"<tr>{row_cells}</tr>\n"

    # HTML生成
    html = f"""
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <title>{date_display} レース一覧</title>
  <link rel="stylesheet" href="../css/styles.css">
</head>
<body>
  <div class="nav"><a href="../index.html">開催日一覧に戻る</a></div>
  <h1>{date_display} レース一覧</h1>
  <table border="1">
    <thead>
      <tr><th>レース</th>{''.join(f'<th>{place_races[k]["display"]}競馬場</th>' for k in place_keys)}</tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>
</body>
</html>
"""

    os.makedirs(output_dir, exist_ok=True)
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

def add_race_day(js_file, new_day):
    """
    racedays.js に日付を追加する関数
    - js_file: racedays.js のファイルパス
    - new_day: 追加する日付 (例 "20250927")
    """

    # ファイルが存在しない場合 → 新規作成
    if not os.path.exists(js_file):
        with open(js_file, "w", encoding="utf-8") as f:
            f.write(f'window.racedays = [\n  "{new_day}"\n];\n')
        print(f"新規作成: {new_day} を追加しました")
        return

    # 既存内容を読み込み
    with open(js_file, "r", encoding="utf-8") as f:
        content = f.read()

    # raceDays の配列を探す
    if "window.racedays" not in content:
        raise ValueError("racedays.js に raceDays 配列が見つかりません")

    # すでに存在する場合は追加しない
    if new_day in content:
        print(f"{new_day} は既に登録済みです")
        return

    # 最後の "]" の直前に追加
    lines = content.strip().splitlines()
    new_content = []
    added = False
    for line in lines:
        if line.strip().startswith("]") and not added:
            # 前の要素の末尾に , がなければ追加
            if not new_content[-1].strip().endswith(","):
                new_content[-1] = new_content[-1] + ","
            new_content.append(f'  "{new_day}"')
            new_content.append("];")
            added = True
        else:
            new_content.append(line)

    # 書き戻し
    with open(js_file, "w", encoding="utf-8") as f:
        f.write("\n".join(new_content) + "\n")

    print(f"{new_day} を追加しました")

import os

def get_subfolders(path):
    """
    指定したフォルダ内にあるフォルダ名一覧を取得する関数
    :param path: 親フォルダのパス
    :return: サブフォルダ名のリスト
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"指定されたパスが存在しません: {path}")
    
    return [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]

# 使用例
if __name__ == "__main__":
    base_path = "../data/RaceCards/"  # 例: racesフォルダ配下の一覧を取得
    folders = get_subfolders(base_path)
    print(folders)
    for day_str in folders:
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
        # global_index_dir = "races"
        # make_global_index( global_index_dir )
        js_path = "js/raceDays.js"
        add_race_day(js_path, day_str)


