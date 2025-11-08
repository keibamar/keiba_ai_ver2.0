import os
import re
from pathlib import Path
import sys
import pandas as pd
from datetime import date, timedelta, datetime

# pycache を生成しない
sys.dont_write_bytecode = True

# web/src を import パスに追加（config パッケージを解決するため）
PROJECT_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # web/src
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

# プロジェクトルートを正しく計算して libs を追加（libs はプロジェクトルート直下）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))  # project root (keiba_ai_ver2.0)
LIBS_PATH = os.path.join(PROJECT_ROOT, "libs")
if LIBS_PATH not in sys.path:
    sys.path.insert(0, LIBS_PATH)

# libs を追加した後に名前ヘッダを import
import name_header
from get_race_id import get_daily_id
import html

try:
    from config.settings import RANK_COLORS, WAKU_COLORS
except Exception:
    # templates.py の定義名が異なる／未定義の場合のフォールバック
    from config import templates as templates_mod
    RANK_COLORS = getattr(templates_mod, "RANK_COLORS", {})
    WAKU_COLORS = getattr(templates_mod, "WAKU_COLORS", {})

from config.path import RACE_HTML_PATH, RACE_INFO_PATH, RACE_CARDS_PATH, RACE_RESULTS_PATH, RACE_RETURNS_PATH, RACE_CALENDAR_FOLDER_PATH, TIME_INFO_PATH, WEIGHT_INFO_PATH, PEDS_RESULTS_PATH, POPS_INFO_PATH, FRAME_INFO_PATH
from utils.format_data import format_date
from utils.format_data import merge_rank_score


def read_race_csv(date_str, target_id):
    """CSVを読み込んで必要列を返す。失敗時はNoneを返す"""
    csv_path = os.path.join(RACE_CARDS_PATH, date_str, f"{target_id}.csv")
    try:        
        df = pd.read_csv(csv_path)
        # 必要列のみ抽出（存在しない列があれば KeyError を投げるので保護）
        cols = ["枠", "馬番", "馬名", "性齢", "斤量", "騎手", "馬体重(増減)", "score", "rank"]
        existing = [c for c in cols if c in df.columns]
        df = df[existing]
        return df
    except Exception as e:
        print(f"CSV読み込み失敗: {csv_path} - {e}")
        return None

def read_peds_results_csv(path):
  """CSVを読み込んで返す。失敗時はNoneを返す"""
  if os.path.exists(path):
      return pd.read_csv(path)
  return pd.DataFrame()

def get_result_table(date_str, place_id, target_id) :
    year = date_str[:4]
    # result_csv = os.path.join(RACE_RESULTS_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_race_results.csv")
    # if not os.path.exists(result_csv):
    #     print(f"警告: レース結果ファイルが存在しません: {result_csv}")
    #     return pd.DataFrame()

    # df = pd.read_csv(result_csv, dtype=str, index_col=0)
    # df_race = df.loc[df.index == int(target_id)]

    # if df_race.empty:
    #     print(f"警告: 指定レースの結果データが存在しません: {target_id}")
    #     return pd.DataFrame()

    result_csv = os.path.join(RACE_RESULTS_PATH, name_header.PLACE_LIST[place_id - 1], year, f"{target_id}.csv")
    if not os.path.exists(result_csv):
      print(f"警告: レース結果ファイルが存在しません: {result_csv}")
      return pd.DataFrame()
    
    df_race = pd.read_csv(result_csv, dtype=str, index_col=0)
    return df_race.copy()

def get_returns_table(date_str, place_id, target_id) :
    year = date_str[:4]
    # result_csv = os.path.join(RACE_RETURNS_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_race_returns.csv")
    # if not os.path.exists(result_csv):
    #     print(f"警告: 配当結果ファイルが存在しません: {result_csv}")
    #     return pd.DataFrame()

    # df = pd.read_csv(result_csv, dtype=str, index_col=0)
    # df_race = df.loc[df.index == int(target_id)]
    # df_race.columns = ["式別", "馬番", "配当", "人気"]
    # # print(df_race)
    # if df_race.empty:
    #     print(f"警告: 指定レースの配当結果データが存在しません: {target_id}")
    #     return pd.DataFrame()
    returns_csv = os.path.join(RACE_RETURNS_PATH, name_header.PLACE_LIST[place_id - 1], year, f"{target_id}.csv")
    if not os.path.exists(returns_csv):
        print(f"警告: 配当結果ファイルが存在しません: {returns_csv}")
        return pd.DataFrame()
    df_race = pd.read_csv(returns_csv, dtype=str, index_col=0)
    return df_race.copy()

def get_race_info(year, place_id, target_id):
  race_info_path = os.path.join(RACE_INFO_PATH, name_header.PLACE_LIST[place_id - 1], year, f"{target_id}.csv")
  if os.path.exists(race_info_path):
      df_info = pd.read_csv(race_info_path, dtype=str)
      if not df_info.empty:
          race_type = str(df_info.iloc[0].get("race_type", ""))
          course_len = int(df_info.iloc[0].get("course_len", ""))
          ground_state = str(df_info.iloc[0].get("ground_state", ""))
          race_class = str(df_info.iloc[0].get("class", ""))

           # --- クラス表記を統一（全角数字 → 半角数字）---
          trans_table = str.maketrans("０１２３４５６７８９", "0123456789")
          race_class = race_class.translate(trans_table)
          return race_type, course_len, ground_state, race_class
      else:
          print("Failed Get Race Info:", target_id)
          return None, None, None, None
  else:
      print("No Race Info:", target_id)
      return None, None, None, None


def build_table_rows(df):
    """メインの出走表（csv側）から HTML の行文字列を作成"""
    if df is None or df.empty:
        return ""
    rows = ""
    for _, row in df.iterrows():
        # 安全に値を取り出す
        waku = int(row['枠']) if '枠' in row and pd.notna(row['枠']) else ""
        umaban = int(row['馬番']) if '馬番' in row and pd.notna(row['馬番']) else ""
        name = row.get('馬名', '')
        seirei = row.get('性齢', '')
        kinryo = row.get('斤量', '')
        jockey = row.get('騎手', '')
        body = row.get('馬体重(増減)', '')
        score = row.get('score', "")
        rank = row.get('rank', "")

        # score/rank 表示の整形
        try:
            score_fmt = f"{float(score):.3f}" if score != "" and pd.notna(score) else ""
        except Exception:
            score_fmt = str(score)

        try:
            rank_fmt = int(rank) if rank != "" and pd.notna(rank) else ""
        except Exception:
            rank_fmt = rank

        rows += f"""
        <tr>
          <td>{waku}</td>
          <td>{umaban}</td>
          <td>{html.escape(str(name))}</td>
          <td>{seirei}</td>
          <td>{kinryo}</td>
          <td>{jockey}</td>
          <td>{body}</td>
          <td>{score_fmt}</td>
          <td>{rank_fmt}</td>
        </tr>
        """
    return rows

def build_nav_html(output_dir, date_str, place_id, target_id):
    """前後レースリンクを作成して返す"""
    prev_link = ""
    next_link = ""
    race_info_path = os.path.join(RACE_CALENDAR_FOLDER_PATH , f"race_time_id_list/{date_str}.csv")
    if os.path.exists(race_info_path):
        df_info = pd.read_csv(race_info_path, dtype=str)
        df_info = df_info[df_info["race_id"].astype(str).str.startswith(str(target_id)[:10])]
        df_info = df_info.sort_values("race_id").reset_index(drop=True)
        race_ids = df_info["race_id"].astype(str).tolist()
        if str(target_id) in race_ids:
            idx = race_ids.index(str(target_id))
            # 出力ディレクトリ（ファイルの格納先）
            out_dir = output_dir
            # 前のレース
            if idx > 0:
                prev = df_info.iloc[idx - 1]
                prev_name = str(prev["race_name"])
                prev_num = int(str(prev["race_id"])[-2:])
                prev_file = f"{name_header.PLACE_LIST[place_id - 1]}R{prev_num}.html"
                if os.path.exists(os.path.join(out_dir, prev_file)):
                    prev_link = f'<a href="{prev_file}">← 前のレース（{prev_name}）</a>'
                else:
                    prev_link = f'<span class="disabled">← 前のレース（{prev_name}）</span>'

            # 次のレース
            if idx < len(df_info) - 1:
                nxt = df_info.iloc[idx + 1]
                nxt_name = str(nxt["race_name"])
                nxt_num = int(str(nxt["race_id"])[-2:])
                next_file = f"{name_header.PLACE_LIST[place_id - 1]}R{nxt_num}.html"
                if os.path.exists(os.path.join(out_dir, next_file)):
                    next_link = f'<a href="{next_file}">次のレース（{nxt_name}） →</a>'
                else:
                    next_link = f'<span class="disabled">次のレース（{nxt_name}） →</span>'
    nav_html = f"""
    <div class="nav">
      <a href="index.html">この日の一覧に戻る</a><br>
      <div class="subnav">
        {prev_link if prev_link else '<span class="disabled">← 前のレースなし</span>'}
        {next_link if next_link else '<span class="disabled">次のレースなし →</span>'}
      </div>
    </div>
    """
    return nav_html

def build_html_content(date_display, place_id, race_num, race_name, race_time, nav_html, table_rows, run_time_info, weight_info, peds_info, pops_info, frames_info, recent_html, result_table_html, payout_table_html):
    """HTMLテンプレートを返す"""
    race_time_display = f"{race_time[:2]}:{race_time[2:]}" if race_time else ""
    place_name = name_header.NAME_LIST[place_id - 1]
    return """
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <title>{date_display} {place_name}競馬場 第{race_num}R {race_name}</title>
  <style>
    body {{
      font-family: sans-serif;
      margin: 20px;
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
      font-weight: bold;
    }}
    .subnav {{
      margin-top: 5px;
    }}
    .subnav a {{
      margin-right: 10px;
    }}
    .disabled {{
      color: #aaa;
      margin-right: 10px;
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
    #payoutTable td.num {{
      text-align: right;
      padding-right: 10px;
      white-space: nowrap;
    }}
  </style>
</head>
<body>
  {nav_html}
  <h2>{date_display} </h2>
  <h2>{place_name}競馬場 第{race_num}R </h2>
  <h2>{race_name}</h2>
  <p>発走時刻: {race_time_display}</p>
  <table id="raceTable">
    <thead>
      <tr>
        <th>枠</th>
        <th>馬番 ▼</th>
        <th>馬名</th>
        <th>性齢</th>
        <th>斤量</th>
        <th>騎手</th>
        <th>馬体重</th>
        <th>Score</th>
        <th>Rank ▼</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>
  {run_time_info}
  {weight_info}
  {peds_info}
  {pops_info}
  {frames_info}
  {recent_html}
  {result_table_html}
  {payout_table_html}
  <script>
  document.addEventListener("DOMContentLoaded", () => {{
    // ======== スタイル設定部分 ========
    const rows = document.querySelectorAll("#raceTable tbody tr");
    rows.forEach(row => {{
      const waku = parseInt(row.children[0].innerText);
      row.children[0].classList.add(`waku-${{waku}}`);
      row.children[1].classList.add(`waku-${{waku}}`);
      const rank = parseInt(row.children[8].innerText);
      if (rank === 1) row.children[8].classList.add("rank-1");
      if (rank === 2) row.children[8].classList.add("rank-2");
      if (rank === 3) row.children[8].classList.add("rank-3");
      const score = parseFloat(row.children[7].innerText);
      if (score >= 0.1) row.children[7].classList.add("score-high");
      if (score < 0 && score >= -1) row.children[7].classList.add("score-low");
      if (score < -1) row.children[7].classList.add("score-verylow");
    }});
    // ======== ソート機能部分 ========
    const table = document.getElementById("raceTable");
    const headers = table.querySelectorAll("th");

    function getCellValue(tr, idx) {{
      const val = tr.children[idx].innerText.trim();
      return isNaN(val) ? val : Number(val);
    }}

    function clearSortIndicators() {{
      headers.forEach(th => {{
        const ind = th.querySelector(".sort-ind");
        if (ind) ind.textContent = "";
      }});
    }}

    function sortTable(colIndex, th) {{
      const tbody = table.tBodies[0];
      const rowsArray = Array.from(tbody.querySelectorAll("tr"));

      // 前回の状態を取得（デフォルト asc）
      const currentDir = th.dataset.sortDir === "asc" ? "desc" : "asc";
      th.dataset.sortDir = currentDir;

      // 他ヘッダの矢印をリセット
      headers.forEach(header => {{
        if (header !== th) header.dataset.sortDir = "";
      }});

      // ソート方向アイコン
      clearSortIndicators();
      let indicator = th.querySelector(".sort-ind");
      if (!indicator) {{
        indicator = document.createElement("span");
        indicator.classList.add("sort-ind");
        indicator.style.marginLeft = "6px";
        th.appendChild(indicator);
      }}
      // indicator.textContent = currentDir === "asc" ? "▲" : "▼";

      // ソート処理
      rowsArray.sort((a, b) => {{
        const A = getCellValue(a, colIndex);
        const B = getCellValue(b, colIndex);
        if (typeof A === "number" && typeof B === "number") {{
          return currentDir === "asc" ? A - B : B - A;
        }} else {{
          return currentDir === "asc"
            ? A.toString().localeCompare(B)
            : B.toString().localeCompare(A);
        }}
      }});

      // 並び替え反映
      rowsArray.forEach(r => tbody.appendChild(r));
    }}

    // ======== 対象列にクリックイベントを追加 ========
    [1, 8].forEach(idx => {{
      const th = headers[idx];
      if (th) {{
        th.style.cursor = "pointer";
        const indicator = document.createElement("span");
        indicator.classList.add("sort-ind");
        indicator.style.marginLeft = "6px";
        th.appendChild(indicator);
        th.addEventListener("click", () => sortTable(idx, th));
      }}
    }});
  }});
  </script>
</body>
</html>
""".format(
    date_display=date_display,
    place_name=place_name,
    race_num=race_num,
    race_name=race_name,
    race_time_display=race_time_display,
    nav_html=nav_html,
    table_rows=table_rows,
    run_time_info=run_time_info,
    weight_info=weight_info,
    peds_info = peds_info,
    pops_info = pops_info,
    frames_info = frames_info,
    recent_html = recent_html,
    result_table_html=result_table_html,
    payout_table_html=payout_table_html,
    )

def generate_result_table(df) :
    if df.empty:
        return "<p>レース結果データが見つかりません。</p>"
    
    result_rows = ""
    for _, row in df.iterrows():
        rank = row["着順"]
        waku = row.get("枠", row.get("枠番", None))
        umaban = row["馬番"]
        horse = html.escape(str(row["馬名"]))
        jockey = html.escape(str(row["騎手"]))
        horse_weight = row["馬体重"] if "馬体重" in row and pd.notna(row["馬体重"]) else ""
        time = row["タイム"]
        diff = row["着差"] if pd.notna(row["着差"]) else ""
        pop = str(int(float(row["人気"]))) if pd.notna(row["人気"]) else ""
        last_3f = row["上り"] if "上り" in row and pd.notna(row["上り"]) else ""
        race_position = row["通過"] if "通過" in row and pd.notna(row["通過"]) else ""
        odds = row["単勝"]
        score = row.get("score", "")
        pred_rank = row.get("rank", "")

        #  # --- 着順上位3頭色付け ---
        # rank_color = RANK_COLORS.get(rank, "#ffffff")
        # rank_style = f'background-color:{rank_color};'

        # --- 枠順背景色 ---
        waku_color = WAKU_COLORS.get(waku, "#ffffff")
        waku_style = f'background-color:{waku_color}; color:{"#fff" if waku in ["2","3","4","7"] else "#000"};'

        # --- 人気上位3頭色付け ---
        pop_color = RANK_COLORS.get(pop, "#ffffff")
        pop_style = f'background-color:{pop_color};'

         # --- Rank上位3頭色付け ---
        pred_rank_color = RANK_COLORS.get(str(pred_rank), "#ffffff")
        pred_rank_style = f'background-color:{pred_rank_color};'

        # --- score色付け ---
        score_color = "black"
        if score is not None:
          if (score >= 0.1):
              score_color = "red"
          if (score < 0 and score >= -1):
              score_color = "blue"
          if (score < -1):
              score_color = "dark_blue"
        score_style = f'color:{score_color};'

        # --- score の表示文字列（None対応）---
        score_str = f"{score:.3f}" if isinstance(score, (int, float)) else ""
        
        result_rows += f"""
        <tr>
            <td>{rank}</td>
            <td style="{waku_style}">{waku}</td>
            <td style="{waku_style}">{umaban}</td>
            <td>{horse}</td>
            <td>{jockey}</td>
            <td>{horse_weight}</td>
            <td>{time}</td>
            <td>{diff}</td>
            <td style="{pop_style}">{pop}</td>
            <td>{last_3f}</td>
            <td>{race_position}</td>
            <td>{odds}</td>
            <td style="{score_style}">{score_str}</td>
            <td style="{pred_rank_style}">{pred_rank}</td>
        </tr>
        """

    result_table = f"""
    <h2>レース結果</h2>
    <table id="resultTable">
      <thead>
        <tr>
          <th>着順</th><th>枠</th><th>馬番</th><th>馬名</th>
          <th>騎手</th><th>馬体重</th><th>タイム</th><th>着差</th>
          <th>人気</th><th>上り</th><th>通過</th>
          <th>単勝オッズ</th><th>score</th><th>Rank</th>
        </tr>
      </thead>
      <tbody>
        {result_rows}
      </tbody>
    </table>
    """
    return result_table

def generate_payout_table_html(df):
    """
    指定されたレースIDに対応する配当結果テーブルをHTML化して返す
    """
    if df.empty:
        return "<p>配当結果データが見つかりません。</p>"
    
    # # --- 配当金額を3桁区切りに整形 ---
    df["配当"] = df["配当"].apply(lambda x: f"{int(x):,}円")
    
     # --- 同じ式別でまとめる ---
    grouped = (
        df.groupby("式別", sort=False)
        .apply(
            lambda g: pd.Series({
                "馬番": "<br>".join(g["馬番"].astype(str)),
                "配当": "<br>".join(g["配当"].astype(str)),
                "人気": "<br>".join(g["人気"].astype(int).astype(str))
            })
        )
        .reset_index()
    )

     # --- HTML構築 ---
    rows_html = ""
    for _, row in grouped.iterrows():
        rows_html += f"""
        <tr>
          <td>{row['式別']}</td>
          <td>{row['馬番']}</td>
          <td class="num">{row['配当']}</td>
          <td>{row['人気']}</td>
        </tr>
        """
    payout_html = f"""
    <h2>配当結果</h2>
    <table id="payoutTable">
      <thead>
        <tr>
          <th>式別</th>
          <th>馬番</th>
          <th>配当</th>
          <th>人気</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
    """

    return payout_html

def generate_race_info(date_str, place_id, target_id):
    """ レース情報をcsvファイルから取得する"""
    year = date_str[:4]
    race_info_path = os.path.join(RACE_INFO_PATH, name_header.PLACE_LIST[place_id - 1], year, f"{target_id}.csv")
    if os.path.exists(race_info_path):
        df_info = pd.read_csv(race_info_path, dtype=str)
        if not df_info.empty:
            # 追加部分: コース情報の取得
            race_type = str(df_info.iloc[0].get("race_type", ""))
            course_len = str(df_info.iloc[0].get("course_len", ""))
            weather = str(df_info.iloc[0].get("weather", ""))
            ground_state = str(df_info.iloc[0].get("ground_state", ""))
            race_class = str(df_info.iloc[0].get("class", ""))
            course_info_text = f"{race_type}{course_len}m 天候:{weather} 馬場:{ground_state} クラス:{race_class}"
            return course_info_text
    return None

def generate_run_time_info(date_str, place_id, target_id) :
    """平均勝ち時計/先週の三着内時計/ 同コース/条件 上りタイム"""
    # --- レース情報の取得 ---
    year = date_str[:4]
    race_type, course_len, ground_state, race_class = get_race_info(year, place_id, target_id)
    if race_type == None and course_len == None and ground_state == None and race_class == None:
        return
    # --- パス設定 ---
    total_run_time_path = os.path.join(TIME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], "total_avg_time.csv")
    total_data_path     = os.path.join(TIME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], "total_wineer_time.csv")
    year_run_time_path  = os.path.join(TIME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_avg_time.csv")
    year_data_path      = os.path.join(TIME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_wineer_time.csv")
    # --- データ読み込み ---
    def read_csv_safe(path):
        if os.path.exists(path):
            return pd.read_csv(path)
        else:
            print(f"[warn] ファイルが見つかりません: {path}")
            return pd.DataFrame()

    total_run_df = read_csv_safe(total_run_time_path)
    total_data_df = read_csv_safe(total_data_path)
    year_run_df = read_csv_safe(year_run_time_path)
    year_data_df = read_csv_safe(year_data_path)

    # --- CSV読み込み（なければ空DataFrame）---
    def read_if_exists(path):
        if os.path.exists(path):
            return pd.read_csv(path, dtype=str)
        return pd.DataFrame()

    total_run_df = read_if_exists(total_run_time_path)
    total_data_df = read_if_exists(total_data_path)
    year_run_df = read_if_exists(year_run_time_path)
    year_data_df = read_if_exists(year_data_path)

    # --- 該当行取得関数 ---
    def get_row(df, cls):
        if df.empty:
            return None
        cond = (
            (df["race_type"] == race_type) &
            (df["course_len"].astype(str) == str(course_len)) &
            (df["ground_state"] == ground_state) &
            (df["class"] == cls)
        )
        sub = df[cond]
        if sub.empty:
            return None
        return sub.iloc[0]

    # --- 各行取得 ---
    # 勝ち時計: allクラス用
    year_all_time   = get_row(year_run_df, "all")
    year_class_time = get_row(year_run_df, race_class)
    total_all_time  = get_row(total_run_df, "all")
    total_class_time  = get_row(total_run_df, race_class)

    # 上り/通過: 各クラス用
    year_all_data  = get_row(year_data_df, "all")
    year_class_data  = get_row(year_data_df, race_class)
    total_all_data = get_row(total_data_df, "all")
    total_class_data = get_row(total_data_df, race_class)

    # --- HTML整形用ユーティリティ ---
    def fmt_avg_time_html(row):
        """勝ち時計(ms) → mm:ss.ms 形式"""
        if row is None or pd.isna(row.get("avg_time", None)) or row["avg_time"] == "":
            return "―"
        try:
            val = float(row["avg_time"])
            total_seconds = int(val // 1000)
            ms = int(val % 1000)
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            return f"{minutes}:{seconds:02d}.{ms:03d}"
        except:
            return str(row["avg_time"])

    def color_for_position(pos):
        """通過順位の色を決定"""
        try:
            pos = int(pos)
        except:
            return "black"
        if 1 <= pos <= 2:
            return "red"
        elif 3 <= pos <= 9:
            return "orange"
        elif 10 <= pos <= 16:
            return "deepskyblue"
        elif pos >= 17:
            return "blue"
        return "black"

    def fmt_passing_html(row):
        """通過列（複数）を整形"""
        if row is None:
            return "―"
        passes = [row[col] for col in row.index if col.startswith("通過") and pd.notna(row[col]) and row[col] != ""]
        if not passes:
            return "―"
        html_parts = []
        for p in passes:
            color = color_for_position(p)
            html_parts.append(f'<span style="color:{color}; font-weight:bold;">{p}</span>')
        return "-".join(html_parts)

    def fmt_last_html(row):
        """上りタイムを整形"""
        if row is None or "上り" not in row or pd.isna(row["上り"]) or row["上り"] == "":
            return "―"
        return f"{row['上り']}"

    # --- HTML整形 ---
    run_time_info_html = f"""
    <div id="runtimeInfo" style="margin: 20px 0; padding: 10px; border: 1px solid #ccc; background: #fafafa;">
      <h3>🏇 コース別平均タイム情報 ({race_type} {course_len}m {ground_state} {race_class})</h3>
      <table style="border-collapse: collapse; width: 100%; text-align: center;">
        <thead>
          <tr style="background: #f2f2f2;">
            <th>区分</th>
            <th>対象</th>
            <th>平均勝ち時計</th>
            <th>上り</th>
            <th>通過</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td rowspan="2">全クラス</td>
            <td>{year}年平均</td>
            <td>{fmt_avg_time_html(year_all_time)}</td>
            <td>{fmt_last_html(year_all_data)}</td>
            <td>{fmt_passing_html(year_all_data)}</td>
          </tr>
          <tr>
            <td>TOTAL平均</td>
            <td>{fmt_avg_time_html(total_all_time)}</td>
            <td>{fmt_last_html(total_all_data)}</td>
            <td>{fmt_passing_html(total_all_data)}</td>
          </tr>
          <tr>
            <td rowspan="2">{race_class}</td>
            <td>{year}年平均</td>
            <td>{fmt_avg_time_html(year_class_time)}</td>
            <td>{fmt_last_html(year_class_data)}</td>
            <td>{fmt_passing_html(year_class_data)}</td>
          </tr>
          <tr>
            <td>TOTAL平均</td>
            <td>{fmt_avg_time_html(total_class_time)}</td>
            <td>{fmt_last_html(total_class_data)}</td>
            <td>{fmt_passing_html(total_class_data)}</td>
          </tr>
        </tbody>
      </table>
    </div>
    """.strip()

    return run_time_info_html

def generate_weight_info(date_str, place_id, target_id):
  """
  勝ち馬の平均馬体重を取得して HTML を生成する。
  """
  # --- レース情報の取得 ---
  year = date_str[:4]
  race_type, course_len, ground_state, race_class = get_race_info(year, place_id, target_id)
  if race_type == None and course_len == None and ground_state == None and race_class == None:
      return

  # --- パス設定 ---
  total_weight_path = os.path.join(WEIGHT_INFO_PATH, name_header.PLACE_LIST[place_id - 1], "total_wineer_weight.csv")
  year_weight_path = os.path.join(WEIGHT_INFO_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_wineer_weight.csv")

  def read_if_exists(path):
      if os.path.exists(path):
          return pd.read_csv(path, dtype=str)
      return pd.DataFrame()

  total_df = read_if_exists(total_weight_path)
  year_df = read_if_exists(year_weight_path)

  def get_row(df, cls):
      if df.empty:
          return None
      cond = (
          (df["race_type"] == race_type) &
          (df["course_len"].astype(str) == str(course_len)) &
          (df["ground_state"] == ground_state) &
          (df["class"] == cls)
      )
      sub = df[cond]
      if sub.empty:
          return None
      return sub.iloc[0]

  year_all = get_row(year_df, "all")
  year_class = get_row(year_df, race_class)
  total_all = get_row(total_df, "all")
  total_class = get_row(total_df, race_class)

  def fmt_weight_html(row):
      """馬体重に色をつけて表示"""
      if row is None or "馬体重" not in row or pd.isna(row["馬体重"]) or row["馬体重"] == "":
          return "―"
      try:
          weight = float(row["馬体重"])
          if weight >= 500:
              color = "red"
          elif weight <= 450:
              color = "deepskyblue"
          else:
              color = "black"
          return f'<span style="color:{color}; font-weight:bold;">{weight:.1f}kg</span>'
      except:
          return str(row["馬体重"])

  # --- HTML生成 ---
  weight_info_html = f"""
  <div id="weightInfo" style="margin: 20px 0; padding: 10px; border: 1px solid #ccc; background: #fefefe;">
    <h3>🏇 コース別平均馬体重情報 ({race_type} {course_len}m {ground_state} {race_class})</h3>
    <table style="border-collapse: collapse; width: 100%; text-align: center;">
      <thead>
        <tr style="background: #f2f2f2;">
          <th>区分</th>
          <th>対象</th>
          <th>平均馬体重</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td rowspan="2">全クラス</td>
          <td>{year}年平均</td>
          <td>{fmt_weight_html(year_all)}</td>
        </tr>
        <tr>
          <td>TOTAL平均</td>
          <td>{fmt_weight_html(total_all)}</td>
        </tr>
        <tr>
          <td rowspan="2">{race_class}</td>
          <td>{year}年平均</td>
          <td>{fmt_weight_html(year_class)}</td>
        </tr>
        <tr>
          <td>TOTAL平均</td>
          <td>{fmt_weight_html(total_class)}</td>
        </tr>
      </tbody>
    </table>
  </div>
  """.strip()

  return weight_info_html

def generate_peds_result_html(date_str, place_id, target_id):
    """血統別成績（PedsResults）をHTMLで整形して返す"""
    # --- レース情報の取得 ---
    year = date_str[:4]
    race_type, course_len, ground_state, race_class = get_race_info(year, place_id, target_id)
    if race_type == None and course_len == None and ground_state == None and race_class == None:
        return
    
    total_path = os.path.join(PEDS_RESULTS_PATH, name_header.PLACE_LIST[place_id - 1], "Total", f"{race_type}_{course_len}m_{ground_state}.csv")
    year_path  = os.path.join(PEDS_RESULTS_PATH, name_header.PLACE_LIST[place_id - 1], str(year), f"{race_type}_{course_len}m_{ground_state}.csv")

    total_df = read_peds_results_csv(total_path)
    year_df = read_peds_results_csv(year_path)
    # --- どちらも空なら表示なし ---
    if total_df.empty and year_df.empty:
        return f"""
        <div class="peds-result-block"; style="margin: 20px 0; padding: 10px; border: 1px solid #ccc; background: #fefefe;">
          <h3>🐎 血統別成績 ({race_type} {course_len}m {ground_state})</h3>
          <p style="color:#888;">データが存在しません。</p>
        </div>
        """
    
     # --- クラス順保持 ---
    CLASS_ORDER = ["all", "未勝利", "新馬", "1勝クラス", "2勝クラス", "3勝クラス", "オープン"]
    # --- クラス列が存在する場合のみカテゴリ化 ---
    for df in [total_df, year_df]:
        if not df.empty and "クラス" in df.columns:
            df["クラス"] = pd.Categorical(df["クラス"], categories=CLASS_ORDER, ordered=True)

    def make_table_html(df, cls_name, title):
        """サブテーブル作成"""
        if df.empty or "クラス" not in df.columns:
            return f"<h4>{title}：データなし</h4>"

        sub = df[df["クラス"] == cls_name].copy()
        if sub.empty:
            return f"<h4>{title}：データなし</h4>"

        # 上位5件（1着多い順→2着→3着）
        sub = sub.sort_values(by=["1着", "2着", "3着"], ascending=False).head(5)
        sub["総数"] = sub[["1着", "2着", "3着", "着外"]].sum(axis=1)
        sub["勝率"] = (sub["1着"] / sub["総数"] * 100).round(1)
        sub["複勝率"] = ((sub["1着"] + sub["2着"] + sub["3着"]) / sub["総数"] * 100).round(1)

        rows = ""
        for _, r in sub.iterrows():
            stat = f"({int(r['1着'])},{int(r['2着'])},{int(r['3着'])},{int(r['着外'])})"
            rows += f"""
            <tr>
              <td>{r['血統']}</td>
              <td>{stat}</td>
              <td>{r['勝率']}%</td>
              <td>{r['複勝率']}%</td>
            </tr>"""

        html = f"""
        <h4>{title} </h4>
        <table class="peds-table">
          <thead>
            <tr><th>血統</th><th>成績(1,2,3,着外)</th><th>勝率</th><th>複勝率</th></tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        """
        return html

    # --- 出力HTML作成 ---
    html = f"""
    <div class="peds-result-block"; style="margin: 20px 0; padding: 10px; border: 1px solid #ccc; background: #fefefe;">
      <h3>🐎 血統別成績 ({race_type} {course_len}m {ground_state})</h3>

      {make_table_html(total_df, "all", f"全クラス 2019~{year}")}
      {make_table_html(total_df, race_class, f"{race_class} 2019~{year}")}
      {make_table_html(year_df, "all", f"全クラス {year}年")}
      {make_table_html(year_df, race_class, f"{race_class} {year}年")}
    </div>
    """

    return html

def generate_pops_info(date_str, place_id, target_id):
    """
    勝ち馬の平均人気と3着内平均人気を取得して HTML を生成する。
    """
    # --- レース情報の取得 ---
    year = date_str[:4]
    race_type, course_len, ground_state, race_class = get_race_info(year, place_id, target_id)
    if race_type == None and course_len == None and ground_state == None and race_class == None:
        return

    # --- パス設定 ---
    total_pops_path = os.path.join(POPS_INFO_PATH, name_header.PLACE_LIST[place_id - 1], "total_average_pops.csv")
    year_pops_path  = os.path.join(POPS_INFO_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_average_pops.csv")

    def read_if_exists(path):
        if os.path.exists(path):
          df = pd.read_csv(path, dtype=str)
          # class列の全角数字→半角数字を統一
          if "class" in df.columns:
              trans_table = str.maketrans("０１２３４５６７８９", "0123456789")
              df["class"] = df["class"].astype(str).apply(lambda x: x.translate(trans_table).strip())
          return df
        return pd.DataFrame()

    total_df = read_if_exists(total_pops_path)
    year_df = read_if_exists(year_pops_path)

    def get_row(df, cls):
        if df.empty:
            return None
        # クラス名を全角→半角に変換しておく
        trans_table = str.maketrans("０１２３４５６７８９", "0123456789")
        cls = str(cls).translate(trans_table).strip()
        cond = (
            (df["race_type"] == race_type) &
            (df["course_len"].astype(str) == str(course_len)) &
            (df["ground_state"] == ground_state) &
            (df["class"] == cls)
        )
        sub = df[cond]
        if sub.empty:
            return None
        return sub.iloc[0]

    year_all = get_row(year_df, "all")
    year_class = get_row(year_df, race_class)
    total_all = get_row(total_df, "all")
    total_class = get_row(total_df, race_class)

    # --- 表示フォーマット ---
    def fmt_pops_html(pops_value):
        """人気数値に色をつけて表示"""
        if pops_value is None or pops_value == "" or pd.isna(pops_value):
            return "―"
        try:
            pops = float(pops_value)
            if pops >= 12:
                color = "red"
            elif pops >= 6:
                color = "deepskyblue"
            else:
                color = "black"
            return f'<span style="color:{color}; font-weight:bold;">{pops:.1f}番人気</span>'
        except:
            return str(pops_value)

    # --- HTML生成 ---
    pops_info_html = f"""
    <div id="popsInfo" style="margin: 20px 0; padding: 10px; border: 1px solid #ccc; background: #fefefe;">
      <h3>🏇 コース別平均人気情報 ({race_type} {course_len}m {ground_state} {race_class})</h3>
      <table style="border-collapse: collapse; width: 100%; text-align: center;">
        <thead>
          <tr style="background: #f2f2f2;">
            <th>区分</th>
            <th>対象</th>
            <th>平均勝馬人気</th>
            <th>平均着内人気</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td rowspan="2">全クラス</td>
            <td>{year}年平均</td>
            <td>{fmt_pops_html(year_all["winner_pops"]) if year_all is not None else "―"}</td>
            <td>{fmt_pops_html(year_all["place_pops"]) if year_all is not None else "―"}</td>
          </tr>
          <tr>
            <td>TOTAL平均</td>
            <td>{fmt_pops_html(total_all["winner_pops"]) if total_all is not None else "―"}</td>
            <td>{fmt_pops_html(total_all["place_pops"]) if total_all is not None else "―"}</td>
          </tr>
          <tr>
            <td rowspan="2">{race_class}</td>
            <td>{year}年平均</td>
            <td>{fmt_pops_html(year_class["winner_pops"]) if year_class is not None else "―"}</td>
            <td>{fmt_pops_html(year_class["place_pops"]) if year_class is not None else "―"}</td>
          </tr>
          <tr>
            <td>TOTAL平均</td>
            <td>{fmt_pops_html(total_class["winner_pops"]) if total_class is not None else "―"}</td>
            <td>{fmt_pops_html(total_class["place_pops"]) if total_class is not None else "―"}</td>
          </tr>
        </tbody>
      </table>
    </div>
    """.strip()

    return pops_info_html

def generate_frame_horse_info(date_str, place_id, target_id):
    """
    勝ち馬と3着内馬の平均枠番・平均馬番を取得して HTML を生成する。
    """
    # --- レース情報の取得 ---
    year = date_str[:4]
    race_type, course_len, ground_state, race_class = get_race_info(year, place_id, target_id)
    if race_type is None:
        return ""

    # --- パス設定 ---
    total_frame_path = os.path.join(FRAME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], "total_average_frames.csv")
    total_top3_path  = os.path.join(FRAME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], "total_average_frames_top3.csv")
    year_frame_path = os.path.join(FRAME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_average_frames.csv")
    year_top3_path  = os.path.join(FRAME_INFO_PATH, name_header.PLACE_LIST[place_id - 1], f"{year}_average_frames_top3.csv")

    def read_if_exists(path):
        if os.path.exists(path):
            df = pd.read_csv(path, dtype=str)
            # class列の全角数字→半角数字を統一
            if "class" in df.columns:
                trans_table = str.maketrans("０１２３４５６７８９", "0123456789")
                df["class"] = df["class"].astype(str).apply(lambda x: x.translate(trans_table).strip())
            return df
        return pd.DataFrame()

    total_df = read_if_exists(total_frame_path)
    total_top3_df = read_if_exists(total_top3_path)
    year_df = read_if_exists(year_frame_path)
    year_top3_df = read_if_exists(year_top3_path)

    def get_row(df, cls):
        if df.empty:
            return None
        # クラス名の統一（全角→半角）
        trans_table = str.maketrans("０１２３４５６７８９", "0123456789")
        cls = str(cls).translate(trans_table).strip()
        cond = (
            (df["race_type"] == race_type)
            & (df["course_len"].astype(str) == str(course_len))
            & (df["ground_state"] == ground_state)
            & (df["class"] == cls)
        )
        sub = df[cond]
        if sub.empty:
            return None
        return sub.iloc[0]

    # --- 該当行取得 ---
    total_class = get_row(total_df, race_class)
    total_all   = get_row(total_df, "all")
    total_top3_class  = get_row(total_top3_df, race_class)
    total_top3_all    = get_row(total_top3_df, "all")

    year_class = get_row(year_df, race_class)
    year_all   = get_row(year_df, "all")
    year_top3_class  = get_row(year_top3_df, race_class)
    year_top3_all    = get_row(year_top3_df, "all")

    # --- HTML整形 ---
    def fmt_frame_color(value):
        """枠番の色付け：1〜2=赤、7〜8=青、それ以外=黒"""
        if value is None or value == "" or pd.isna(value):
            return "―"
        try:
            val = float(value)
            if val in [1, 2]:
                color = "red"
            elif val in [7, 8]:
                color = "deepskyblue"
            else:
                color = "black"
            return f'<span style="color:{color}; font-weight:bold;">{val:.2f}</span>'
        except:
            return str(value)

    def fmt_horse_color(value):
        """馬番の色付け：1〜4=赤、13〜18=青、それ以外=黒"""
        if value is None or value == "" or pd.isna(value):
            return "―"
        try:
            val = float(value)
            if 1 <= val <= 4:
                color = "red"
            elif 13 <= val <= 18:
                color = "deepskyblue"
            else:
                color = "black"
            return f'<span style="color:{color}; font-weight:bold;">{val:.2f}</span>'
        except:
            return str(value)

    # --- HTML生成 ---
    html = f"""
    <div id="frameHorseInfo" style="margin:20px 0; padding:10px; border:1px solid #ccc; background:#fefefe;">
      <h3>📊 枠番・馬番 平均情報 ({race_type} {course_len}m {ground_state} {race_class})</h3>
      <table style="border-collapse:collapse; width:100%; text-align:center;">
        <thead>
          <tr style="background:#f2f2f2;">
            <th>区分</th>
            <th>対象</th>
            <th>平均枠番(勝ち馬)</th>
            <th>平均馬番(勝ち馬)</th>
            <th>平均枠番(3着内)</th>
            <th>平均馬番(3着内)</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td rowspan="2">全クラス</td>
            <td>{year}年平均</td>
            <td>{fmt_frame_color(year_all["avg_frame"]) if year_all is not None else "―"}</td>
            <td>{fmt_horse_color(year_all["avg_horse"]) if year_all is not None else "―"}</td>
            <td>{fmt_frame_color(year_top3_all["avg_frame"]) if year_top3_all is not None else "―"}</td>
            <td>{fmt_horse_color(year_top3_all["avg_horse"]) if year_top3_all is not None else "―"}</td>
          </tr>
          <tr>
            <td>TOTAL平均</td>
            <td>{fmt_frame_color(total_all["avg_frame"]) if total_all is not None else "―"}</td>
            <td>{fmt_horse_color(total_all["avg_horse"]) if total_all is not None else "―"}</td>
            <td>{fmt_frame_color(total_top3_all["avg_frame"]) if total_top3_all is not None else "―"}</td>
            <td>{fmt_horse_color(total_top3_all["avg_horse"]) if total_top3_all is not None else "―"}</td>
          </tr>
          <tr>
            <td rowspan="2">{race_class}</td>
            <td>{year}年平均</td>
            <td>{fmt_frame_color(year_class["avg_frame"]) if year_class is not None else "―"}</td>
            <td>{fmt_horse_color(year_class["avg_horse"]) if year_class is not None else "―"}</td>
            <td>{fmt_frame_color(year_top3_class["avg_frame"]) if year_top3_class is not None else "―"}</td>
            <td>{fmt_horse_color(year_top3_class["avg_horse"]) if year_top3_class is not None else "―"}</td>
          </tr>
          <tr>
            <td>TOTAL平均</td>
            <td>{fmt_frame_color(total_class["avg_frame"]) if total_class is not None else "―"}</td>
            <td>{fmt_horse_color(total_class["avg_horse"]) if total_class is not None else "―"}</td>
            <td>{fmt_frame_color(total_top3_class["avg_frame"]) if total_top3_class is not None else "―"}</td>
            <td>{fmt_horse_color(total_top3_class["avg_horse"]) if total_top3_class is not None else "―"}</td>
          </tr>
        </tbody>
      </table>
    </div>
    """.strip()

    return html

def generate_recent_same_condition_html(date_str, place_id, target_id):
    """
    近10日間の同条件レース上位3頭をHTMLで表示する
    """
    # --- 基準レース情報取得 ---
    year = date_str[:4]
    base_type, base_len, ground_state, race_class = get_race_info(year, place_id, target_id)
    if base_type == None and base_len == None and ground_state == None and race_class == None:
        return

    # --- 日付処理 ---
    base_date = datetime.strptime(date_str, "%Y%m%d")
    recent_days = [base_date - timedelta(days=i) for i in range(1, 11)]
    matched_race_ids = []
    # --- 各日付ごとに処理 ---
    for race_day in recent_days:
        race_day_str = race_day.strftime("%Y%m%d")
        try:
            daily_ids = get_daily_id(place_id, race_day)
        except Exception:
            continue

        for rid in daily_ids:
            info = get_race_info(year, place_id, rid)
            if info is None:
                continue
            race_type, course_len, ground_state, race_class = info

            # 条件一致
            if race_type == base_type and str(course_len) == str(base_len):
                # print(f"一致: {race_day_str} {race_type} {course_len}")
                matched_race_ids.append((rid, race_day_str, race_class, ground_state))

    if not matched_race_ids:
        return "<div>同条件の近走レースはありません。</div>"
    # print(matched_race_ids)
     # --- HTML構築開始 ---
    html = f"""
    <div id="recentSameCondition" style="margin-top:20px; padding:10px; border:1px solid #ccc; background:#fefefe;">
      <h3>🏇 近10日間の同条件レース結果 ({base_type} {base_len}m)</h3>
    """

    for race_id, race_date_str, race_class, ground_state in matched_race_ids:
        result_csv = os.path.join(RACE_RESULTS_PATH, name_header.PLACE_LIST[place_id - 1], year, f"{race_id}.csv")
        if not os.path.exists(result_csv):
          print(f"警告: レース結果ファイルが存在しません: {result_csv}")
          continue
    
        df_all = pd.read_csv(result_csv, dtype=str, index_col=0)

        if df_all is None or df_all.empty:
            continue

        # race_id列がインデックスになっている場合もあるので両対応
        if "Unnamed: 0" in df_all.columns:
            df_all.rename(columns={"Unnamed: 0": "race_id"}, inplace=True)

        # indexをrace_idに変換しているケースもあるのでケア
        if "race_id" not in df_all.columns:
            df_all = df_all.reset_index().rename(columns={"index": "race_id"})

        # race_id一致行を抽出
        df_race = df_all[df_all["race_id"].astype(str) == str(race_id)]
        if df_race.empty:
            continue

        # 上位3頭抽出
        df_top3 = df_race.head(3)[["馬名", "タイム", "人気", "単勝", "上り", "通過", "馬体重"]]

        # レース情報
        type, len, ground, race_class_name = get_race_info(year, place_id, race_id)
        race_num = str(int(race_id[-2:]))

        # レース名
        race_info_path = os.path.join(RACE_CALENDAR_FOLDER_PATH, f"race_time_id_list/{date_str}.csv")
        race_name = ""
        if os.path.exists(race_info_path):
            df_info = pd.read_csv(race_info_path, dtype=str)
            match = df_info[df_info["race_id"].astype(str) == str(target_id)]
            if not match.empty:
                race_name = str(match.iloc[0]["race_name"])

        # --- HTML組み立て ---
        html += f"""
        <div style="margin-top:10px; padding:5px; border:1px solid #ddd;">
          <h4>{race_date_str}:{race_num}R {race_name} {type}{len}m {race_class_name} ({ground})</h4>
          <table style="width:100%; border-collapse:collapse; text-align:center; font-size:14px;">
            <thead>
              <tr style="background:#f2f2f2;">
                <th>順位</th><th>馬名</th><th>タイム</th><th>人気</th><th>単勝</th><th>上り</th><th>通過</th><th>馬体重</th>
              </tr>
            </thead>
            <tbody>
        """

        for i, row in df_top3.iterrows():
            html += f"""
              <tr>
                <td>{i + 1}</td>
                <td>{row["馬名"]}</td>
                <td>{row["タイム"]}</td>
                <td>{row["人気"]}</td>
                <td>{row["単勝"]}</td>
                <td>{row["上り"]}</td>
                <td>{row["通過"]}</td>
                <td>{row["馬体重"]}</td>
              </tr>
            """

        html += "</tbody></table></div>"

    html += "</div>"
    return html

def make_race_card_html(date_str, place_id, target_id):
    """レースカード HTML を生成して output_path に保存する"""
    race_num = int(str(target_id)[-2:])
    output_dir = RACE_HTML_PATH + f"{date_str}/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    output_path = output_dir + f"{name_header.PLACE_LIST[place_id - 1]}R{race_num}.html"
    date_display = format_date(date_str)
    output_dir = os.path.dirname(output_path) or "."
    # CSV読込
    df = read_race_csv(date_str, target_id)
    if df is None:
        return

    # --- レース情報（コース・距離・馬場・クラス）を取得 ---
    course_info_text = generate_race_info(date_str, place_id, target_id)

    # --- レース結果、配当取得 ---
    result_df = get_result_table(date_str, place_id, target_id)
    if not result_df.empty:
        result_df = merge_rank_score(result_df, df)
    result_table_html = generate_result_table(result_df)

    returns_df = get_returns_table(date_str, place_id, target_id)
    payout_table_html = generate_payout_table_html(returns_df)

    # テーブル行作成
    table_rows = build_table_rows(df)

    # レース名・時刻取得
    race_info_path = os.path.join(RACE_CALENDAR_FOLDER_PATH, f"race_time_id_list/{date_str}.csv")
    race_name = ""
    race_time = ""
    if os.path.exists(race_info_path):
        df_info = pd.read_csv(race_info_path, dtype=str)
        match = df_info[df_info["race_id"].astype(str) == str(target_id)]
        if not match.empty:
            race_name = str(match.iloc[0]["race_name"])
            race_time = str(match.iloc[0]["race_time"])

    # レースの平均時計、上り時計を取得
    run_time_info = generate_run_time_info(date_str, place_id, target_id)
    # レースの勝ち馬の平均馬体重情報を取得
    weight_info = generate_weight_info(date_str, place_id, target_id)
    # レースの血統情報を取得
    peds_info = generate_peds_result_html(date_str, place_id, target_id)
    # レースの人気情報を取得
    pops_info = generate_pops_info(date_str, place_id, target_id)
    # レースの枠順情報を取得
    frames_info = generate_frame_horse_info(date_str, place_id, target_id)
    # ナビゲーション作成
    nav_html = build_nav_html(output_dir, date_str, place_id, target_id)
    # 近走の結果を取得
    recent_html = generate_recent_same_condition_html(date_str, place_id, target_id)

    # --- HTML生成・書き込み ---
    html_content = build_html_content(
        date_display=date_display,
        place_id=place_id,
        race_num=race_num,
        race_name=race_name,
        race_time=race_time,
        nav_html=nav_html,
        table_rows=table_rows,
        run_time_info = run_time_info,
        weight_info = weight_info,
        peds_info = peds_info,
        pops_info = pops_info,
        frames_info = frames_info,
        recent_html = recent_html,
        result_table_html=result_table_html,
        payout_table_html=payout_table_html,
    )

    # 🆕 コース情報をHTMLに挿入
    html_content = html_content.replace(
        "<p>発走時刻:",
        f"<p>{course_info_text}</p>\n  <p>発走時刻:"
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

def make_daily_race_card_html(race_day = date.today()):
    """指定された日付の全レースカード HTML を生成する"""
    date_str = race_day.strftime("%Y%m%d")
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
      race_id_list = get_daily_id(place_id, race_day)
      if not race_id_list:
          print("指定日のレースIDが見つかりません: ", date_str, name_header.PLACE_LIST[place_id - 1])
          continue
      for race_id in race_id_list:
          make_race_card_html(date_str, place_id, race_id)

if __name__ == "__main__":
    # テスト用実行コード
    race_day = date(2025, 10, 1)
    # make_daily_race_card_html(race_day)

    today = date.today()
    current = race_day

    while current <= today:
        print(f"🏇 {current} のレースカードを作成中...")
        make_daily_race_card_html(current)
        current += timedelta(days=1)

    print("🎉 すべての日付のレースカード作成が完了しました！")