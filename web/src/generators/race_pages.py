import os
import sys
import pandas as pd
from datetime import date

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

from config.path import RACE_HTML_PATH, RACE_INFO_PATH, RACE_CARDS_PATH, RACE_RESULTS_PATH, RACE_RETURNS_PATH, RACE_CALENDAR_FOLDER_PATH
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

def build_html_content(date_display, place_id, race_num, race_name, race_time, nav_html, table_rows, result_table_html, payout_table_html):
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
    result_table_html=result_table_html,
    payout_table_html=payout_table_html,
    )

def generate_result_table(df) :
    if df.empty:
        return "<p>レース結果データが見つかりません。</p>"
    
    result_rows = ""
    for _, row in df.iterrows():
        rank = row["着順"]
        waku = row["枠"]
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
    print(race_info_path)
    if os.path.exists(race_info_path):
        df_info = pd.read_csv(race_info_path, dtype=str)
        print(df_info)
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

    # ナビゲーション作成
    nav_html = build_nav_html(output_dir, date_str, place_id, target_id)

    # --- HTML生成・書き込み ---
    html_content = build_html_content(
        date_display=date_display,
        place_id=place_id,
        race_num=race_num,
        race_name=race_name,
        race_time=race_time,
        nav_html=nav_html,
        table_rows=table_rows,
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
    race_day = date(2025, 10, 18)
    make_daily_race_card_html(race_day)