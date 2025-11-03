import os
import sys
import re

from datetime import date
import pandas as pd
import numpy as np
import warnings
warnings.simplefilter('ignore')

sys.dont_write_bytecode = True
sys.path.append(r"C:\keiba_ai\keiba_ai_ver2.0\libs")
import name_header

# # --- 固定リスト ---
CLASSES = ["all", "未勝利","新馬", "1勝クラス", "2勝クラス", "3勝クラス", "オープン"]
GROUNDS = ["全", "良", "稍重", "重", "不良"]

def analyze_winner_weights_multi_years(base_dir, place_id, start_year):
    """
    各年度（start_year〜今年）について勝ち馬の平均馬体重を算出し、
    年ごとの結果 + 全期間平均の DataFrame を返す。

    Parameters
    ----------
    base_dir : str
        CSV ファイルのディレクトリパス（例: "./data/"）
        → ファイル名は "{year}_race_results.csv" のようにする前提。
    place_id : int
        競馬場ID (1〜10)
    start_year : int
        集計を始める年（例: 2020）

    Returns
    -------
    results_by_year : dict[int, pd.DataFrame]
        年度別の結果を格納した辞書。
    total_df : pd.DataFrame
        全期間の平均結果。
    """
    current_year = int(date.today().year)
    results_by_year = {}

    for year in range(start_year, current_year + 1):
        csv_path = os.path.join(base_dir, f"{year}_race_results.csv")
        if not os.path.exists(csv_path):
            print(f"⚠️ {csv_path} が見つかりません。スキップします。")
            continue
        print(f"📘 {year}年のデータを処理中 ...")
        df_year = analyze_winner_weights(csv_path, place_id)
        if not df_year.empty:
            df_year["year"] = year
            results_by_year[year] = df_year

    if not results_by_year:
        print("❌ 有効なデータがありません。")
        return {}, pd.DataFrame()

    combined_df = pd.concat(results_by_year.values(), ignore_index=True)

    group_cols = ["race_type", "course_len", "ground_state", "class"]
    total_df = (
        combined_df.groupby(group_cols, dropna=False)["馬体重"]
        .mean()
        .round(1)
        .reset_index()
    )

    total_df["race_type"] = pd.Categorical(total_df["race_type"], categories=["芝", "ダート"], ordered=True)
    total_df["class"] = pd.Categorical(total_df["class"], categories=CLASSES, ordered=True)
    total_df["ground_state"] = pd.Categorical(total_df["ground_state"], categories=GROUNDS, ordered=True)
    total_df = total_df.sort_values(["race_type", "course_len", "class", "ground_state"]).reset_index(drop=True)
    total_df = total_df.reindex(columns=["race_type", "course_len", "ground_state", "class", "馬体重"])

    print(f"✅ 全期間平均（{start_year}〜{current_year}）を作成しました。")
    return total_df 

def analyze_winner_weights(csv_path, place_id):
    """
    勝ち馬の「馬体重」平均を race_type, course_len, ground_state, class ごとに算出する。
    """
    if os.path.isfile(csv_path):
        df_raw = pd.read_csv(csv_path, dtype=str, index_col=0).reset_index().rename(columns={"index": "race_id"})
    else:
        return pd.DataFrame()

    df = df_raw.copy()
    df["着順"] = pd.to_numeric(df["着順"], errors="coerce")
    df["course_len"] = pd.to_numeric(df["course_len"], errors="coerce")
    df["馬体重"] = pd.to_numeric(df["馬体重"], errors="coerce")

    winners = df[df["着順"] == 1].copy()
    if winners.empty:
        return pd.DataFrame()

    all_results = []
    courses = name_header.COURSE_LISTS[place_id - 1]

    for race_type, course_len in courses:
        base_data = winners[
            (winners["race_type"] == race_type) &
            (winners["course_len"] == float(course_len))
        ]

        for cls in CLASSES:
            for grd in GROUNDS:
                tmp = base_data.copy()
                if cls != "all":
                    tmp = tmp[tmp["class"] == cls]
                if grd != "全":
                    tmp = tmp[tmp["ground_state"] == grd]

                avg_weight = tmp["馬体重"].mean() if not tmp.empty else None

                all_results.append({
                    "race_type": race_type,
                    "course_len": int(course_len),
                    "ground_state": grd,
                    "class": cls,
                    "馬体重": round(avg_weight, 1) if avg_weight is not None else None,
                })

    df_result = pd.DataFrame(all_results).round(1)
    df_result["race_type"] = pd.Categorical(df_result["race_type"], categories=["芝", "ダート"], ordered=True)
    df_result["class"] = pd.Categorical(df_result["class"], categories=CLASSES, ordered=True)
    df_result["ground_state"] = pd.Categorical(df_result["ground_state"], categories=GROUNDS, ordered=True)
    df_result = df_result.sort_values(["race_type", "course_len", "class", "ground_state"]).reset_index(drop=True)

    df_result = df_result.reindex(columns=["race_type", "course_len", "ground_state", "class", "馬体重"])

    return df_result

if __name__ == '__main__':
# --- 使用例 ---
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        # 各年の記録を計算
        for year in range(2019,date.today().year + 1):
            csv_path = name_header.DATA_PATH + "//RaceResults//" + name_header.PLACE_LIST[place_id -1] + "//" + f"{year}_race_results.csv"
            result = analyze_winner_weights(csv_path, place_id)
            if not result.empty:
                output_path = name_header.DATA_PATH + "//AverageWeights//" + name_header.PLACE_LIST[place_id -1] + "//" + f"{year}_wineer_weight.csv"
                result.to_csv(output_path)
        # totalの記録を計算
        base_dir = name_header.DATA_PATH + "//RaceResults//" + name_header.PLACE_LIST[place_id -1] + "//"
        total_df = analyze_winner_weights_multi_years(base_dir, place_id, 2019)
        total_ouutput_path = name_header.DATA_PATH + "//AverageWeights//" + name_header.PLACE_LIST[place_id -1] + "//" + "total_wineer_weight.csv"
        total_df.to_csv(total_ouutput_path)