import os
import re


def format_date(date_str):
    """YYMMDD → YYYY/M/D"""
    if not date_str:
        return ""
    dt = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    return dt


def merge_rank_score(df_race, df_analysis):
    # 数値型に揃える
    df_race["馬番"] = df_race["馬番"].astype(str)
    df_analysis["馬番"] = df_analysis["馬番"].astype(str)

    # 必要列だけ抽出
    entry_sub = df_analysis[["馬番", "rank", "score"]].copy()

    # 結合
    merged = pd.merge(df_race, entry_sub, on="馬番", how="left")
    return merged

def get_subfolders(path):
    """
    指定したフォルダ内にあるフォルダ名一覧を取得する関数
    :param path: 親フォルダのパス
    :return: サブフォルダ名のリスト
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"指定されたパスが存在しません: {path}")
    
    return [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]