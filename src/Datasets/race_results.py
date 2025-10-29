import os
import re
import sys

from datetime import date, timedelta
import numpy as np
import pandas as pd
from tqdm import tqdm

# pycache を生成しない
sys.dont_write_bytecode = True
sys.path.append(r"C:\keiba_ai\keiba_ai_ver2.0\libs")
import get_race_id
import name_header
import scraping

def race_results_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """
    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def scrape_race_results_dataframe(race_id_list):
    """ race_id_listのrace_resultsのDataFrameを作成 
        Args:
            race_id_list list(str) : race_idのリスト
        Returns:
            DataFrame : race_id_listのrace_results    
    """
    # スクレイピング
    race_results_df = pd.DataFrame()
    for race_id in tqdm(race_id_list):
        race_results_df = pd.concat([race_results_df,scraping.scrape_race_results(race_id)],axis = 0)

    return race_results_df

def format_race_results_dataframe(race_results_df):
    """ race_resultsのフォーマットを整える 
        Args:
            race_results_df（pd.DataFrame） : race_resultデータセット
        Return:
            race_results_df（pd.DataFrame） : フォーマットされたrace_resultデータセット
    """
    # 欠損値をnanで埋める
    race_results_df.fillna(np.nan)   
    # "斤量","人気"をfloatに固定
    race_results_df['人気'] = race_results_df['人気'].astype(float)
    race_results_df['斤量'] = race_results_df['斤量'].astype(float)
    # 文字列に変換
    race_results_df = race_results_df.astype(str)
    # 重複している内容を消去
    race_results_df = race_results_df.drop_duplicates(keep = 'first')

    return race_results_df

def save_race_results_dataset(place_id, year, race_results_df):
    """ race_resultsのDataFrameを保存 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
            race_results_df（pd.DataFrame） : race_resultデータセット
    """
    try:
        if any(race_results_df):
            # フォーマットを整える
            race_results_df = format_race_results_dataframe(race_results_df)
            # csv/pickleに保存
            race_results_df.to_csv(name_header.DATA_PATH + "RaceResults\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_race_results.csv')
            race_results_df.to_pickle(name_header.DATA_PATH + "RaceResults\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_race_results.pickle')
    except Exception as e:
            race_results_error(e)

def get_race_results_csv(place_id, year):
    """ race_resultsのcsvを取得する 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
    """
    # csvを読み込む 
    path = name_header.DATA_PATH + "RaceResults\\" + name_header.PLACE_LIST[place_id - 1] +  '//' + str(year) + '_race_results.csv'
    if os.path.isfile(path):
        df = pd.read_csv(path, index_col = 0, dtype = str)
        df.fillna(np.nan)  
    else :
        df = pd.DataFrame()

    return df

def get_race_id_result(race_id):
    """race_idのレース結果の取得
        Args:
            race_id(str) : race_id
        Returns:
            race_results_df(pd.DataFrame) : レース結果のDataFrame
    """
    # race_idからyear, place_idの抽出
    year = int(str(race_id)[0] + str(race_id)[1] + str(race_id)[2] + str(race_id)[3])
    place_id = int(str(race_id)[4] + str(race_id)[5])

    # race_resultsの読出し
    df_results = get_race_results_csv(place_id, year)
    df_results.index = df_results.index.astype(str)
    # race_idのデータのみ抽出
    df_result = df_results[race_id:race_id]
    df_result = df_result.reset_index(drop = True)

    return df_result

def get_course_info(result):
    """レース情報を取得
        Args:
            result(pd.DataFrame) : race_resultのデータセット
        Returns:
            course_info : place_id, race_type, course_len, ground_state, race_class
    """
    # 開催競馬場を取得 (JRA以外は-1)
    place_id = -1
    course = result["開催"]
    for i in range(len(name_header.NAME_LIST)):
        if name_header.NAME_LIST[i] in course:
            place_id = i + 1
    if place_id < 0:
        return[place_id, " "," "," "," "]
    # レース情報を取得
    race_info = result["距離"]
    if "芝" in race_info:
        race_type = "芝"
    elif "ダ" in race_info:
        race_type = "ダート"
    else:
        race_type = "障害"
    course_len = re.sub(r"\D","",race_info)

    # 馬場状態を取得
    ground_state = result["馬 場"]
    if ground_state in "稍":
        ground_state = "稍重"
    if ground_state in "不":
        ground_state = "不良"

    # クラスを取得
    race_name = result["レース名"]
    if "新馬" in race_name:
        race_class = "新馬"
    elif "未勝利" in race_name:
        race_class = "未勝利"
    elif "1勝クラス" in race_name or "１勝クラス" in race_name:
        race_class = "1勝クラス"
    elif "2勝クラス" in race_name or "２勝クラス" in race_name:
        race_class = "2勝クラス"
    elif "3勝クラス" in race_name or "３勝クラス" in race_name:
        race_class = "3勝クラス"
    else :
        race_class = "オープン"

    return [place_id, race_type, course_len, ground_state, race_class]

def update_race_results_dataset(place_id, day = date.today()):
    """ 開催コースと日にちを指定して、過去1週間分のrace_resultsデータセットを更新する 
        Args:
            place_id (int) : 開催コースid
            day(int) : 日（初期値：今日）
    """
    # race_id_listの取得
    race_id_list = get_race_id.get_past_weekly_id(place_id, day)

    # 過去のデータセットを取得
    old_race_results_df = get_race_results_csv(place_id, day.year)

    # レース結果の取得
    new_race_results_df = scrape_race_results_dataframe(race_id_list)

    # 更新するデータセットがあれば更新
    if any(new_race_results_df):
        try:
            new_race_results_df = pd.concat([old_race_results_df,new_race_results_df],axis = 0)

            # csv/pickleに書き込む
            save_race_results_dataset(place_id, day.year, new_race_results_df)
        except Exception as e:
            race_results_error(e)

def make_yearly_race_results_dataset(place_id, year = date.today().year):
    """ 開催コースと年を指定して、1年間のrace_resultsデータセットを作成 
        Args:
            place_id (int) : 開催コースid
            year(int) : 年（初期値：今年）
    """
    #race_id_listの取得
    race_id_list = get_race_id.get_year_id_all(place_id, year)

    # スクレイピング
    race_results_df = scrape_race_results_dataframe(race_id_list)

    # csv/pickleに書き込む
    save_race_results_dataset(place_id, year, race_results_df)

def make_up_to_day_dataset(place_id, day = date.today()):
    """ 指定日までの、年間のrace_resultsデータセットを作成 
        Args:
            place_id (int) : 開催コースid
            day(int) : 日（初期値：今日）
    """
    #race_id_listの取得
    race_id_list = get_race_id.get_past_year_id(place_id, day)

    # スクレイピング
    race_results_df = scrape_race_results_dataframe(race_id_list)

    try:
        # csv/pickleに書き込む
        save_race_results_dataset(place_id, day.year, race_results_df)
    except Exception as e:
            race_results_error(e) 

def weekly_update_race_results(day = date.today()):
    """ 指定した日にちから、１週間分のデータセットを更新  
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print("[WeeklyUpdate]" + name_header.PLACE_LIST[place_id -1] + " RaceResults")
        update_race_results_dataset(place_id, day)

def montly_update_race_results(day = date.today()):
    """ 指定した日にちまでのその年のデータセットを更新  
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print("[MonthlyUpdate]" + name_header.PLACE_LIST[place_id -1] + " RaceResults")
        make_up_to_day_dataset(place_id, day)

def make_all_race_results(year = date.today().year):
    """ 指定した年までの、すべてのデータセットを作成 
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for y in range(2019, year + 1):
        for place_id in range(1, len(name_header.PLACE_LIST) + 1):
            print("[NewMake]" + str(y) + ":" + name_header.PLACE_LIST[place_id -1] + " RaceResults")
            make_yearly_race_results_dataset(place_id, y)

def split_race_results_by_year(place_id, year):
    """ place_idのrace_resultsを指定年で分割して取得
        Args:
            place_id(int) : place_id
            year(int) : 年 
    """
    # CSVファイルの読み込み（ファイル名は適宜変更してね）
    df = get_race_results_csv(place_id, year)

    # 出力先ディレクトリ（必要なら作成）
    output_dir = name_header.DATA_PATH + "RaceResults\\" + name_header.PLACE_LIST[place_id - 1] +  '//' + str(year)
    os.makedirs(output_dir, exist_ok=True)

    # race_id ごとに分割して保存
    for race_id, group in df.groupby(df.index):
        output_path = os.path.join(output_dir, f"{race_id}.csv")
        group.to_csv(output_path, index=True)
        print(f"保存完了: {output_path}")

if __name__ == "__main__":
    # montly_update_race_results()
    # weekly_update_race_results()
    make_all_race_results()