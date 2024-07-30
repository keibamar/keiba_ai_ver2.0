import os
import sys

from datetime import date, timedelta
import numpy as np
import pandas as pd
from tqdm import tqdm

# pycache を生成しない
sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
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

if __name__ == "__main__":
    montly_update_race_results()
    weekly_update_race_results()