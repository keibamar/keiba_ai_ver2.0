import os
import re
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
import horse_peds
import race_results
import past_performance

def peds_dataset_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """
    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def make_peds_dataset_from_race_results(place_id, year):
    """ peds_datasetをrace_resultsから作成する 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
    """
    #レース結果のcsv情報を取得
    df_course = race_results.get_race_results_csv(place_id, year)
    
    if not df_course.empty:
        # ホースIDの抽出
        horse_id_list = df_course["horse_id"].to_list()

        # 血統データの取得
        df_peds = get_peds_dataset_from_horse_id_list(horse_id_list)
        
        # datasetの保存
        save_peds_dataset(df_peds, place_id, year)

def save_peds_dataset(peds_df, place_id, year):
    """ peds_datasetのDataFrameを保存 
        Args:
            peds_df(pd.DataFrame)
            horse_id (int) :horse_id
            past_performance_df.DataFrame） : past_performanceのデータセット
    """
    try:
        if any(peds_df):
            # 重複している内容を消去
            peds_df = peds_df.drop_duplicates(keep = 'first')
            # str型に変更
            peds_df = peds_df.astype(str)
            # ローカル保存
            peds_df.to_csv(name_header.DATA_PATH + "/PedsResults/" +  name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_peds.csv')
            peds_df.to_pickle(name_header.DATA_PATH + "/PedsResults/" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_peds.pickle')
    except Exception as e:
            peds_dataset_error(e)

def get_peds_dataset_csv(place_id, year):
    """ pedsのcsvを取得する 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
    """
    # csvを読み込む 
    path = name_header.DATA_PATH + "/PedsResults/" +  name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_peds.csv'
    if os.path.isfile(path):
        df = pd.read_csv(path, index_col = 0, dtype = str)
        df.fillna(np.nan)  
    else :
        df = pd.DataFrame()
    return df

def get_peds_dataset_from_horse_id_list(horse_id_list):
    """ horse_id_listからpeds_datasetのDatasetを作成 
        Args:
            horse_id_list : horse_idのリスト
        Returns:
            peds_df(pd.DataFrame) : peds_dataset
    """
    # 血統データの取得
    peds_df = pd.DataFrame()
    for horse_id in horse_id_list:
        peds_df = pd.concat([peds_df,horse_peds.get_horse_peds_csv(horse_id).T],axis = 0)
    return peds_df

def merge_pedsdata_with_race_results(place_id, year):
    """ 血統とレース結果の統合データ作成 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
    """
    #レース結果のcsv情報を取得
    df_course = race_results.get_race_results_csv(place_id, year)
    if not df_course.empty:
        # レース結果から，"着順","race_type","course_len","ground_state","class"を抽出
        df_result = df_course['着順']
        df_result =pd.concat([df_result,df_course['race_type']],axis = 1)
        df_result =pd.concat([df_result,df_course['course_len']],axis = 1)
        df_result =pd.concat([df_result,df_course['ground_state']],axis = 1)
        df_result =pd.concat([df_result,df_course['class']],axis = 1)

        # 血統データの取得
        df_peds = get_peds_dataset_csv(place_id, year)    
        # 血統情報を結合
        df_result = pd.concat([df_result.reset_index(), df_peds.reset_index(drop = True)], axis = 1).set_index('index')

        # データセットの保存
        df_result.to_csv(name_header.DATA_PATH + "/PedsResults/" +  name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_peds_data.csv')
        df_result.to_pickle(name_header.DATA_PATH + "/PedsResults/" +  name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_peds_data.pickle')

def update__peds_dataset(place_id, day = date.today()):
    """ 指定したコースの指定日から、１週間分のデータセットを更新  
    Args:
        place_id (int) : 開催コースid
        day(Date) : 日（初期値：今日）
    """ 
    race_id_list = get_race_id.get_past_weekly_id(place_id, day)
    horse_id_list = past_performance.get_horse_id_list_from_race_id_list(race_id_list)
    if any(horse_id_list):
        # 新しいデータセットの取得
        new_peds_df = get_peds_dataset_from_horse_id_list(horse_id_list)
        # 既存のデータセットを取得
        old_peds_df = get_peds_dataset_csv(place_id, day.year)
        # 新しいデータセットを統合
        new_peds_df = pd.concat([old_peds_df,new_peds_df],axis = 0)
        save_peds_dataset(new_peds_df, place_id, day.year)

def weekly_update_pedsdata(day = date.today()):
    """ 指定した日にちから、１週間分のデータセットを更新  
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print("[WeeklyUpdate]" + name_header.PLACE_LIST[place_id -1] + " RaceResults")
        update__peds_dataset(place_id, day)
        merge_pedsdata_with_race_results(place_id, day.year)

def monthly_update_pedsdata(day = date.today()):
    """ 指定した日にちまでのその年のデータセットを更新  
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print("[MonthlyUpdate]" + name_header.PLACE_LIST[place_id -1] + " RaceResults")
        make_peds_dataset_from_race_results(place_id, day.year)
        merge_pedsdata_with_race_results(place_id, day.year)

def make_all_pedsdata(year = date.today().year):
    """ 指定した年までの、すべてのデータセットを作成 
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for y in range(2019, year + 1):
        for place_id in range(1, len(name_header.PLACE_LIST) + 1):
            print("[NewMake]" + str(year) + ":" + name_header.PLACE_LIST[place_id -1] + " PedsResults")
            make_peds_dataset_from_race_results(place_id, y)
            merge_pedsdata_with_race_results(place_id, y)

if __name__ == "__main__":
   monthly_update_pedsdata()
   weekly_update_pedsdata()