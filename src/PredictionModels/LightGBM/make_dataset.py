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
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\src\Datasets")
import name_header

import horse_peds
import race_results
import peds_results
import past_performance
import average_time

index = [ 
        # racce_id
        "race_id",
        # 血統情報
            # コース着度数
            "f_course1", "f_course2", "f_course3", "f_courseout",
            "mf_course1", "mf_course2", "mf_course3", "mf_courseout",
            "f_mf_course1", "f_mf_course2", "f_mf_course3", "f_mf_courseout", 
            # キョリ着度数
            "f_len1", "f_len2", "f_len3","f_lenout",
            "mf_len1", "mf_len2", "mf_len3", "mf_classout",
            "f_mf_len1", "f_mf_len2", "f_mf_len3", "f_mf_classout", 
            # クラス着度数
            "f_class1", "f_class2", "f_class3", "f_classout", 
            "mf_class1", "mf_class2", "mf_class3", "mf_classout", 
            "f_mf_class1", "f_mf_class2", "f_mf_class3", "f_mf_classout", 
        # 過去レース情報
        "time_df_course1", "time_df_class1", "ninki_1", "result_1", 
        "time_df_course2", "time_df_class2", "ninki_2", "result_2", 
        "time_df_course3", "time_df_class3", "ninki_3", "result_3", 
        # 枠順
        "waku", "umaban",
        ]

def make_dataset_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """
    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def save_LightGBM_dataset_csv(place_id, year, type, length, df_dataset):
    """ LightGBM_datasetのDataFrameを保存 
        Args:
            place_id(int) : place_id
            year (int) : 年
            type(str) : レースタイプ
            length(int) : キョリ
            df_dataset(pd.DataFrame） : lightGBM用のデータセット
    """
    try:
        if any(df_dataset):
            path = name_header.DATA_PATH + "/PredictionModels/LightGBM/Datasets/" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + "_" + str(type) + str(length) + '_ai_dataset_for_rank.csv'
            df_dataset = df_dataset.reset_index(drop = True)
            # ローカル保存
            df_dataset.to_csv(path)
    except Exception as e:
            make_dataset_error(e)    

def sava_LightGBM_dataset_flag_csv(place_id, year, type, length, flag_list):
    """ LightGBM_datasetのDataFrameを保存 
        Args:
            place_id(int) : place_id
            year (int) : 年
            type(str) : レースタイプ
            length(int) : キョリ
            flag_list(list） : lightGBM用のflagデータセット
    """
    try:
        if flag_list:
            df_flag = pd.DataFrame(flag_list)
            df_flag.columns = ["result_flag"]
            flag_path = name_header.DATA_PATH + "/PredictionModels/LightGBM/Datasets/" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + "_" + str(type) + str(length)  + '_ai_dataset_flag.csv'
            df_flag.to_csv(flag_path)
    except Exception as e:
            make_dataset_error(e)

def get_LightGBM_dataset_csv(place_id, year, type, length):
    """ LightGBM_datasetのDataFrameを保存 
        Args:
            place_id(int) : place_id
            year (int) : 年
            type(str) : レースタイプ
            length(int) : キョリ
        Returns:
            df(pd.DataFrame） : lightGBM用のデータセット
    """
    # csvを読み込む 
    path = name_header.DATA_PATH + "/PredictionModels/LightGBM/Datasets/" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + "_" + str(type) + str(length) + '_ai_dataset_for_rank.csv'
    if os.path.isfile(path):
        df = pd.read_csv(path, index_col = 0, dtype = float)
    else :
        df = pd.DataFrame()
    return df

def get_LightGBM_dataset_flag_csv(place_id, year, type, length):
    """ LightGBM_datasetのDataFrameを保存 
        Args:
            place_id(int) : place_id
            year (int) : 年
            type(str) : レースタイプ
            length(int) : キョリ
        Returns:
            df(pd.DataFrame） : lightGBM用のflagデータセット
    """
    # csvを読み込む 
    path = name_header.DATA_PATH + "/PredictionModels/LightGBM/Datasets/" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + "_" + str(type) + str(length)  + '_ai_dataset_flag.csv'
    if os.path.isfile(path):
        df = pd.read_csv(path, index_col = 0, dtype = int)
    else :
        df = pd.DataFrame()
    return df


def is_in_show(df_result, race_id):
    """ df_resultの結果が3着内かチェックする 
        Args:
            df_result(pd.DataFrame） : lightGBM用のデータセット
            race_id(int) : race_id
        Returns:
            Bool 
    """
    rank = df_result.at[race_id,"着順"]
    if rank.isdigit():
        rank = int(rank)
        if rank <= 3:
            return 1
        else:
            return 0
    else:
        return 0

def get_past_race_info_data(race_info_df):
    """ 過去レースのタイム指数、着順、人気を取得 
        Args:
            race_info_df(pd.DataFrame）: lightGBM用のデータセット
        Returns:
            race_score_list(list) : race_scoreのlist
    """
    race_score_list = []
    for i in range(3):
        # レース結果がある場合
        if i < len(race_info_df.index):
            # レース結果を取得(time_df(コース、距離、馬場), time_df(同クラス),"course_flag1", "len_flag1", "type_flag1", "state_flag1", "class_flag1" )
            df_time = get_time_info(race_info_df.iloc[i])

            if "除" in str(race_info_df.at[i,"着 順"]) or  "取" in str(race_info_df.at[i,"着 順"]):
                df_time.append(np.nan) 
                df_time.append(np.nan)
            elif  "中" in str(race_info_df.at[i,"着 順"]) or "失" in str(race_info_df.at[i,"着 順"]):
                df_time.append(float(re.sub(r"\D", "", race_info_df.at[i,"人 気"])))
                df_time.append(np.nan)  
            elif np.isnan(float(race_info_df.at[i,"人 気"])):
                df_time.append(np.nan)
                df_time.append(np.nan)  
            else:
                df_time.append(float(re.sub(r"\D", "", race_info_df.at[i,"人 気"]))) 
                df_time.append(float(re.sub(r"\D", "", race_info_df.at[i,"着 順"])))
            race_score_list.append(df_time)
            
        else:
            race_score_list.append([np.nan, np.nan, np.nan, np.nan])
    
    # リストの１次元化
    race_score_list = sum(race_score_list, [])

    return race_score_list

def get_time_info(race_info):
    """ 過去のレース結果よりタイム差を取得(タイム差/平均タイム[ms]) 
        Args:
            race_info(pd.DataFrame) : race結果のdataframe
        Returns:
            diff_time_list(list) : タイム差リスト
    """
    # 過去レースのコース情報を取得
    course_info = race_results.get_course_info(race_info)
    # JRA以外のコースの場合0を返す
    if  course_info[0] < 0:
        return [0,0]
    # 走破タイムをmsecに変換
    race_time = average_time.get_race_time_msec(race_info["タイム"])
    # タイム差の計算
    diff_time_list = average_time.get_time_diff(race_time, course_info)
    return diff_time_list

def make_dataset_for_train(place_id, year = date.today().year):
    """ LightGBM_datasetのDataFrameを作成
        Args:
            place_id(int) : place_id
            year (int) : 年
    """
    # 過去のレース結果をデータフレームで取得
    df_results = race_results.get_race_results_csv(place_id, year)
    if df_results.empty:
        print("not data:", year, name_header.PLACE_LIST[place_id - 1])
        return
    
    # コースごとにデータセットを分割
    for type, length in name_header.COURSE_LISTS[place_id - 1]:
        print(type,length)
        df_results_course = df_results[df_results["race_type"] == type]
        df_results_course = df_results_course[df_results_course["course_len"] == length]
        if df_results_course.empty:
            continue
        # race_idの取得
        race_id_list = pd.DataFrame(df_results_course.index, columns = ["race_id"])
        
        # datasetを格納するDataFrame
        df_dataset = pd.DataFrame()
        # 3着内か否かのflag
        flag_list = []

        # 各馬のデータセットを作成
        for i in tqdm(range(len(df_results_course))):
            df_result = df_results_course[i:i+1]
            race_id = int(df_result.index.values)
            
            # 3着内かチェック
            flag_list.append(is_in_show(df_result, race_id))
            
            # レース情報の取得
            course_info = [place_id, df_result.at[race_id,"race_type"], df_result.at[race_id,"course_len"], df_result.at[race_id,"ground_state"],df_result.at[race_id,"class"] ]
            horse_id = df_result.at[race_id, "horse_id"]
            race_year = re.findall(r"\d+",df_result.at[race_id,"date"])[0]
                 
            # 血統情報の取得
            peds_info = horse_peds.get_peds_info(horse_id)
            # 血統データの取得
            df_peds = peds_results.peds_index(peds_info[0], peds_info[1], course_info, race_year)
            # リストの１次元化
            df_peds = sum(df_peds.T.values.tolist(), [])

            # 過去レースの情報を取得
            race_info_df = past_performance.get_past_race_info(horse_id, race_id, race_num = 3)
            # 過去レース分の人気・着順を取得
            df_race = get_past_race_info_data(race_info_df)
            # データセットの結合
            df_index = df_peds + df_race 
            
            # データフレームに格納
            df_index = pd.DataFrame(df_index).T
            df_dataset = pd.concat([df_dataset.reset_index(drop = True), df_index.reset_index(drop = True)])

        # race_idの結合
        df_dataset = pd.concat([race_id_list, df_dataset.reset_index(drop = True)], axis = 1)
        # 枠番・馬番の結合
        df_dataset = pd.concat([df_dataset, df_results_course["枠番"].reset_index(drop = True),df_results_course["馬番"].reset_index(drop = True)], axis = 1).reset_index(drop = True)
        df_dataset.columns = index

        # csvでデータセットを出力
        save_LightGBM_dataset_csv(place_id, year, type, length, df_dataset)
        # csvでフラグデータセットを出力
        sava_LightGBM_dataset_flag_csv(place_id, year, type, length, flag_list)

if __name__ == "__main__":
   print("analysis_datasets")
   for year in range(2020, 2025):
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print(year, name_header.PLACE_LIST[place_id - 1])
        make_dataset_for_train(place_id, year)