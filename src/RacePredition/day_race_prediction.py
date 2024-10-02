import os
import re
import sys
import time

from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import requests
from tqdm import tqdm
import warnings
warnings.simplefilter('ignore')

sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\src\PredictionModels\LightGBM")
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\src\Dataset")

import make_dataset
import peds_results

# AI予想のランキングを計算
def rank_prediction(race_id, horse_ids, race_info_df, waku_df):
    if(0):
        # datasetを格納するDataFrame
        race_dataset = pd.DataFrame(columns = make_dataset.index)
        # レース情報の取得
        race_year = int(str(race_id)[0] + str(race_id)[1] + str(race_id)[2] + str(race_id)[3])
        place_id = int(str(race_id)[4] + str(race_id)[5])
        course_info = [place_id, race_info_df.at[0,"race_type"], race_info_df.at[0,"course_len"], race_info_df.at[0,"ground_state"],race_info_df.at[0, "class"] ]
        
        for horse_id in horse_ids: 
            # 血統情報の取得
            peds_info = get_peds(horse_id).values.tolist()
            peds_info = [peds_info[0], peds_info[4]]

            # 血統データの取得
            df_peds = ai_analysis.calc_peds_info(peds_info, course_info, race_year)

            # 過去3レースの情報を取得
            race_info = ai_analysis.get_3race_info(horse_id, race_id)
            
            # 過去3レース分のタイムデータを取得
            df_race = ai_analysis.calc_race_info(race_info, course_info)

            # データセットの結合
            df_result = df_peds + df_race

            # データフレームに格納
            df_result = pd.DataFrame(df_result).T
            df_result.columns = ai_analysis.index
            race_dataset = pd.concat([race_dataset, df_result])
        # 枠番・馬番の結合
        race_dataset = pd.concat([race_dataset.reset_index(drop = True), waku_df.reset_index(drop = True)], axis = 1)
            
        # レーススコアの予想
        rank_df =  lightgbm_analysis.prediction_race_score(place_id, course_info[1], course_info[2], race_dataset)
        return rank_df
    else:
        # datasetを格納するDataFrame
        score = [0] * len(horse_ids)
        rank = [0] * len(horse_ids)
        return pd.concat([pd.DataFrame(score, columns = ["score"]), pd.DataFrame(rank, columns = ["rank"])], axis = 1)


# 指数の順位を出力(出馬表から)
def calc_index(race_id, race_info_df, horse_ids, horse_peds, horse_results):
    
    # 指数計算用のコース情報を抽出
    year = int(str(race_id)[0] + str(race_id)[1] + str(race_id)[2] + str(race_id)[3])
    place_id = int(str(race_id)[4] + str(race_id)[5])
    race_type = race_info_df.at[0, "race_type"]
    course_len = str(race_info_df.at[0,"course_len"])
    ground_state = race_info_df.at[0, "ground_state"]
    race_class = race_info_df.at[0, "class"]
    course_info = [place_id, race_type, course_len, ground_state, race_class]

    peds_index_list = []
    time_index_list = []
    total_index_list =[]
    for i in range(len(horse_ids)):
        # 血統データ用の情報を抽出
        peds_info = [horse_peds.at[0,horse_ids[i]], horse_peds.at[4,horse_ids[i]]]

        # 血統スコアの計算
        peds_index = peds_results.peds_index(peds_info, course_info, year)
        peds_index_list.append(round(float(peds_index),4))
        # タイムスコアの計算
        time_index = analysis_datasets.time_index(horse_results[i], course_info)
        time_index_list.append(round(float(time_index),4))
        # totalスコアの計算
        total_index = analysis_datasets.total_index(peds_index, time_index)
        total_index_list.append(total_index)
    # 指数の順位を表示
    return analysis_datasets.rank_index(total_index_list), peds_index_list, time_index_list, total_index_list

