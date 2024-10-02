
import os
import re
import sys
from datetime import date, timedelta

from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import requests
from tqdm import tqdm
import warnings
warnings.simplefilter('ignore')

sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
import get_race_id
import name_header
import scraping

import day_race_prediction

def make_race_card_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """
    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def save_race_cards(race_card_df, day, race_id):
    """出馬表を保存
        Args:
            race_card_df(pd.DataFrame) : 出馬表データ
            day(date) : レース開催日
            race_id(int) : race_id
    """
    try:
        str_day = day.strftime("%Y%m%d")
        folder_path = name_header.DATA_PATH + "RaceCards//" + str_day
        if not os.path.isdir(folder_path):
            os.mkdir(folder_path)
        race_card_df.to_csv(folder_path + "//" + str(race_id) + ".csv")
    except Exception as e:
        make_race_card_error(e)

def get_peds_for_display(horse_peds):
    """出馬表用に父，母，母父のみ抽出
        Args:
            horse_peds(pd.DataFrame) : 血統データ
        Returns:
            horse_peds_display(pd.DataFrame) : 表示用血統データ
    """
    horse_peds_display = horse_peds.iloc[:5]
    horse_peds_display = horse_peds_display.drop(horse_peds_display.index[[2,3]])
    horse_peds_display = horse_peds_display.rename(index = {0: '父'})
    horse_peds_display = horse_peds_display.rename(index = {1: '母'})
    horse_peds_display = horse_peds_display.rename(index = {4: '母父'})    
    return horse_peds_display

def make_race_card(place_id = 0, race_day = date.today()):
    """出馬表を出力
        Args:
            place_id (int) : 開催コースid (初期値0=全コース)
            race_day(Date) : 日（初期値：今日）
    """
    # 今日のid_listを取得
    race_id_list = get_race_id.get_daily_id(place_id, race_day)
    print("MakeRaceCards")
    print(race_id_list)
    # レース情報を取得
    for race_id in tqdm(race_id_list):
        # 出馬表を取得
        race_info, race_info_df, race_card_df = scraping.scrape_race_card(race_id)

        # 出走馬の過去成績と血統情報を取得
        horse_results = []
        horse_peds = pd.DataFrame()
        horse_ids = race_card_df.at[str(race_id),"horse_id"]
        for horse_id in horse_ids:
            # 過去成績を取得
            horse_results.append(scraping.scrape_horse_results(horse_id))
            # 血統情報を取得
            horse_peds = pd.concat([horse_peds,scraping.scrape_peds(horse_id)], axis = 1)
              
        # 枠順、馬番を取得
        waku_df = pd.concat([race_card_df["枠"].reset_index(drop = True),race_card_df["馬番"].reset_index(drop = True)], axis = 1)
        # AI予想
        rank_df = day_race_prediction.rank_prediction(race_id, horse_ids, race_info_df, waku_df)
        
        # 父，母，母父のみ抽出
        horse_peds_display = get_peds_for_display(horse_peds) 
        # データセットの統合
        race_card_df = pd.concat([race_card_df.reset_index(drop = True), horse_peds_display.T.reset_index(drop = True)], axis = 1)
        race_card_df = pd.concat([race_card_df, rank_df.reset_index(drop=True)], axis = 1)

        # csvファイルで出力
        save_race_cards(race_card_df, date.today(), race_id)

if __name__ == "__main__":
    place_id = 6
    race_day = date.today() - timedelta(4)
    make_race_card(place_id, race_day)
            