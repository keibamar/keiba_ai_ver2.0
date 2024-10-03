import os
import sys

from datetime import date, timedelta
import pandas as pd
import warnings
warnings.simplefilter('ignore')

sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
import name_header
import get_race_id
import scraping

import make_race_card

def get_race_info(race_id):
    """レース情報(レース名・発走時刻)を取得
        Args:
            race_id(int) : race_id
        Returns:
            race_info(list) : レース情報
    """
    race_info, race_info_df, shutubahyou_df = scraping.scrape_race_card(race_id)
    return race_info

def extract_top5_pred(race_data_df):
    """予想結果の上位5頭のリストを返す
        Args:
            race_data_df(pd.DataFrame) : 出馬表データセット
        Returns:
            result_list(pd.DataFrame) : 上位5頭のデータセット(昇順)
    """
    result_list = []
    for i in range(1, 6):
        temp = race_data_df[race_data_df["rank"] == i].reset_index(drop = True)
        if not temp.empty:
            num = temp.at[0,"馬番"]
            name = temp.at[0,"馬名"]
            result_list.append([num, name])
    return result_list

def make_race_text(race_day, race_id):
    """レースの予想のテキスト作成
        Args:
            race_day(date) : レース開催日
            race_id(int) : race_id
    """
    # 予想結果を抽出
    race_data_df = make_race_card.get_race_cards(race_day, race_id)
    if not "rank" in race_data_df.columns:
        print("not rank:" + str(race_id))
        return

    # 予想結果から上位5頭を抽出
    pred_list = extract_top5_pred(race_data_df)

    # テキストファイルの準備
    folder_path = name_header.TEXT_PATH + "race_prediction/" + race_day.strftime("%Y%m%d")
    if not os.path.isdir(folder_path):
        os.mkdir(folder_path)
    text_data_path = folder_path + "//" + str(race_id) + ".txt"
    
    f = open(text_data_path, "w", encoding = "UTF-8")

    # 開催情報の抽出
    place_id = int(str(race_id)[4] + str(race_id)[5])
    race_num = int(str(race_id)[10] + str(race_id)[11])
    race_info = get_race_info(race_id)
    race_name = race_info[0]
    start_time = str(race_info[1]) + ":" + str(race_info[2])

    # 日付の出力
    f.write(str(race_day.year) + "/" + str(race_day.month) + "/" + str(race_day.day) + "\n")
    # 開催情報の出力
    f.write(name_header.NAME_LIST[place_id - 1] + str(race_num) + "R" + " " + race_name + " " + start_time + "\n\n")
    # 予想の出力
    for rank in range(5):
        if rank < len(pred_list):
            f.write(" " + name_header.SYMBOL_LIST[rank] + " " + str(pred_list[rank][0]) + " " + pred_list[rank][1] + "\n")
    f.write("\n\n")

    # タグの出力
    f.write("#MAR競馬予想\n")
    f.write("#競馬予想AI\n")
    f.write("#競馬 #競馬予想\n")
    f.write("#" + name_header.NAME_LIST[place_id - 1] + "競馬場\n")

    # メインレースのみ名前を取得
    if str(race_num) == "11" :
        f.write("#" + race_name + "\n")
 
    f.close()

if __name__ == "__main__":
    place_id = 6
    race_day = date.today() - timedelta(5)
    race_id_list = get_race_id.get_daily_id(place_id, race_day)
    for race_id in  race_id_list:
        make_race_text(race_id, race_day)