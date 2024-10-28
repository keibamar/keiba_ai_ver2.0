import re
import sys

import datetime
from datetime import date, timedelta
from time import sleep
import warnings
warnings.simplefilter('ignore')

sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
import get_race_id
import name_header
import post_text

import make_text
import race_card

def post_race_pred(race_id, race_day):
    """レース予想のポスト
        Args:
            race_id(int) : race_id
            race_day(date) : レース開催日
    """
    # テキストファイルの読み込み
    text_path = name_header.TEXT_PATH + "race_prediction/" + race_day.strftime("%Y%m%d") + "//" + str(race_id) + ".txt"
    post_text.post_text(text_path)

def post_pred_return(place_id, race_day):
    """予想結果の配当のポスト
        Args:
            place_id(int) : place_id
            race_day(date) : レース開催日
    """
    # テキストファイルの読み込み
    text_path = name_header.TEXT_PATH + "race_returns/" + race_day.strftime("%Y%m%d") + "//" + name_header.PLACE_LIST[place_id - 1] + "_pred_score.txt"
    post_text.post_text(text_path)

def get_race_time(race_id):
    """ 発走時間に取得
        Args:
            race_id(int) : race_id
        Returns:
            race_time(str) : 発走時間
    """
    race_info = race_card.get_race_info(race_id)
    race_time =  str(race_info[1]) + str(race_info[2][0]) + str(race_info[2][1])
    race_time = re.findall(r'\d+', race_time)  # 文字列から数字にマッチするものをリストとして取得
    race_time = ''.join(race_time)
    return race_time

def sort_time(time_id_list):
    """発走時間リストを発走時間順にソート(コース順から発走時間順へソート)
        Args:
            time_id_list(list) : [race_time, race_id]
        Returns:
            time_id_list(list) : ソートされたtime_id_list
    """
    for i in range(len(time_id_list)):
        for l in range(i + 1, len(time_id_list)):
            if (int(time_id_list[i][0]) > int(time_id_list[l][0])):
               temp = time_id_list[i]
               time_id_list[i] = time_id_list[l]
               time_id_list[l] = temp
    
    return time_id_list

def make_time_id_list(race_day = date.today()):
    """time_id_listの作成
        Args:
            race_day(date) : レース開催日(初期値:今日)
        Returns:
            time_id_list(list) : レース開催日の[race_time, race_id]のリスト
    """
    time_id_list = []
    race_id_list = get_race_id.get_today_id_list(race_day)
    for race_id in race_id_list:
        race_time = get_race_time(race_id)
        time_id_list.append([race_time, race_id])
    
    time_id_list = sort_time(time_id_list)
    print(time_id_list)

    return time_id_list

def post_daily_race_pred(race_day = date.today()):
    """一日のレースの予想をポスト
        Args:
            race_day(date) : レース開催日(初期値:今日)
    """
    time_id_list = make_time_id_list(race_day)

    while(any(time_id_list)):
        # レース10分前に投稿
        comp_time = datetime.now() + timedelta(minutes=10)
        str_comp_time = str(comp_time.hour).zfill(2) + str(comp_time.minute).zfill(2)

        # 実行時間を過ぎていたら投稿を実行
        if(int(time_id_list[0][0]) <= (int(str_comp_time))):
           race_id = time_id_list[0][1]
           try:
                # 予想の更新
                race_card.make_race_card(race_id)
                # textの作成
                make_text.make_race_text(race_id, race_day)
                # textのpost
                post_race_pred(race_id, race_day)
                print("post:" + str(time_id_list[0][0] + ":" + str(time_id_list[0][1] )))
           except :
               print("post_error:" + str(time_id_list[0][0] + ":" + str(time_id_list[0][1] )))
               print(sys.exc_info())
          
           time_id_list.pop(0)
        
        # 1分ごとに実行
        sleep(60)

if __name__ == '__main__':
    post_daily_race_pred()
