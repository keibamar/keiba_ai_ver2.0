
import sys

import datetime
from datetime import date, timedelta
from time import sleep
import warnings
warnings.simplefilter('ignore')

sys.dont_write_bytecode = True
sys.path.append(r"C:\keiba_ai\keiba_ai_ver2.0\libs")
import name_header
import post_text

import make_time_id_list
import make_text
import race_card

def post_race_pred(race_id, race_day):
    """レース予想のポスト
        Args:
            race_id(int) : race_id
            race_day(date) : レース開催日
    """
    # テキストファイルの読み込み
    text_path = name_header.TEXT_PATH + "race_prediction/" + race_day.strftime("%Y%m%d") + "/" + str(race_id) + ".txt"
    print(text_path)
    post_text.post_text_data(text_path)

def post_pred_return(place_id, race_day):
    """予想結果の配当のポスト
        Args:
            place_id(int) : place_id
            race_day(date) : レース開催日
    """
    # テキストファイルの読み込み
    text_path = name_header.TEXT_PATH + "race_returns/" + race_day.strftime("%Y%m%d") + "/" + name_header.PLACE_LIST[place_id - 1] + "_pred_score.txt"
    post_text.post_text_data(text_path)

def is_post_race(race_id):
    """ポストするレースか判定する
        Args:
            race_id: race_id
        Returns:
            bool : ポストするか否か
    """
    place_id = int(str(race_id)[4] + str(race_id)[5])
    race_num = int(str(race_id)[10] + str(race_id)[11])
    for post_race_num in name_header.POST_RACE_LIST[place_id - 1]:
        if race_num == post_race_num:
            return True
    return False


def post_daily_race_pred(race_day = date.today()):
    """一日のレースの予想をポスト
        Args:
            race_day(date) : レース開催日(初期値:今日)
    """
    time_id_list = make_time_id_list.get_time_id_list(race_day)

    while(any(time_id_list)):
        # レース10分前に投稿
        comp_time = datetime.datetime.now() + timedelta(minutes=10)
        str_comp_time = str(comp_time.hour).zfill(2) + str(comp_time.minute).zfill(2)
        race_time = time_id_list[0][0]
        # 実行時間を過ぎていたら投稿を実行
        if(int(race_time) <= (int(str_comp_time))):
           race_id = time_id_list[0][1]
           try:
                # 予想の更新
                race_card_df = race_card.make_race_card(race_id)
                # csvファイルで出力
                race_card.save_race_cards(race_card_df, race_day, race_id)
                # textの作成
                make_text.make_race_text(race_day, race_id)
                # API対策で計12レースのみ投稿
                # if len(time_id_list) <= 12:
                if is_post_race(race_id):
                    post_race_pred(race_id, race_day)
                    print("post:" + str(race_time + ":" + str(race_id)))
                else :
                    print("no post for API restricctinos")
           except :
               print("post_error:" + str(race_time + ":" + str(race_id)))
               print(sys.exc_info())
          
           time_id_list.pop(0)
        
        # 1分ごとに実行
        sleep(60)

if __name__ == '__main__':
    post_daily_race_pred()