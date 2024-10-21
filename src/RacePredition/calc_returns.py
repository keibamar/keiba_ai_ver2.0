import os
import re
import sys

from datetime import date, timedelta
import pandas as pd
from tqdm import tqdm
import warnings
warnings.simplefilter('ignore')

sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
import get_race_id
import name_header
import scraping
import race_returns

sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\src\Datasets")
import race_card

def calc_returns_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """
    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def split_selection_returns(df_return, selection):
    """配当結果を指定した式別で分割して返す
        Args:
            df_return(pd.DataFrame) : 配当結果データフレーム
            selection(str) : 式別
        Returns:
            df_selection_return(pd.DataFrame) : 指定した式別のデータフレーム
    """
    temp = df_return[selection:selection].reset_index().T
    df_selection_return = pd.DataFrame()
    try:
        # 式別に応じて調整
        if selection == "枠連" or selection == "馬連" or selection == "ワイド" or selection == "馬単":
            temp.iloc[1,:] = race_returns.replace_br(temp.iloc[1,:].values, 2)
        elif selection == "3連複" or selection == "3連単":
            temp.iloc[1,:] = race_returns.replace_br(temp.iloc[1,:].values, 3)

        for i in range(1, 4):
            df_selection_return = pd.concat([df_selection_return, temp.iloc[i,:].str.split('br', expand = True)])

        # 分割数に応じて列を追加
        title = pd.DataFrame()
        for num in range(len(df_selection_return.T)):
            title = pd.concat([title, pd.DataFrame([selection])])
        df_selection_return = pd.concat([title.reset_index(drop = True),df_selection_return.reset_index(drop = True).T], axis = 1).T.reset_index(drop = True)
        return df_selection_return.T
    except Exception as e:
        calc_returns_error(e)
        return pd.DataFrame()

def get_race_return(race_id):
    """配当結果の取得
        Args: 
            race_id(str) : race_id
        Returns:
            df_returns(pd.DataFrame) : 配当結果
    """
    df_return = scraping.scrape_day_race_returns(race_id)
    df_returns = pd.DataFrame()
    df_returns = pd.concat([df_returns, split_selection_returns(df_return, "単勝")])
    df_returns = pd.concat([df_returns, split_selection_returns(df_return, "複勝")])
    if race_returns.is_bracket_quinella(df_return) :
        df_returns = pd.concat([df_returns, split_selection_returns(df_return, "枠連")])
    df_returns = pd.concat([df_returns, split_selection_returns(df_return, "馬連")])
    df_returns = pd.concat([df_returns, split_selection_returns(df_return, "ワイド")])
    df_returns = pd.concat([df_returns, split_selection_returns(df_return, "馬単")])
    df_returns = pd.concat([df_returns, split_selection_returns(df_return, "3連複")])
    df_returns = pd.concat([df_returns, split_selection_returns(df_return, "3連単")])

    return df_returns.reset_index(drop = True)

# 指数一位馬の単勝回収率・的中率・的中レースを出力
def get_win_result(race_day, race_id_list):
    win_hit_rate = 0
    win_return_rate = 0
    win_hit_race = ""
    race_num_diff = 0
    for race_id in race_id_list:
        # 予想結果の取得
        pred_df = race_card.get_race_cards(race_day, race_id)
        if not "rank" in pred_df.columns:
            print("not rank:" + str(race_id))
            race_num_diff += 1
            continue
        
        # 配当データの取得
        returns_df = get_race_return(race_id)
        if returns_df.empty:
            race_num_diff += 1
            continue

        # 1着的中率・回収率
        win_df = returns_df[returns_df[0] == "単勝"].reset_index(drop = True)
        for i in range(len(win_df)):
            num = int(win_df.at[i,1])
            if pred_df.at[num - 1,"rank"] == 1:
                win_hit_rate += 1
                win_return_rate = win_return_rate + float(win_df.at[i,2])
                win_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + " "
    
    win_hit_rate = (win_hit_rate / (len(race_id_list) - race_num_diff)) 
    win_return_rate = (win_return_rate / (len(race_id_list) - race_num_diff))

    return win_hit_rate, win_return_rate, win_hit_race

# 指数一位馬の複勝回収率・的中率・的中レースを出力
def get_place_result(race_day, race_id_list):
    place_hit_rate = 0
    place_return_rate = 0
    place_hit_race = ""
    race_num_diff = 0
    for race_id in race_id_list:
        # 予想結果の取得
        pred_df = race_card.get_race_cards(race_day, race_id)
        if not "rank" in pred_df.columns:
            print("not rank:" + str(race_id))
            race_num_diff += 1
            continue
        
        # 配当データの取得
        returns_df = get_race_return(race_id)
        if returns_df.empty:
            race_num_diff += 1
            continue
        
       # ３着内率・複勝回収率
        place_df = returns_df[returns_df[0] == "複勝"].reset_index(drop = True)
        for i in range(len(place_df)):
            num = int(place_df.at[i,1])
            if pred_df.at[num - 1, "rank"] == 1:
                place_hit_rate = place_hit_rate + 1
                if type(place_df.at[i,2]) == str:
                    place_df.at[i,2] = re.sub(r"\D","",place_df.at[i,2])
                place_return_rate = place_return_rate + float(place_df.at[i,2])
                place_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + " "
    

    place_hit_rate = (place_hit_rate / (len(race_id_list) - race_num_diff)) 
    place_return_rate = (place_return_rate / (len(race_id_list) - race_num_diff))

    return place_hit_rate, place_return_rate, place_hit_race

# 指数上位5頭の三連複BOXの回収率・的中率・的中レースを出力
def get_trio_box_result(race_day, race_id_list):
    trio5_hit_rate = 0
    trio5_return_rate = 0
    trio5_hit_race = ""
    race_num_diff = 0
    for race_id in race_id_list:
        # 予想結果の取得
        pred_df = race_card.get_race_cards(race_day, race_id)
        if not "rank" in pred_df.columns:
            print("not rank:" + str(race_id))
            race_num_diff += 1
            continue
        
        # 配当データの取得
        returns_df = get_race_return(race_id)
        if returns_df.empty:
            race_num_diff += 1
            continue
        
       # 三連複的中率・回収率(上位3頭/上位5頭BOX)
        trio_df = returns_df[returns_df[0] == "三連複"].reset_index(drop = True)
        for i in range(len(trio_df)):
            # 三連複の馬番の取得
            num_list = re.findall(r"\d+", trio_df.at[i,1])
            # １着馬・２着馬・３着馬の予想順位を取得
            rank_1 = pred_df[pred_df["馬番"] == int(num_list[0])].reset_index(drop = True).at[0 ,"rank"]
            rank_2 = pred_df[pred_df["馬番"] == int(num_list[1])].reset_index(drop = True).at[0 ,"rank"]
            rank_3 = pred_df[pred_df["馬番"] == int(num_list[2])].reset_index(drop = True).at[0 ,"rank"]

            # ランキング５位以内で３頭の場合
            eval_1 = rank_1  <= 5
            eval_2 = rank_2  <= 5
            eval_3 = rank_3  <= 5
            if eval_1 and eval_2 and eval_3:
                trio5_hit_rate +=  1
                if type(trio_df.at[i, 2]) == str:
                    trio_df.at[i, 2] = re.sub(r"\D","",trio_df.at[i, 2])
                trio5_return_rate += float(trio_df.at[i, 2])
                trio5_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + " "
    
    trio5_hit_rate = (trio5_hit_rate / (len(race_id_list) - race_num_diff)) 
    trio5_return_rate = (trio5_return_rate /(len(race_id_list) - race_num_diff)) / 10

    return trio5_hit_rate, trio5_return_rate, trio5_hit_race

# 1日の回収率・的中率を計算
def calc_day_race_return(place_id, str_day, race_id_list):
    sub_num = 0
    # 各的中率・回収率の計算用
    first_hit_rate = 0
    third_hit_rate = 0
    umaren3_hit_rate = 0
    umaren1_3nagashi_hit_rate = 0
    sanrenpuku3_hit_rate = 0
    sanrenpuku5_hit_rate = 0
    sanrenpuku1_3nagashi_hit_rate = 0
    sanrenpuku1_5nagashi_hit_rate = 0

    first_return_rate = 0
    third_return_rate = 0
    umaren3_return_rate = 0
    umaren1_3nagashi_return_rate = 0
    sanrenpuku3_return_rate = 0
    sanrenpuku5_return_rate = 0
    sanrenpuku1_3nagashi_return_rate = 0
    sanrenpuku1_5nagashi_return_rate = 0

    # 的中レースのレース番号を格納
    first_hit_race = ""
    third_hit_race = ""
    umaren3_hit_race = ""
    umaren1_3nagashi_hit_race = ""
    sanrenpuku3_hit_race = ""
    sanrenpuku5_hit_race = ""
    sanrenpuku1_3nagashi_hit_race = ""
    sanrenpuku1_5nagashi_hit_race = ""

    # 各レースの結果を計算
    for race_id in race_id_list:
        # 予想結果の取得
        pred_path = output_path + str_day + "//" + str(race_id) + ".csv"
        if not os.path.isfile(pred_path):
            print("not file:" + str(pred_path))
            sub_num += 1
            continue
        pred_df = pd.read_csv(pred_path, index_col = 0)

        if not "rank" in pred_df.columns:
            print("not rank:" + str(pred_path))
            sub_num += 1
            continue
        # 配当データの取得
        returns_df = get_race_returns(race_id)
        if returns_df.empty:
            continue
        
        # 的中率・回収率の計算
        # 1着的中率・回収率
        first_df = returns_df[returns_df[0] == "単勝"].reset_index(drop = True)
        for i in range(len(first_df)):
            num = int(first_df.at[i,1])
            if pred_df.at[num - 1,"rank"] == 1:
                first_hit_rate += 1
                first_return_rate = first_return_rate + float(first_df.at[i,2])
                first_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","
        
        # ３着内率・複勝回収率
        third_df = returns_df[returns_df[0] == "複勝"].reset_index(drop = True)
        for i in range(len(third_df)):
            num = int(third_df.at[i,1])
            if pred_df.at[num - 1, "rank"] == 1:
                third_hit_rate = third_hit_rate + 1
                if type(third_df.at[i,2]) == str:
                    third_df.at[i,2] = re.sub(r"\D","",third_df.at[i,2])
                third_return_rate = third_return_rate + float(third_df.at[i,2])
                third_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","

        # 馬連的中率・回収率(上位3頭BOX)
        umaren_df = returns_df[returns_df[0] == "馬連"].reset_index(drop = True)
        for i in range(len(umaren_df)):
            # 馬連の馬番の取得
            num_list = re.findall(r"\d+", umaren_df.at[i,1])
            # １着馬・２着馬の予想順位を取得
            rank_1 = pred_df[pred_df["馬番"] == int(num_list[0])].reset_index(drop = True).at[0 ,"rank"]
            rank_2 = pred_df[pred_df["馬番"] == int(num_list[1])].reset_index(drop = True).at[0 ,"rank"]
            
            # ランキング３位以内の２頭の場合
            eval_1 = rank_1  <= 3
            eval_2 = rank_2  <= 3
            
            if eval_1 and eval_2:
                umaren3_hit_rate +=  1
                if type(umaren_df.at[i, 2]) == str:
                    umaren_df.at[i, 2] = re.sub(r"\D","",umaren_df.at[i, 2])
                umaren3_return_rate += float(umaren_df.at[i, 2])
                umaren3_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","

            # 軸１頭(1位)３頭(2～4位)流し
            if (rank_1 == 1 and rank_2 <= 4) or (rank_1 <= 4 and rank_2 == 1):
                umaren1_3nagashi_hit_rate +=  1
                if type(umaren_df.at[i, 2]) == str:
                    umaren_df.at[i, 2] = re.sub(r"\D","",umaren_df.at[i, 2])
                umaren1_3nagashi_return_rate += float(umaren_df.at[i, 2])
                umaren1_3nagashi_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","

        # 三連複的中率・回収率(上位3頭/上位5頭BOX)
        trio_df = returns_df[returns_df[0] == "三連複"].reset_index(drop = True)
        for i in range(len(trio_df)):
            # 三連複の馬番の取得
            num_list = re.findall(r"\d+", trio_df.at[i,1])
            # １着馬・２着馬・３着馬の予想順位を取得
            rank_1 = pred_df[pred_df["馬番"] == int(num_list[0])].reset_index(drop = True).at[0 ,"rank"]
            rank_2 = pred_df[pred_df["馬番"] == int(num_list[1])].reset_index(drop = True).at[0 ,"rank"]
            rank_3 = pred_df[pred_df["馬番"] == int(num_list[2])].reset_index(drop = True).at[0 ,"rank"]
            
            # ランキング３位以内で３頭の場合
            eval_1 = rank_1  <= 3
            eval_2 = rank_2  <= 3
            eval_3 = rank_3  <= 3
            
            if eval_1 and eval_2 and eval_3:
                triopuku3_hit_rate +=  1
                if type(trio_df.at[i, 2]) == str:
                    trio_df.at[i, 2] = re.sub(r"\D","",trio_df.at[i, 2])
                triopuku3_return_rate += float(trio_df.at[i, 2])
                triopuku3_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","

            # ランキング５位以内で３頭の場合
            eval_1 = rank_1  <= 5
            eval_2 = rank_2  <= 5
            eval_3 = rank_3  <= 5

            if eval_1 and eval_2 and eval_3:
                sanrenpuku5_hit_rate +=  1
                if type(sanren_df.at[i, 2]) == str:
                    sanren_df.at[i, 2] = re.sub(r"\D","",sanren_df.at[i, 2])
                sanrenpuku5_return_rate += float(sanren_df.at[i, 2])
                sanrenpuku5_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","
            
            # 軸１頭(1位)３頭(2～4位)流し
            if (rank_1 == 1 and rank_2 <= 4 and rank_3 <= 4) or (rank_1 <= 4 and rank_2 == 1 and rank_3 <= 4) or (rank_1 <= 4 and rank_2 <= 4 and rank_3 == 1):
                sanrenpuku1_3nagashi_hit_rate +=  1
                if type(sanren_df.at[i, 2]) == str:
                    sanren_df.at[i, 2] = re.sub(r"\D","",sanren_df.at[i, 2])
                sanrenpuku1_3nagashi_return_rate += float(sanren_df.at[i, 2])
                sanrenpuku1_3nagashi_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","
            
            # 軸１頭(1位)５頭(2～6位)流し
            if (rank_1 == 1 and rank_2 <= 6 and rank_3 <= 6) or (rank_1 <= 6 and rank_2 == 1 and rank_3 <= 6) or (rank_1 <= 6 and rank_2 <= 6 and rank_3 == 1):
                sanrenpuku1_5nagashi_hit_rate +=  1
                if type(sanren_df.at[i, 2]) == str:
                    sanren_df.at[i, 2] = re.sub(r"\D","",sanren_df.at[i, 2])
                sanrenpuku1_5nagashi_return_rate += float(sanren_df.at[i, 2])
                sanrenpuku1_5nagashi_hit_race += str(int(str(race_id)[10] + str(race_id)[11])) + ","

     # 評価結果の保存
    text_path = output_path + str_day + "//" + header.place_list[place_id - 1] + "_return_rate.txt"
    f = open(text_path, "w", encoding = "UTF-8")
    race_num = len(race_id_list) - sub_num
    f.write("レース数:   " + str(race_num) + "\n")
    f.write("単勝的中率: " + str(first_hit_rate / race_num) + "\n")
    f.write("単勝回収率: " + str(first_return_rate / race_num) + "\n")
    f.write("複勝的中率: " + str(third_hit_rate / race_num) + "\n")
    f.write("複勝回収率: " + str(third_return_rate / race_num) + "\n")
    f.write("馬連上位3頭BOX的中率:    " + str(umaren3_hit_rate / race_num) + "\n")
    f.write("馬連上位3頭BOX回収率:    " + str(umaren3_return_rate / race_num / 3) + "\n")
    f.write("馬連軸1頭-3頭流し的中率: " + str(umaren1_3nagashi_hit_rate / race_num) + "\n")
    f.write("馬連軸1頭-3頭流し回収率: " + str(umaren1_3nagashi_return_rate / race_num / 3) + "\n")
    f.write("三連複上位3頭BOX的中率:  " + str(sanrenpuku3_hit_rate / race_num) + "\n")
    f.write("三連複上位3頭BOX回収率:  " + str(sanrenpuku3_return_rate / race_num) + "\n")
    f.write("三連複上位5頭BOX的中率:  " + str(sanrenpuku5_hit_rate / race_num) + "\n")
    f.write("三連複上位5頭BOX回収率:  " + str(sanrenpuku5_return_rate / race_num / 10) + "\n")
    f.write("三連複軸1頭-3頭流し的中率:  " + str(sanrenpuku1_3nagashi_hit_rate / race_num) + "\n")
    f.write("三連複軸1頭-3頭流し回収率:  " + str(sanrenpuku1_3nagashi_return_rate / race_num / 3) + "\n")
    f.write("三連複軸1頭-5頭流し的中率:  " + str(sanrenpuku1_5nagashi_hit_rate / race_num) + "\n")
    f.write("三連複軸1頭-5頭流し回収率:  " + str(sanrenpuku1_5nagashi_return_rate / race_num / 10) + "\n")
    f.write("\n")
    f.write("単勝的中レース : " + first_hit_race + "\n")
    f.write("複勝的中レース : " + third_hit_race + "\n")
    f.write("馬連上位3頭BOX的中レース : " + umaren3_hit_race + "\n")
    f.write("馬連軸1頭-3頭流し的中レース : " + umaren1_3nagashi_hit_race + "\n")
    f.write("三連複上位3頭BOX的中レース : " + sanrenpuku3_hit_race + "\n")
    f.write("三連複上位5頭BOX的中レース : " + sanrenpuku5_hit_race + "\n")
    f.write("三連複軸1頭-3頭流し的中レース : " + sanrenpuku1_3nagashi_hit_race + "\n")
    f.write("三連複軸1頭-5頭流し的中レース : " + sanrenpuku1_5nagashi_hit_race + "\n")
    f.close()



# 当日の回収率・的中率を計算
def calc_today_race_return(day = date.today()):
    str_day = day.strftime("%Y%m%d")
    # 今日のid_listを取得
    race_id_list = get_race_id.get_daily_id_list(day)

    # 各競馬場ごとにrace_idを分割
    course_list = []
    for i in range(0, len(race_id_list), 12):
        course_list.append(race_id_list[i: i + 12])

    # 各競馬場毎の的中率・回収率の計算
    for race_id_list in course_list:
        print(race_id_list)
        place_id = int(str(race_id_list[0])[4] + str(race_id_list[0])[5])
        calc_day_race_return(place_id, str_day, race_id_list)



