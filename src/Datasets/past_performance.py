import os
import sys

from datetime import date, timedelta
import pandas as pd
from tqdm import tqdm

# pycache を生成しない
sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
import get_race_id
import name_header
import scraping
import race_results

def past_performance_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """

    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def make_past_performance_dataset(horse_id):
    """ hotse_idから、過去成績のDataFrameを作成 
        Args:
            horse_id (str) : horse_id

        Returns:
            DataFrame: horse_idの過去成績をDataFrame型で返す    
    """
    url = 'https://db.netkeiba.com/horse/' + horse_id
    # スクレイピング
    horse_results_df = scraping.scrape_df(url)
    # 新馬戦の場合を除外
    if len(horse_results_df) < 4:
        return pd.DataFrame()
    
    # 過去のレース結果テーブルを取得
    df = horse_results_df[3]
    
    #受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
    if df.columns[0]=='受賞歴':
        df = horse_results_df[4]

    # 不要な要素を消去する
    df = df.drop(columns = df.columns[5])
    df = df.drop(columns = df.columns[16 - 1])
    df = df.drop(columns = df.columns[19 - 2])
    df = df.drop(columns = df.columns[24 - 3])
    df = df.drop(columns = df.columns[25 - 4])
    df = df.drop(columns = df.columns[27 - 5])
    df = df.reset_index(drop = True)

    return df

def save_past_performance_dataset(horse_id, past_performance_df):
    """ past_performanceのDataFrameを保存 
        Args:
            horse_id (int) :horse_id
            past_performance_df(pd.DataFrame） : past_performanceのデータセット
    """
    try:
        if any(past_performance_df):
            # csv/pickleに保存
            past_performance_df.to_csv(name_header.DATA_PATH + "PastPerformance\\" + str(horse_id) + '.csv')
            past_performance_df.to_pickle(name_header.DATA_PATH + "PastPerformance\\" + str(horse_id) + '.pickle')
    except Exception as e:
            past_performance_error(e)

def get_horse_id_from_peds_dataset(place_id, year):
    """  過去の結果からhorse_idを抽出
    Args:
        place_id (int) : 開催コースid
        year(int) : 開催年
    Returns:
        list(str): horse_idのリスト    
    """
    # 過去の血統データを取得する
    path = name_header.DATA_PATH + name_header.PLACE_LIST[place_id - 1] + "/" + str(year) + "_peds.csv"
    if os.path.isfile(path):
        df = pd.read_csv(path, index_col = 0)
    else :
        print("not_is_file:" + path)
        return []

    # horse_idのみを抽出し返す
    return df.index

def get_horse_id_list_from_race_id_list(race_id_list):
    """ race_id_listから出走馬のhorse_idを取得
        Args:
            race_id(int) : race_id  
        Returns:
            horse_id_list : horse_idのリスト
    """ 
    try:
        horse_id_list = []
        for race_id in race_id_list:
            horse_id_list.append(get_horse_id_from_race_id(race_id))
        horse_id_list = sum(horse_id_list, [])
        return horse_id_list
    except Exception as e:
            past_performance_error(e)
            return horse_id_list

def get_horse_id_from_race_id(race_id):
    """ race_idtから出走馬のhorse_idを取得
        Args:
            race_id(int) : race_id  
        Returns:
            horse_id_list : horse_idのリスト
    """ 
    horse_id_list = []
    # race_idから年,place_idを抽出
    year = str(race_id)[0] + str(race_id)[1] + str(race_id)[2] + str(race_id)[3] 
    place_id = int(str(race_id)[4] + str(race_id)[5])

    # レース結果を取得
    df = race_results.get_race_results_csv(place_id, year)
    if df.empty:
        return horse_id_list

    # race_idのレースのみを抽出
    df.index = df.index.astype(str)
    df = df[race_id:race_id]
    # horse_idのみを抽出
    horse_id_list = df.loc[:,"horse_id"].to_list()

    return horse_id_list

def make_past_performanece_datasets(horse_id_list):
    """ horse_id_listから過去成績のデータセットを作成  
        Args:
         horse_id_list : horse_idのリスト
    """ 
    for horse_id in tqdm(horse_id_list):
        horse_results_df = make_past_performance_dataset(str(horse_id))
        save_past_performance_dataset(str(horse_id), horse_results_df)

def make_past_performance_dataset_from_race_id_list(race_id_list):
    """ race_id_listから過去成績のデータセットを作成  
        Args:
         race_id_list : race_idのリスト
    """ 
    horse_id_list = get_horse_id_list_from_race_id_list(race_id_list)
    if any(horse_id_list):
        make_past_performanece_datasets(horse_id_list)

def weekly_update_past_performance(day = date.today()):
    """ 指定した日にちから、１週間分のデータセットを更新  
        Args:
         day(Date) : 日（初期値：今日）
    """ 
    for place_id in tqdm(range(1, len(name_header.PLACE_LIST) + 1)):
        print("[WeeklyUpdate]" + name_header.PLACE_LIST[place_id -1] + "PastPerformance")
        race_id_list = get_race_id.get_past_weekly_id(place_id, day)
        make_past_performance_dataset_from_race_id_list(race_id_list)
        
def monthly_update_past_performance(day = date.today()):
    """ 指定した日にちまでのその年のデータセットを更新  
        Args:
        day(Date) : 日（初期値：今日）
    """ 
    for place_id in tqdm(range(1, len(name_header.PLACE_LIST) + 1)):
        print("[MonthlyUpdate]" + name_header.PLACE_LIST[place_id -1] + "PastPerformance")
        race_id_list = get_race_id.get_past_year_id(place_id, day)
        make_past_performance_dataset_from_race_id_list(race_id_list)

def make_all_past_performance(year = date.today().year):
    """ 指定した年までの、すべてのデータセットを作成 
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for y in range(2019, year + 1):
        for place_id in range(1, len(name_header.PLACE_LIST) + 1):
            print("[NewMake]" + str(y) + ":" + name_header.PLACE_LIST[place_id -1] + " PastPerformance")
            race_id_list = get_race_id.get_year_id_all(place_id,y)
            make_past_performance_dataset_from_race_id_list(race_id_list)

if __name__ == "__main__":
    monthly_update_past_performance()
