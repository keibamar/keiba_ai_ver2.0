import os
import re
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

def race_returns_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """

    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def type_check(return_df, type):
    """ 式別の有無をチェックする
        Args:
            return_df (DataFrame) : 配当結果のデータフレーム
            type : 配当の識別
        
        Returns:
            Bool : 配当結果にtype識別がある場合True/ない場合False

    """
    for betting_type in return_df.index:
        if str(betting_type) == type:
            return True
    return False

def extratc_race_return_table(race_return_df):
    """ スクレイピングしたテーブルから配当結果を抽出する
    Args:
        race_return_df (DataFrame) : スクレイピングしたのデータフレーム
    
    Returns:
        DataFrame : 配当結果のテーブルのデータフレーム

    """
    try:
        # 単勝、複勝、枠連、馬連のデータ
        return_df1 = race_return_df[1]
        # ワイド、馬単、三連複、三連単のデータ
        return_df2 = race_return_df[2]

        return pd.concat([return_df1,return_df2]).reset_index(drop = True)
    except:
        return race_return_df

def save_race_returns_dataset(place_id, year, race_returns_df):
    """ race_returnsのDataFrameを保存 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
            race_returns_df（pd.DataFrame） : race_resultデータセット
    """
    try:
        if any(race_returns_df):
            # 重複している内容を消去
            race_returns_df = race_returns_df.drop_duplicates(keep = 'first')
            # csv/pickleに保存
            race_returns_df.to_csv(name_header.DATA_PATH + "RaceReturns\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_race_returns.csv')
            race_returns_df.to_pickle(name_header.DATA_PATH + "RaceReturns\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_race_returns.pickle')
    except Exception as e:
            race_returns_error(e)

def replace_br(string, num):
    """ 指定個数のbrで文字列を分割 
        Args:
            string : 文字列
            num : 分割する文字の個数
        Returns:
            result_str_list : 指定文字数で分割した文字列 
    """
    result_str_list = []
    for temp in string:
        # brで文字列を分割
        str_list = re.split("br", temp)
        result_str = ""
        # 指定個数のbr感覚で指定文字を付与
        for i in range(len(str_list)):
            # 分割数 * 2 - 1 個で1セット
            if i % (2 * num - 1) == 2 * num - 2 and i != len(str_list) - 1:
                result_str += str_list[i] + "br"
            else :
                result_str += str_list[i] 
        result_str_list.append(result_str)
    
    return result_str_list

def format_type_returns_dataframe(return_df, type):
    """ 各式別フォーマットの整形
        Args:
            return_df : 配当結果のデータフレーム
            tyoe : 式別
        Returns:
            return_df : 指定式別のフォーマットを整理したDataFrame 
    """

    try:
        # 各式別毎に馬番を分割
        if type == "枠連" or type == "馬連" or type == "ワイド" or type == "馬単":
            return_df.iloc[1,:] = replace_br(return_df.iloc[1,:].values, 2)
        elif type == "三連複" or type == "三連単":
            return_df.iloc[1,:] = replace_br(return_df.iloc[1,:].values, 3)
       
        # 各式別の数分割する(同着の場合複数)
        split_results = pd.DataFrame()
        for i in range(1, 4):
            split_results = pd.concat([split_results, return_df.iloc[i,:].str.split('br', expand = True)])

        # 分割数に応じて列を追加
        type_result = pd.DataFrame()
        for num in range(len(split_results.T)):
            type_result = pd.concat([type_result, pd.DataFrame([type])])
        type_result = pd.concat([type_result.reset_index(drop = True),split_results.reset_index(drop = True).T], axis = 1).T.reset_index(drop = True)
        
        return type_result.T
    except Exception as e:
        race_returns_error(e)
        return return_df

def format_race_return_dataframe(race_return_df):
    """ 配当結果のフォーマットを整える
        Args:
            race_return_df : 配当結果のデータフレーム
        Returns:
            race_return_df : 指定式別のフォーマットを整理したDataFrame 
    """
    try:
        # 不要な文字の消去
        for i in range(len(race_return_df.index)):
            race_return_df.at[i,1] = race_return_df.at[i,1].replace(' ', 'br')
            race_return_df.at[i,2] = race_return_df.at[i,2].replace(' ', 'br')
            race_return_df.at[i,3] = race_return_df.at[i,3].replace(' ', 'br')
            race_return_df.at[i,2] = race_return_df.at[i,2].replace('円', '')
            race_return_df.at[i,2] = race_return_df.at[i,2].replace(',', '')
        race_return_df = race_return_df.set_index(0)
        return race_return_df
    except:
        return pd.DataFrame()

def scrape_race_returns_dataframe(race_id_list):
    """ race_id_listから配当結果を取得する
        Args:
            race_id_list : race_idのlist
        Returns:
            race_return_df : 配当結果のデータフレーム
    """
    return_tables = {}
    for race_id in tqdm(race_id_list):
        # レース結果のスクレイピング
        url =  "https://db.netkeiba.com/race/" + str(race_id)
        race_return_df = scraping.scrape_df(url)
        # 配当結果の抽出
        race_return_df = extratc_race_return_table(race_return_df)
        # フォーマットを整える
        race_return_df = format_race_return_dataframe(race_return_df)
        if not race_return_df.empty:
            format_race_return_df = pd.DataFrame()
            # 各式別のフォーマットを整形
            for type in name_header.BETTING_TYPE_LIST:
                if type_check(race_return_df, type):
                    format_race_return_df = pd.concat([format_race_return_df, format_type_returns_dataframe(race_return_df[type:type].reset_index().T, type).set_index(0)])
            # race_idをインデックスにする
            format_race_return_df = format_race_return_df.reset_index()
            format_race_return_df.index = [race_id] * len(format_race_return_df)
            return_tables[race_id] = format_race_return_df
    #pd.DataFrame型にして一つのデータにまとめる
    if any(return_tables):
        return_tables_df = pd.concat([return_tables[key] for key in return_tables])
        return return_tables_df
    else:
        return return_tables

def get_race_returns_csv(place_id, year):

    """ race_returnsのcsvを取得する 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
    """
    # csvを読み込む 
    path = name_header.DATA_PATH + "RaceReturns\\" + name_header.PLACE_LIST[place_id - 1] +  '//' + str(year) + '_race_returns.csv'
    if os.path.isfile(path):
        df = pd.read_csv(path, index_col = 0, dtype = str)
    else :
        df = pd.DataFrame()

    return df

def update_race_returns_dataset(place_id, day = date.today()):
    """ 開催コースと日にちを指定して、過去1週間分のrace_returnsデータセットを更新する 
        Args:
            place_id (int) : 開催コースid
            day(int) : 日（初期値：今日）
    """

    # race_id_listの取得
    race_id_list = get_race_id.get_past_weekly_id(place_id, day)

    # 過去のデータセットを取得
    old_race_returns_df = get_race_returns_csv(place_id, day.year)

    # レース結果の取得
    new_race_returns_df = scrape_race_returns_dataframe(race_id_list)

    # 更新するデータセットがあれば更新
    if any(new_race_returns_df):
        try:
            # columnsをそろえてデータセットを統合
            new_race_returns_df.columns = old_race_returns_df.columns
            new_race_returns_df = pd.concat([old_race_returns_df,new_race_returns_df],axis = 0)

            # csv/pickleに書き込む
            save_race_returns_dataset(place_id, day.year, new_race_returns_df)
        except Exception as e:
            race_returns_error(e)

def make_yearly_race_returns_dataset(place_id, year = date.today().year):
    """ 開催コースと年を指定して、1年間のrace_returnsデータセットを作成 
        Args:
            place_id (int) : 開催コースid
            year(int) : 年（初期値：今年）
    """
    #race_id_listの取得
    race_id_list = get_race_id.get_year_id_all(place_id, year)

    # スクレイピング
    race_returns_df = scrape_race_returns_dataframe(race_id_list)

    # csv/pickleに書き込む
    save_race_returns_dataset(place_id, year, race_returns_df)

def make_up_to_day_dataset(place_id, day = date.today()):
    """ 指定日までの、年間のrace_returnsデータセットを作成 
        Args:
            place_id (int) : 開催コースid
            day(int) : 日（初期値：今日）
    """
    #race_id_listの取得
    race_id_list = get_race_id.get_past_year_id(place_id, day)

    # スクレイピング
    race_returns_df = scrape_race_returns_dataframe(race_id_list)

    try:
        # csv/pickleに書き込む
        save_race_returns_dataset(place_id, day.year, race_returns_df)
    except Exception as e:
            race_returns_error(e) 

def weekly_update_race_returns(day = date.today()):
    """ 指定した日にちから、１週間分のデータセットを更新  
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for place_id in range(1, 11):
        print("[WeeklyUpdate]" + name_header.PLACE_LIST[place_id -1] + " RaceReturns")
        update_race_returns_dataset(place_id, day)

def montly_update_race_returns(day = date.today()):
    """ 指定した日にちまでのその年のデータセットを更新  
    Args:
    day(Date) : 日（初期値：今日）
    """ 
    for place_id in range(1, 11):
        print("[MonthlyUpdate]" + name_header.PLACE_LIST[place_id -1] + " RaceReturns")
        make_up_to_day_dataset(place_id, day)

def make_all_race_returns(year = date.today().year):
    """ 指定した年までの、すべてのデータセットを作成 
    Args:
        day(Date) : 日（初期値：今日）
    """ 
    for y in range(2019, year + 1):
        for place_id in range(1, 11):
            print("[NewMake]" + str(y) + ":" + name_header.PLACE_LIST[place_id -1] + " RaceReturns")
            make_yearly_race_returns_dataset(place_id, y)

if __name__ == "__main__":
    weekly_update_race_returns()
