import sys

import datetime
from datetime import date, timedelta
import numpy as np
import pandas as pd

# pycache を生成しない
sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
import name_header
import race_results


def average_time_dataset_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """
    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def extract_course_race_results(race_type, course_len, race_results_df):
    """ race_resultsデータセットからコースの情報を抽出する 
        Args:
            race_type(str) : 芝/ダート
            course_len(str) : 距離
            race_results_df(pd.DataFrame) : race_resultsデータセット
        Returns:
            course_race_results(pd.DataFrame) : 該当コースのrace_resultsデータセット
    """
    course_race_results = race_results_df[race_results_df['race_type'] == str(race_type)]
    course_race_results = course_race_results[course_race_results['course_len'] == str(course_len)]
    course_race_results = course_race_results[course_race_results['着順'] == str("1")]

    return course_race_results

def make_avg_time_dataset(race_type, course_len, class_name, avg_time_list):
    """ 平均タイムリストからデータセットを作成する 
        Args:
            race_type(str) : 芝/ダート
            course_len(str) : 距離
            class_name(str) : クラス名
            avg_time_list(liat) : 平均タイムのリストデータセット
        Returns:
                return df_avg_time_course(pd.DataFrame) : 該当コースのaverage_timeデータセット
    """
    avg_time = pd.DataFrame(avg_time_list)
    avg_time.columns = ['avg_time']
    course_data = pd.DataFrame({'race_type': [str(race_type),str(race_type),str(race_type),str(race_type),str(race_type)],
                                'course_len': [str(course_len),str(course_len),str(course_len),str(course_len),str(course_len)],
                                'ground_state':['全', '良', '稍重', '重', '不'],})
    class_data = pd.DataFrame({'class' : [class_name, class_name, class_name, class_name, class_name]})
    # データを結合
    df_avg_time_course = pd.concat([course_data, class_data,  avg_time], axis = 1)

    return df_avg_time_course

def save_annual_average_datasets(place_id, year, df_avg_time):
    """ 年間の平均タイムのDataFrameを保存 
        Args:
            place_id (int) : 開催コースid
            year(int) : 開催年
            df_avg_time（pd.DataFrame） : average_timeデータセット
    """
    try:
        if any(df_avg_time):
            # indexをリセット
            df_avg_time  = df_avg_time.reset_index(drop = True)
            # csv/pickleに保存
            df_avg_time.to_csv(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_avg_time.csv')
            df_avg_time.to_pickle(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_avg_time.pickle')
    except Exception as e:
        average_time_dataset_error(e)

def save_total_average_datasets(place_id, df_avg_time):
    """ totalの平均タイムのDataFrameを保存 
        Args:
            place_id (int) : 開催コースid
            df_avg_time（pd.DataFrame） : average_timeデータセット
    """
    try:
        if any(df_avg_time):
            # indexをリセット
            df_avg_time  = df_avg_time.reset_index(drop = True)
            # csv/pickleに保存
            df_avg_time.to_csv(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//total_avg_time.csv')
            df_avg_time.to_pickle(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//total_avg_time.pickle')
    except Exception as e:
        average_time_dataset_error(e)

def get_avg_time_list_from_race_results_df(df_course_race_results):
    """ race_resultsから平均タイムリストを作成するのフォーマットを整える 
        Args:
            df_course_race_results (pd.DataFrame） : 各コースのrace_resultデータセット
        Returns:
            avg_time_list(list) : 平均タイムのリスト
    """
    # 全馬場状態の平均タイム
    avg_time_list = []
    all_all_time = df_course_race_results["タイム"].reset_index(drop = True)
    avg_time_list.append(calc_avg_time(all_all_time))
    
    # 各馬場状態の平均タイム
    for ground_state in name_header.GROUND_STATE_LIST:
        df_temp = df_course_race_results[df_course_race_results['ground_state'] == ground_state]
        time_temp = df_temp["タイム"].reset_index(drop = True)
        avg_time_list.append(calc_avg_time(time_temp))
    
    return avg_time_list

def make_average_time_datasets(df_race_resutls, place_id):
    """ race_resultsからaverage_timeデータセットを作成する 
        Args:
            df_course_race_results (pd.DataFrame） : 各コースのrace_resultデータセット
        Returns:
            df_avg_time (pd.DataFrame) : average_timeデータセット
    """
    df_avg_time = pd.DataFrame()
    # コース・距離のデータを抽出
    for course in name_header.COURSE_LISTS[place_id - 1]:
        df_all_all = extract_course_race_results(course[0], course[1], df_race_resutls)
        # 全クラス・全馬場状態の平均タイム
        all_avg_time = get_avg_time_list_from_race_results_df(df_all_all)

        # データを結合
        df_return_course = make_avg_time_dataset(course[0], course[1], 'all', all_avg_time)
        df_avg_time = pd.concat([df_avg_time, df_return_course])

        # 各クラスごとの平均タイム
        for class_name in name_header.CLASS_LIST :
            df_all_class = df_all_all[df_all_all['class'] == class_name]
            # 全馬場状態の平均タイム
            class_avg_time = get_avg_time_list_from_race_results_df(df_all_class)           
            
            # データを結合
            df_return_course = make_avg_time_dataset(course[0], course[1], class_name, class_avg_time)
            df_avg_time = pd.concat([df_avg_time, df_return_course])
    return df_avg_time

def calc_avg_time(time_data):
    """ time_dataから平均タイムを計算する 
        Args:
            time_data (pd.DataFrame） : 走破時計のデータセット
        Returns:
            avg_time (int) : 平均タイム(ms)
    """
    if len(time_data) > 0:
        # 時間の型変換
        time_format = '%H:%M:%S.%f'
        for i in range (len(time_data)):
            time_data[i] = datetime.datetime.strptime(time_data.iloc[i], time_format)
        # 平均時間の計算
        time_data = time_data.astype("datetime64[ms]").to_numpy()
        base_time = np.datetime64(0, 'ms')
        avg_time = ((time_data - base_time) % np.timedelta64(1, 'D')).mean()
        return avg_time.astype(int)
    else:
        return np.timedelta64('NaT')

def make_annual_average_time_datasets(place_id, year = date.today().year):
    """ 指定したコース、年の、年度ごとの平均タイムデータセットを作成 
        Args:
            place_id (int) : 開催コースid
            year(int) : 年（初期値：今年）
    """ 
    # レース結果の取得
    df_course = race_results.get_race_results_csv(place_id, year)
    if not df_course.empty:
        # データセットの作成
        df_return = make_average_time_datasets(df_course, place_id)
        # ローカル保存
        save_annual_average_datasets(place_id, year, df_return)
            
def total_average_datas(place_id, year = date.today().year):
    """ 指定したコースの合計の平均タイムデータセットを作成 
        Args:
            place_id (int) : 開催コースid
            year(int) : 年（初期値：今年）
    """
    df_race_results_all = pd.DataFrame()
    # 各年度のレース結果の取得
    for y in range(2019, int(year) + 1):
        df_race_results_year = race_results.get_race_results_csv(place_id, y)
        df_race_results_all = pd.concat([df_race_results_all, df_race_results_year])
    
    if not df_race_results_all.empty:
        # データセットの作成
        df_return = make_average_time_datasets(df_race_results_all, place_id)
        # ローカル保存
        save_total_average_datasets(place_id, df_return)
   
def timedata_update(year = date.today().year):
    """ 指定した年の平均タイムデータセットを更新 
        Args:
            year(int) : 年（初期値：今年）
    """
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print("[Update]" +name_header.PLACE_LIST[place_id -1] + " AverageTimes")
        make_annual_average_time_datasets(place_id, year)
        total_average_datas(place_id, year)

if __name__ == "__main__":
    for year in range(2019, 2024 + 1):
        timedata_update(year)