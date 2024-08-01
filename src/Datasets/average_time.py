import sys

import datetime
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
    course_race_results = race_results_df[race_results_df['race_type'] == str(race_type)]
    course_race_results = course_race_results[course_race_results['course_len'] == str(course_len)]
    course_race_results = course_race_results[course_race_results['着順'] == str("1")]

    return course_race_results

def make_avg_time_dataset(race_type, course_len, class_name, avg_time_list):
    avg_time = pd.DataFrame(avg_time_list)
    avg_time.columns = ['avg_time']
    course_data = pd.DataFrame({'race_type': [str(race_type),str(race_type),str(race_type),str(race_type),str(race_type)],
                                'course_len': [str(course_len),str(course_len),str(course_len),str(course_len),str(course_len)],
                                'ground_state':['全', '良', '稍重', '重', '不'],})
    class_data = pd.DataFrame({'class' : [class_name, class_name, class_name, class_name, class_name]})
    # データを結合
    df_avg_time_course = pd.concat([course_data, class_data,  avg_time], axis = 1)

    return df_avg_time_course

def save_annual_average_datasets(df_avg_time, place_id, year):
    df_avg_time  = df_avg_time.reset_index(drop = True)
    df_avg_time.to_csv(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_avg_time.csv')
    df_avg_time.to_pickle(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//' + str(year) + '_avg_time.pickle')

def save_total_average_datasets(df_avg_time, place_id):
    df_avg_time  = df_avg_time.reset_index(drop = True)
    df_avg_time.to_csv(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//total_avg_time.csv')
    df_avg_time.to_pickle(name_header.DATA_PATH + "AverageTimes\\" + name_header.PLACE_LIST[place_id - 1] + '//total_avg_time.pickle')

def get_avg_time_list_from_df(df_course_race_results):
    avg_time_list = []
    all_all_time = df_course_race_results["タイム"].reset_index(drop = True)
    avg_time_list.append(calc_avg_time(all_all_time))
    
    # 全クラス・ 各馬場状態の平均タイム
    for ground_state in name_header.GROUND_STATE_LIST:
        df_temp = df_course_race_results[df_course_race_results['ground_state'] == ground_state]
        time_temp = df_temp["タイム"].reset_index(drop = True)
        avg_time_list.append(calc_avg_time(time_temp))
    
    return avg_time_list

def make_average_time_datasets(df_race_resutls, place_id):
    df_return = pd.DataFrame()
    # コース・距離のデータを抽出
    for course in name_header.COURSE_LISTS[place_id - 1]:
        df_all_all = extract_course_race_results(course[0], course[1], df_race_resutls)
        # 全クラス・全馬場状態の平均タイム
        all_avg_time = get_avg_time_list_from_df(df_all_all)

        # データを結合
        df_return_course = make_avg_time_dataset(course[0], course[1], 'all', all_avg_time)
        df_return = pd.concat([df_return, df_return_course])

        # 各クラスごとの平均タイム
        for class_name in name_header.CLASS_LIST :
            df_all_class = df_all_all[df_all_all['class'] == class_name]
            # 全馬場状態の平均タイム
            class_avg_time = get_avg_time_list_from_df(df_all_class)           
            
            # データを結合
            df_return_course = make_avg_time_dataset(course[0], course[1], class_name, class_avg_time)
            df_return = pd.concat([df_return, df_return_course])
    return df_return


# 型変換して平均時間を計算(ms)
def calc_avg_time(time_data):
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

# 年度ごとの平均タイムを抽出
def make_annual_average_time_datasets(place_id, year):
    df_course = race_results.get_race_results_csv(place_id, year)
    if not df_course.empty:
        df_return = make_average_time_datasets(df_course, place_id)
    
        # ローカル保存
        save_annual_average_datasets(df_return, place_id, year)
            
# 合計の平均タイムを抽出
def total_average_datas(place_id, year):
    df_race_results_all = pd.DataFrame()
    # 各年度のデータを抽出
    for y in range(2019, int(year) + 1):
        df_race_results_year = race_results.get_race_results_csv(place_id, y)
        df_race_results_all = pd.concat([df_race_results_all, df_race_results_year])
    if not df_race_results_all.empty:
        df_return = make_average_time_datasets(df_race_results_all, place_id)
        save_total_average_datasets(df_return, place_id)
   


# 週に一回（金曜0:00)アップデート
def timedata_update(year):
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print("[Update]" +name_header.PLACE_LIST[place_id -1] + " AverageTimes")
        make_annual_average_time_datasets(place_id, year)
        total_average_datas(place_id, year)

if __name__ == "__main__":
    for year in range(2019, 2024 + 1):
        timedata_update(year)