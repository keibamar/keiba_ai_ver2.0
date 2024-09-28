import itertools
import os
import re
import sys
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

from datetime import date, timedelta
import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import rankdata
from tqdm import tqdm

# pycache を生成しない
sys.dont_write_bytecode = True
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\libs")
sys.path.append("C:\keiba_ai\keiba_ai_ver2.0\src\Datasets")
import name_header

import race_results
import make_dataset

def get_lightGBM_model(place_id, type_str, length):
    model_path = name_header.DATA_PATH + "/PredictionModels/LightGBM/Models/" + name_header.PLACE_LIST[place_id - 1] + '//' + str(type_str) + str(length) + "_lambdarank_model.txt"
    model = lgb.Booster(model_file = model_path)
    return model

# スコアの順位を計算
def rank_index(index_list):
    rank = rankdata(index_list)
    # 降順の計算
    rank = (len(rank) - rank + 1).astype(int)
    return list(rank)

def data_split(df):
    # 説明変数の項目名を取得
    feature = list(df.drop(["race_id"], axis=1).columns)

    group = df['race_id'].value_counts()
    df = df.set_index(['race_id'])
    group = group.sort_index()

    # 説明変数(x)と目的変数(y)をインデックスでソート
    df.sort_index(inplace=True)
    # 説明変数(x)と目的変数(y)を設定
    x = df[feature]

    return x, group

# モデルの学習
def lightGBM_rank_train(place_id, year = date.today().year):
    for type, length  in name_header.COURSE_LISTS[place_id - 1]:
        print(type, length)
        X,Y = pd.DataFrame(), pd.DataFrame()
        # データの読み込み
        for y in range(2020, int(year) + 1):
            X = pd.concat([X, make_dataset.get_LightGBM_dataset_csv(place_id, y, type, length)])
            Y = pd.concat([Y, make_dataset.get_LightGBM_dataset_flag_csv(place_id, y, type, length)])
        # nanを-1に変換
        X = X.fillna(-1)
        X = X.reset_index(drop = True)
        Y = Y.reset_index(drop = True)
        if X.empty or Y.empty:
            continue
        # 訓練データとテストデータを分割
        test_ratio = 0.2
        test_size = int(len(X.index) * test_ratio)
        test_flag = True
        while X.at[test_size - 1, "race_id"] == X.at[test_size, "race_id"]:
            test_size = test_size + 1
            if test_size == len(X.index):
                test_flag = False
                break
        if test_flag == False:
            continue

        x_test = X.iloc[0 : test_size, :]
        x_train = X.iloc[test_size : len(X.index), :]
        y_test = Y.iloc[0 : test_size, :]
        y_train = Y.iloc[test_size : len(X.index), :]

        # データのグループを作成
        x_train, train_group = data_split(x_train)
        x_test, test_group = data_split(x_test)
        
        print(train_group)
        print(y_train)
        # モデルの学習
        model = lgb.LGBMRanker(random_state=0, force_col_wise=True)
        model.fit(x_train, y_train, group = train_group, eval_set = [(x_test, y_test)], eval_group = [list(test_group)])
        if type == "芝":
            type = "turf"
        else :
            type = "dirt"
        model_path = name_header.DATA_PATH + "/PredictionModels/LightGBM/Models/" + name_header.PLACE_LIST[place_id - 1] + '//' + str(type) + str(length) + "_lambdarank_model.txt"
        model.booster_.save_model(model_path)


# ランキングの推定
def prediction_rank(place_id, year = date.today().year):
    # ranking結果データセットの作成
    result_df = race_results.get_race_results_csv(place_id, year)

    for type, length  in name_header.COURSE_LISTS[place_id - 1]:
        print(type, length)
        # データの読み込み
        X = make_dataset.get_LightGBM_dataset_csv(place_id, year, type, length)
        Y = make_dataset.get_LightGBM_dataset_flag_csv(place_id, year, type, length)
        if X.empty or Y.empty:
            continue

        # データのグループを作成
        x_eval, eval_group = data_split(X)
        
        # モデル読み込み
        if type == "芝":
            type_str = "turf"
        else :
            type_str = "dirt"
        bst = get_lightGBM_model(place_id, type_str, length)

        sum = 0
        rank_list = []
        score_list = []
        
        for race_num in range(len(eval_group)):
            # 1レース分のデータを取得
            x = x_eval.iloc[sum : sum + eval_group.iloc[race_num], : ]
            y = Y.iloc[sum : sum + eval_group.iloc[race_num], : ].reset_index(drop = True)
            sum += eval_group.iloc[race_num]
            
            # テストデータの予測
            y_pred = bst.predict(x, num_iteration = bst.best_iteration)
            # y_pred = bst.predict(xgb.DMatrix(x))
            rank = rank_index(y_pred)

            # 予想順位を保存
            score_list.append(y_pred)
            rank_list.append(rank)
            
            # 予想順位の評価
            row = pd.concat([pd.DataFrame(y_pred, columns = ["score"]), y], axis = 1)
            row = row.sort_values(by="score", ascending=False).reset_index(drop = True)
        
        
        # リストの１次元化
        score_list = list(itertools.chain.from_iterable(score_list))
        rank_list = list(itertools.chain.from_iterable(rank_list))     
        print(result_df)
        result_df_course = result_df[result_df["race_type"] == type]
        result_df_course = result_df_course[result_df_course["course_len"] == str(length)]
        print(result_df_course)
        race_id_list = pd.DataFrame(result_df_course.index, columns = ["race_id"])
        result_df_course = result_df_course[["着順","馬番"]].reset_index(drop = True)

        rank_df = pd.concat([race_id_list, result_df_course, pd.DataFrame(score_list, columns = ["score"]), pd.DataFrame(rank_list, columns = ["pred_rank"])], axis = 1)

        # ranking結果データセットの出力
        rank_path = name_header.DATA_PATH + "/PredictionModels/LightGBM/Models/" + name_header.PLACE_LIST[place_id - 1] + "//"  + str(year) + "_" + str(type) + str(length) + "_pred_rank.csv"
        rank_df.to_csv(rank_path)



if __name__ == "__main__":
    for place_id in range(1, len(name_header.PLACE_LIST) + 1):
        print(name_header.PLACE_LIST[place_id - 1])
        prediction_rank(place_id, 2023)