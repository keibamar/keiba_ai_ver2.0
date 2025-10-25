import pandas as pd
import os
import sys
from glob import glob
import re
from datetime import date,datetime, timedelta
import calendar


# pycache を生成しない
sys.dont_write_bytecode = True
sys.path.append(r"C:\keiba_ai\keiba_ai_ver2.0\libs")
import name_header

# 使用例
if __name__ == "__main__":
    base_path = "../data/RaceCards/"  # 例: racesフォルダ配下の一覧を取得
    folders = get_subfolders(base_path)
    # print(folders)
    for day_str in folders:
        print(day_str)
        input_dir = f"../data/RaceCards/{day_str}"
        output_dir = f"races/{day_str}"

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        files_info_list = list_files_and_parse(input_dir)
        # print(files_info_list)
        # 各日ごとにHTML生成
        for file_info in files_info_list:
            csv_path = input_dir + "/" + str(file_info['file']) + ".csv"
            csv_to_html(csv_path, os.path.join(output_dir, f"{name_header.PLACE_LIST[file_info['place_id'] - 1]}R{file_info['race_num']}.html"), str(day_str), file_info['race_num'], file_info['place_id'] , file_info["file"])
        make_index_page(day_str, output_dir, files_info_list)

        # # 開催日全体の index.html
        # global_index_dir = "races"
        # make_global_index( global_index_dir )
        js_path = "js/raceDays.js"
        add_race_day(js_path, day_str)


