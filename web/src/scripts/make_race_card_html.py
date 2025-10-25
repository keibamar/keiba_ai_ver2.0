import os
import sys
from datetime import date, datetime, timedelta

# pycache を生成しない
sys.dont_write_bytecode = True

# web/src を import path に追加（generators パッケージを解決）
PROJECT_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

from generators.date_index import add_race_day    
from generators.daily_index import make_daily_index_page
from generators.race_pages import make_daily_race_card_html 

# 使用例
if __name__ == "__main__":
    race_day = date.today()  - timedelta(days=6)
    print(race_day)
    # 各レースのHTMLを生成
    make_daily_race_card_html(race_day)
    # 日付別インデックスページを生成
    make_daily_index_page(race_day)
    # 全開催日インデックスページを更新
    add_race_day(race_day)
