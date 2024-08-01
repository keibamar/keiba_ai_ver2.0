import re
import sys
import warnings

from bs4 import BeautifulSoup
import pandas as pd
import requests


# pycache を生成しない
sys.dont_write_bytecode = True
# エラー表記をしない
warnings.simplefilter('ignore')

def scraping_error(e):
    """ エラー時動作を記載する 
        Args:
            e (Exception) : エラー内容 
    """

    print(__name__ + ":" + __file__)
    print(f"{e.__class__.__name__}: {e}")

def scrape_df(url):

    """ urlからスクレイピングをし、DataFrameを返す 
        Args:
            url (str) : スクレイピングするurl

        Returns:
            DataFrame: url先のテーブルデータをDataFrame型で返す    
        """
    try:
        # urlからスクレイピング
        html = requests.get(url)
        html.encoding = "EUC-JP"
        soup = BeautifulSoup(html.text, "html.parser")
        # バグ対策でdecode
        soup = BeautifulSoup(html.content.decode("euc-jp", "ignore"), "html.parser")
        
        # テーブルデータを取得
        df = [pd.read_html(str(t))[0] for t in soup.select('table:has(tr td)')]
        return df
    
    except Exception as e:
        scraping_error(e)
        return pd.DataFrame()

def scrape_race_results(race_id):

    """ urlからスクレイピングをし、レース結果、レース情報、horse_idを統合して返す 
    Args:
        race_id (str) : スクレイピングするrace_id

    Returns:
        DataFrame: url先のレース結果をレース情報と、horse_idを統合したDataFrame型で返す    
    """                     

    try:
        url = "https://db.netkeiba.com/race/" + str(race_id)
        # urlからスクレイピング
        html = requests.get(url)
        html.encoding = "EUC-JP"
        soup = BeautifulSoup(html.text, "html.parser")
        # バグ対策でdecode
        soup = BeautifulSoup(html.content.decode("euc-jp", "ignore"), "html.parser")
        
        # メインとなるテーブルデータを取得
        df_results = [pd.read_html(str(t))[0] for t in soup.select('table:has(tr td)')][0]
        # 列名に半角スペースがあれば除去する
        df_results = df_results.rename(columns=lambda x: x.replace(' ', ''))

        for i in range(len(df_results)):
            # 時間表記を変更
            if df_results.notnull().at[i,"タイム"]:
                df_results.at[i,"タイム"] = "0:" + df_results.at[i,"タイム"]
            # 馬体重増減削除
            if df_results.notnull().at[i,"馬体重"]:
                temp = df_results.at[i,"馬体重"]
                # 括弧とその中身を含む正規表現パターンを定義する
                pattern = r'\([^()]*\)'
                # 正規表現パターンに一致する部分を空文字列で置換する
                df_results.at[i,"馬体重"] = re.sub(pattern, '', temp)

        # 天候、レースの種類、コースの長さ、馬場の状態、日付を取得する
        texts = (
            soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
            + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
        )
        info = re.findall(r'\w+', texts)

        for text in info:
            # print(text)
            if text in ["芝", "ダート"]:
                df_results["race_type"] = [text] * len(df_results)
            if "障" in text:
                df_results["race_type"] = ["障害"] * len(df_results)
            if "m" in text:
                df_results["course_len"] = [int(re.findall(r"\d+", text)[-1])] * len(df_results)
            if text in ["2歳新馬","3歳新馬"]:
                df_results["class"] = ["新馬"] * len(df_results)
            if text in ["2歳未勝利","3歳未勝利","障害3歳以上未勝利","障害4歳以上未勝利"]:
                df_results["class"] = ["未勝利"] * len(df_results)
            if text in ["2歳1勝クラス","3歳1勝クラス","3歳以上1勝クラス","4歳以上1勝クラス"]:
                df_results["class"] = ["1勝クラス"] * len(df_results)
            if text in ["3歳2勝クラス","3歳以上2勝クラス","4歳以上2勝クラス"]:
                df_results["class"] = ["2勝クラス"] * len(df_results)   
            if text in ["3歳以上3勝クラス","4歳以上3勝クラス"]:
                df_results["class"] = ["3勝クラス"] * len(df_results)    
            if text in ["2歳オープン","3歳オープン","3歳以上オープン","4歳以上オープン","障害3歳以上オープン","障害4歳以上オープン"]:
                df_results["class"] = ["オープン"] * len(df_results)         
            if text in ["良", "稍重", "重", "不良"]:
                df_results["ground_state"] = [text] * len(df_results)
            if text in ["曇", "晴", "雨", "小雨", "小雪", "雪"]:
                df_results["weather"] = [text] * len(df_results)
            if "年" in text:
                df_results["date"] = [text] * len(df_results)
        
        #馬ID、騎手IDをスクレイピング
        horse_id_list = []
        horse_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
            "a", attrs={"href": re.compile("^/horse")}
        )
        for a in horse_a_list:
            horse_id = re.findall(r"\d+", a["href"])
            horse_id_list.append(horse_id[0])
        jockey_id_list = []
        jockey_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
            "a", attrs={"href": re.compile("^/jockey")}
        )
        for a in jockey_a_list:
            jockey_id = re.findall(r"\d+", a["href"])
            jockey_id_list.append(jockey_id[0])
        df_results["horse_id"] = horse_id_list
        df_results["jockey_id"] = jockey_id_list

        #インデックスをrace_idにする
        df_results.index = [race_id] * len(df_results)

        return df_results
    
    except Exception as e:
        # scraping_error(e)
        return pd.DataFrame()

if __name__ == "__main__":
    print(scrape_race_results(str(202402011211)))