import sys
sys.path.append("C:\\keiba_ai\\keiba_ai_ver2.0\\src\\Datasets")
sys.dont_write_bytecode = True

import average_time
import race_results
import race_returns
import horse_peds
import peds_results
import past_performance

sys.path.append("C:\\keiba_ai\\keiba_ai_ver2.0\\src\\PredictionModels")
import LightGBM.make_dataset  as LightGBM_dataset

if __name__ == "__main__":  
    # データセットの更新
    race_results.weekly_update_race_results()
    horse_peds.weekly_update_horse_peds()
    peds_results.weekly_update_pedsdata()
    race_returns.weekly_update_race_returns()
    average_time.timedata_update()
    past_performance.weekly_update_past_performance()
    
    # 予想スコアの計算
    LightGBM_dataset.update_dataset_for_train()
    