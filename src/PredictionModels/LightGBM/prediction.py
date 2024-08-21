# 着度数から指数を計算
def peds_placed_rate_to_peds_score(score):
    if sum(score) == 0 :
        return np.nan
    else:
        if score[0] == 0:
            first = 0
        else :
            first  = score[0] * score[0] * weight_first / (sum(score) * (score[0] + score[3])) 
        if score[1] == 0:
            second = 0
        else :
            second = score[1] * score[1] * weight_second / (sum(score) * (score[1] + score[3]))
        if score[2] == 0:
            third = 0
        else :
            third  = score[2] * score[2] * weight_third / (sum(score) * (score[2] + score[3]))
        return (first + second + third) / 3.0  
