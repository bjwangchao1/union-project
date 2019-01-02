import pandas as pd
import numpy as np


class QddScorecard(object):
    """
    钱到到经营数据评分卡
    """

    def __init__(self, var_data):
        self.var_data = var_data

    def dim_score_sum_score(self):
        var_score_data = self.var_data.assign(
            saas_use_score=lambda x: x.saas_use_duration.apply(self.saas_use_time_score),
            business_days_score=lambda x: x.business_days_180d.apply(self.business_days_180d_score),
            daily_turnover_avg_score=lambda x: x.DayShiJiMoney_mean_90d.apply(self.daily_turnover_avg_90d_score),
            daily_traffic_score=lambda x: x.daily_traffic_mean_90d.apply(self.daily_traffic_avg_90d_score),
            daily_member_recharge_score=lambda x: x.daily_member_recharge_mean_90d.apply(
                self.daily_member_recharge_avg_90d_score),
            rjxf_score=lambda x: x.rjxf_90d.apply(self.per_capita_consumption_90d_score),
            release_ratio_score=lambda x: x.release_ratio.apply(self.retreating_rate_90d_score),
            daily_turnover_avg_xdbd_score=lambda x: x.dayShiJiMoney_xdbd_90d.apply(
                self.daily_turnover_avg_90d_xdbd_score),
            daily_turnover_sum_growrate_score=lambda x: x.dayShiJiMoney_growth_rate_90d.apply(
                self.daily_turnover_sum_90d_growrate_score),
        )
        f_score = var_score_data[
            ['saas_use_score', 'business_days_score', 'daily_turnover_avg_score', 'daily_traffic_score',
             'daily_member_recharge_score', 'rjxf_score', 'release_ratio_score', 'daily_turnover_avg_xdbd_score',
             'daily_turnover_sum_growrate_score']].sum(axis=1)
        return int(f_score), int(self.var_data.volume_avg)

    @staticmethod
    def saas_use_time_score(x: int):
        if 180 <= x < 360:
            return 2
        elif 360 <= x < 720:
            return 5
        elif x >= 720:
            return 7
        else:
            return 0

    @staticmethod
    def business_days_180d_score(x: int):
        if 120 <= x < 150:
            return 5
        elif 150 <= x < 170:
            return 8
        elif 170 <= x <= 185:
            return 11
        else:
            return 0

    @staticmethod
    def daily_turnover_avg_90d_score(x: float):
        if 1500 <= x < 2500:
            return 4
        elif 2500 <= x < 5500:
            return 6
        elif 5500 <= x < 12000:
            return 10
        elif x >= 12000:
            return 8
        else:
            return 0

    @staticmethod
    def daily_traffic_avg_90d_score(x: float):
        if 30 <= x < 60:
            return 3
        elif 60 <= x < 120:
            return 5
        elif 120 <= x < 350:
            return 7
        elif x >= 350:
            return 9
        else:
            return 0

    @staticmethod
    def daily_member_recharge_avg_90d_score(x: float):
        if 1 <= x < 120:
            return 4
        elif 120 <= x < 1000:
            return 6
        elif x >= 1000:
            return 8
        else:
            return 0

    @staticmethod
    def per_capita_consumption_90d_score(x: float):
        if 15 <= x < 40:
            return 3
        elif 40 <= x < 80:
            return 4
        elif 80 <= x < 150:
            return 5
        elif 150 <= x < 500:
            return 8
        elif x >= 500:
            return 6
        else:
            return 0

    @staticmethod
    def retreating_rate_90d_score(x: float):
        if 0.05 <= x < 0.1:
            return 6
        elif 0.01 <= x < 0.05:
            return 10
        elif 0 < x < 0.01:
            return 12
        elif (x - 0) < 1e-5:
            return 14
        else:
            return 0

    @staticmethod
    def daily_turnover_avg_90d_xdbd_score(x: float):
        if 0.5 <= x < 0.8:
            return 5
        elif 0.8 <= x < 1:
            return 10
        elif 1 <= x < 1.3:
            return 15
        elif 1.3 <= x < 2:
            return 12
        elif x >= 2:
            return 8
        else:
            return 0

    @staticmethod
    def daily_turnover_sum_90d_growrate_score(x: float):
        if -0.5 <= x < 0.01:
            return 5
        elif 0.01 <= x < 0.2:
            return 8
        elif 0.2 <= x < 0.5:
            return 14
        elif 0.5 <= x < 1.1:
            return 18
        elif x >= 1.1:
            return 9
        else:
            return 0


if __name__ == '__main__':
    var_data_s = pd.read_csv(r'E:\Users\bjwangchao1\QIANDAODAO\Business_Scorecard\qdd_111_score.csv', encoding='utf-8')
    qdd_score = QddScorecard(var_data=var_data_s)
    print(qdd_score.dim_score_sum_score())
