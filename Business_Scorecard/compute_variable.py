import pandas as pd
import numpy as np
from functools import reduce
from Business_Scorecard.ccx_filter_rules import QddFILTER


class QddVariable(QddFILTER):
    """
    钱到到经营数据变量计算
    """

    def __init__(self, data, start_time, saas_time):
        super(QddVariable, self).__init__(data, start_time)
        self.saas_time = pd.to_datetime(saas_time)

    def business_days(self):
        """
        近180天营业天数
        :return:
        """
        saas_business_days = self.filter_data(gap2=183).groupby('StoreID')['StoreID'].count()
        return pd.DataFrame({'StoreID': self.data.StoreID[0], 'business_days_180d': saas_business_days.values})

    def saas_use_time(self):
        """
        saas使用时间
        :return:
        """
        saas_use_duration = (self.start_time - self.saas_time).days
        return pd.DataFrame({'StoreID': self.data.StoreID[0], 'saas_use_duration': saas_use_duration}, index=[0])

    def other_variable(self):
        """
        经营活跃度变量
        :return:
        """
        act_data_180d = self.operate_record(self.filter_data(gap1=90, gap2=180), name='90_180d')

        self.act_data['rjxf_90d'] = self.act_data.DayShiJiMoney_sum_90d / self.act_data.daily_traffic_sum_90d
        self.act_data['volume_avg'] = self.act_data.DayShiJiMoney_sum_90d / 3
        self.act_data[
            'release_ratio'] = (self.act_data.return_amount_sum_90d / self.act_data.DayShiJiMoney_sum_90d).values
        self.act_data[
            'dayShiJiMoney_xdbd_90d'] = (
                self.act_data.DayShiJiMoney_mean_90d / act_data_180d.DayShiJiMoney_mean_90_180d).values
        self.act_data['dayShiJiMoney_growth_rate_90d'] = (
                (
                        self.act_data.DayShiJiMoney_sum_90d - act_data_180d.DayShiJiMoney_sum_90_180d) / act_data_180d.DayShiJiMoney_sum_90_180d).values
        return self.act_data[
            ['StoreID', 'DayShiJiMoney_mean_90d', 'daily_traffic_mean_90d', 'daily_member_recharge_mean_90d',
             'rjxf_90d', 'release_ratio', 'dayShiJiMoney_xdbd_90d', 'dayShiJiMoney_growth_rate_90d', 'volume_avg']]

    def variable_integration(self):
        var_list = [
            self.business_days(),
            self.saas_use_time(),
            self.other_variable()
        ]

        var_all = reduce(self.merge_reduce, var_list)

        return var_all

    @staticmethod
    def merge_reduce(x, y):
        return pd.merge(x, y, on='StoreID', how='left')


if __name__ == '__main__':
    data_q = pd.read_csv(r'C:\Users\bjwangchao1\Desktop\20181010_钱到到\数据\随机客户营业数据\qdd_csgz_xxx.csv', engine='python',
                         encoding='utf-8')
    qdd_var = QddVariable(data_q, start_time='2018-10-01', saas_time='2016-12-04')
    print(qdd_var.variable_integration().columns)
    qdd_var.variable_integration().to_csv(r'qdd_111_score.csv', index=False, encoding='utf-8')
