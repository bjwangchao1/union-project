import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
import sys
import json


class QddFILTER(object):
    """
    钱到到初筛规则
    """
    now_time = datetime.now().strftime('%Y-%m-%d')

    def __init__(self, data, id_no, start_time=now_time):
        self.data = data
        self.id_no = id_no
        self.start_time = pd.to_datetime(start_time)
        self.act_data = self.operate_record(red=self.filter_data())

    def all_filter_condition(self):
        if not self.business_days()[0]:
            return self.business_days()
        elif not self.business_owner_age()[0]:
            return self.business_owner_age()
        elif not self.activity_aspect()[0]:
            return self.activity_aspect()
        elif not self.stability_aspect()[0]:
            return self.stability_aspect()
        elif not self.payment_method_risk()[0]:
            return self.payment_method_risk()
        else:
            return True, 'pass'

    def business_days(self):
        """
        近180天营业天数
        :return:
        """
        saas_business_days = self.filter_data(gap2=183)['StoreID'].count()
        if saas_business_days < 120:
            return False, sys._getframe().f_code.co_name
        else:
            return True, 'pass'

    @staticmethod
    def get_idno_age(x):
        now = datetime.now().year
        try:
            return now - int(str(x).strip()[6:10])
        except Exception as e:
            return np.nan

    def business_owner_age(self):
        """
        企业主年龄
        :return:
        """
        owner_age = self.get_idno_age(self.id_no)
        if owner_age < 22 or owner_age > 55:
            return False, sys._getframe().f_code.co_name
        else:
            return True, 'pass'

    def activity_aspect(self):
        """
        活跃度
        :return:
        """
        self.act_data['rjxf_90d'] = self.act_data.DayShiJiMoney_sum_90d / self.act_data.daily_traffic_sum_90d
        if self.act_data.DayShiJiMoney_mean_90d.values < 1500 or self.act_data.DayShiJiMoney_mean_90d.values > 34000:
            return False, sys._getframe().f_code.co_name
        elif self.act_data.daily_traffic_mean_90d.values < 30 or self.act_data.daily_traffic_mean_90d.values > 800:
            return False, sys._getframe().f_code.co_name
        elif self.act_data.daily_member_recharge_mean_90d.values < 0 or self.act_data.daily_member_recharge_mean_90d.values > 5000:
            return False, sys._getframe().f_code.co_name
        elif self.act_data.rjxf_90d.values < 15 or self.act_data.rjxf_90d.values > 600:
            return False, sys._getframe().f_code.co_name
        else:
            return True, 'pass'

    def filter_data(self, gap1=0, gap2=90):
        self.data.JieSuanTime = pd.to_datetime(self.data.JieSuanTime)
        df = self.data.assign(
            pase_now=self.start_time,
        ).assign(diff_time=lambda x: (x.pase_now - x.JieSuanTime).apply(lambda s: s.days))
        times = str(gap1) + '<diff_time<=' + str(gap2)
        red = df.query(times)
        return red

    @staticmethod
    def operate_record(red, name='90d'):
        month_all_count = red.groupby('StoreID', as_index=False)[
            ['DayShiJiMoney', 'daily_traffic', 'return_amount', 'daily_member_recharge']].agg(
            [np.mean, np.sum])
        month_all_count.columns = ["_".join(x) for x in month_all_count.columns.ravel()]
        name_df = dict(zip(month_all_count.columns, [i + '_' + name for i in month_all_count.columns]))
        month_all_count.rename(columns=name_df, inplace=True)
        return month_all_count.reset_index()

    def stability_aspect(self):
        """
        稳定性
        :return:
        """
        act_data_180d = self.operate_record(self.filter_data(gap1=90, gap2=180), name='90_180d')
        self.act_data[
            'release_ratio'] = self.act_data.return_amount_sum_90d.values / self.act_data.DayShiJiMoney_sum_90d.values
        dayShiJiMoney_xdbd_90d = self.act_data.DayShiJiMoney_mean_90d.values / act_data_180d.DayShiJiMoney_mean_90_180d.values
        dayShiJiMoney_growth_rate_90d = (
                                                self.act_data.DayShiJiMoney_sum_90d.values - act_data_180d.DayShiJiMoney_sum_90_180d.values) / act_data_180d.DayShiJiMoney_sum_90_180d.values
        if pd.notnull(self.act_data.release_ratio.values) and pd.notnull(dayShiJiMoney_xdbd_90d) and pd.notnull(
                dayShiJiMoney_growth_rate_90d):
            if self.act_data.release_ratio.values < 0 or self.act_data.release_ratio.values > 0.1:
                return False, sys._getframe().f_code.co_name
            elif dayShiJiMoney_xdbd_90d < 0.5 or dayShiJiMoney_xdbd_90d > 5:
                return False, sys._getframe().f_code.co_name
            elif dayShiJiMoney_growth_rate_90d < -0.5 or dayShiJiMoney_growth_rate_90d > 10:
                return False, sys._getframe().f_code.co_name
            else:
                return True, 'pass'
        else:
            return False, sys._getframe().f_code.co_name

    def payment_method_risk(self):
        """
        风险项
        :return:
        """

        def del_with_pay(x):
            return len(json.loads(x))

        pay_count_90d = self.filter_data()['payment_method'].map(del_with_pay).mean()
        pay_count_90_180d = self.filter_data(gap1=90, gap2=180)['payment_method'].map(del_with_pay).mean()
        pay_count_xdbd_90d = pay_count_90d / pay_count_90_180d
        abnormal_count_df = self.abnormal_payment_amount()
        payment_method_bd_30d = self.payment_method_bd()
        if pay_count_90d > 15:
            return False, sys._getframe().f_code.co_name
        elif abnormal_count_df.same_value_count.values >= 5 or abnormal_count_df.int_count.values >= 5:
            return False, sys._getframe().f_code.co_name
        elif pay_count_xdbd_90d < 0.75 or pay_count_xdbd_90d > 2:
            return False, sys._getframe().f_code.co_name
        elif payment_method_bd_30d.values == 1:
            return False, sys._getframe().f_code.co_name
        else:
            return True, 'pass'

    @staticmethod
    def del_with_pay(x):
        pay_data = pd.DataFrame(json.loads(x))
        if pay_data.empty:
            return pd.DataFrame()
        else:
            pay_data['DayPayMethodMoney'] = pay_data['DayPayMethodMoney'].astype(np.float64)
            return pay_data

    def pay_method_fun(self, x):
        try:
            x = list(x)
            add = pd.DataFrame()
            for i in x:
                a = self.del_with_pay(i)
                add = add.append(a)
            add_count = add.groupby('CWKMName', as_index=False)[['DayPayMethodMoney']].sum()
            return str(add_count.sort_values('DayPayMethodMoney', ascending=False)['CWKMName'].values[:3].tolist())
        except Exception as e:
            print(e)

    @staticmethod
    def comp_two_list(x, y):
        try:
            x, y = eval(x), eval(y)
            if len(set(x) & set(y)) >= 1:
                return 0
            else:
                return 1
        except Exception as e:
            return 1

    def payment_method_bd(self):
        pay_month_30d = self.filter_data(gap2=30).groupby('StoreID', as_index=False)['payment_method'].agg(
            {'pay_method_30d': self.pay_method_fun})
        pay_month_30_60d = self.filter_data(gap1=30, gap2=60).groupby('StoreID', as_index=False)['payment_method'].agg(
            {'pay_method_30_60d': self.pay_method_fun})
        pay_month_bd = pd.merge(pay_month_30d, pay_month_30_60d, on='StoreID', how='left')
        result = pay_month_bd.apply(lambda x: self.comp_two_list(x.pay_method_30d, x.pay_method_30_60d), axis=1)
        return result

    @staticmethod
    def same_value_count(x):
        s = list(x)
        a = list(filter(lambda m: m > 100, s))
        if a:
            return max(Counter(a).values())
        else:
            return 0

    @staticmethod
    def int_count(x):
        s = list(x)
        count = 0
        for i in s:
            if i % 100 == 0:
                count += 1
            else:
                continue
        return count

    def abnormal_payment_amount(self):
        data_filter_30d = self.filter_data(gap1=0, gap2=30)
        abnormal_count = data_filter_30d.groupby('StoreID', as_index=False)['DayShiJiMoney'].agg(
            {'same_value_count': self.same_value_count, 'int_count': self.int_count})
        return abnormal_count


if __name__ == '__main__':
    xxx = pd.read_csv(r'C:\Users\bjwangchao1\Desktop\20181010_钱到到\数据\随机客户营业数据\qdd_csgz_xxx.csv', engine='python',
                      encoding='utf-8')
    qdd_filter = QddFILTER(xxx, '110222199703181843', '2018-10-01')
    print(qdd_filter.act_data.columns)
    print(qdd_filter.all_filter_condition())
