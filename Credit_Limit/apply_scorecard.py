import pandas as pd
from datetime import datetime
import numpy as np


class QddApplyScorecard(object):
    """
    Apply for Diligent Scoring Card
    """
    now_time = datetime.now().strftime('%Y-%m-%d')

    def __init__(self, data):
        self.data = data

    def dim_apply_sum_score(self):
        var_score_data = self.data.assign(
            education_sco=lambda x: x.education.apply(self.education_score),
            work_years_sco=lambda x: x.work_years.apply(self.work_years_score),
            annual_income_sco=lambda x: x.annual_income.apply(self.annual_income_score),
            marriage_sco=lambda x: x.marriage.apply(self.marriage_status_score),
            children_number_sco=lambda x: x.children_number.apply(self.children_num_score),
            is_house_property_sco=lambda x: x.is_house_property.apply(self.is_house_vehicle_property_score),
            is_vehicle_property_sco=lambda x: x.is_vehicle_property.apply(self.is_house_vehicle_property_score),
            business_month_sco=lambda x: x.business_starttime.apply(self.business_month_score),
            business_area_sco=lambda x: x.business_area.apply(self.business_area_score),
            employee_number_sco=lambda x: x.employee_number.apply(self.employee_number_score),
            annual_revenue_stream_sco=lambda x: x.annual_revenue_stream.apply(self.annual_revenue_stream_score),
        )
        f_score = var_score_data[
            ['education_sco', 'work_years_sco', 'annual_income_sco', 'marriage_sco',
             'children_number_sco', 'is_house_property_sco', 'is_vehicle_property_sco', 'business_month_sco',
             'business_area_sco', 'employee_number_sco', 'annual_revenue_stream_sco']].sum(axis=1)
        return int(f_score)

    @staticmethod
    def education_score(x: int):
        edu_dict = {1: 11, 2: 9, 3: 7, 4: 5}
        if x in edu_dict.keys():
            return edu_dict.get(x)
        else:
            return 0

    @staticmethod
    def work_years_score(x: float):
        if 0 <= x < 3:
            return 3
        elif 3 <= x < 5:
            return 5
        elif 5 <= x < 10:
            return 7
        elif x >= 10:
            return 9
        else:
            return 0

    @staticmethod
    def annual_income_score(x: float):
        if x < 10:
            return 4
        elif 10 <= x < 30:
            return 6
        elif 30 <= x < 50:
            return 8
        elif 50 <= x < 100:
            return 10
        elif x > 100:
            return 13
        else:
            return 0

    @staticmethod
    def marriage_status_score(x: int):
        marr_dict = {1: 5, 2: 7, 3: 3}
        if x in marr_dict.keys():
            return marr_dict.get(x)
        else:
            return 0

    @staticmethod
    def children_num_score(x: int):
        if x == 0:
            return 2
        elif x == 1:
            return 5
        elif x >= 2:
            return 3
        else:
            return 0

    @staticmethod
    def is_house_vehicle_property_score(x: int):
        if x == 1:
            return 5
        else:
            return 0

    @staticmethod
    def month_differ(x, y):
        """暂不考虑day, 只根据month和year计算相差月份
        Parameters
        ----------
        x, y: 两个datetime.datetime类型的变量

        Return
        ------
        differ: x, y相差的月份
        """
        month_differ = abs((x.year - y.year) * 12 + (x.month - y.month) * 1)
        return month_differ

    def business_month_score(self, x: str):
        x = pd.to_datetime(x)
        bm = self.month_differ(pd.to_datetime(self.now_time), x)
        if bm < 6:
            return 0
        elif 6 <= bm < 12:
            return 6
        elif 12 <= bm < 24:
            return 9
        elif bm >= 24:
            return 13
        else:
            return 0

    @staticmethod
    def business_area_score(x: float):
        if x < 10:
            return 3
        elif 10 <= x < 20:
            return 4
        elif 20 <= x < 50:
            return 6
        elif x >= 50:
            return 8
        else:
            return 0

    @staticmethod
    def employee_number_score(x: int):
        if 1 <= x < 3:
            return 2
        elif 3 <= x < 5:
            return 3
        elif 5 <= x < 10:
            return 4
        elif 10 <= x < 30:
            return 6
        elif x >= 30:
            return 8
        else:
            return 0

    @staticmethod
    def annual_revenue_stream_score(x: float):
        if x < 10:
            return 0
        elif 10 <= x < 50:
            return 4
        elif 50 <= x < 100:
            return 6
        elif 100 <= x < 300:
            return 8
        elif 300 <= x < 500:
            return 12
        elif x > 500:
            return 16
        else:
            return 0


if __name__ == '__main__':
    app_data = pd.read_csv(r'C:\Users\bjwangchao1\Desktop\20181010_钱到到\数据\apply_test_data_20181127.csv',
                           encoding='utf-8', engine='python')
    qdd_apply_obj = QddApplyScorecard(app_data)
    print(qdd_apply_obj.dim_apply_sum_score())
