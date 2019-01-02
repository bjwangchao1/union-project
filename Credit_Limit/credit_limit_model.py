import pandas as pd
import numpy as np


class QddCreditLimit(object):
    """
    钱到到授信额度模型
    """

    def __init__(self, data):
        self.data = data
        self.f_score = self.mix_three_score()

    def mix_three_score(self):
        avg_score = self.data.qdd_score_a * 0.5 + self.data.qdd_score_b * 0.3 + self.data.qdd_score_c * 0.2
        return 100 if int(avg_score) > 100 else int(avg_score)

    def periods_ensure(self):
        period_list = [1, 3, 6, 12]
        if (self.f_score > 50) or (int(self.data.apply_period) == 1):
            return int(self.data.apply_period)
        else:
            return int(period_list[period_list.index(int(self.data.apply_period)) - 1])

    def credit_limit_amount(self):
        if self.f_score > 75:
            return int(self.data.volume_avg * 0.5)
        elif 50 <= self.f_score <= 75:
            return int(self.data.volume_avg * 0.3)
        elif 20 <= self.f_score < 50:
            return int(self.data.volume_avg * 0.1)
        else:
            return int(self.data.volume_avg * 0.05)

    def final_limit_amount(self):
        init_limit = self.credit_limit_amount()
        if self.data.first_class_count_12m[0] > 3 or self.data.max_tax_inclusive_price_12m[0] > 2000:
            init_limit *= 1.1
        if init_limit % 1000 != 0:
            init_limit = round(init_limit / 1000) * 1000

        return min(max(10000, init_limit), 200000)

    def credit_output(self):
        period = self.periods_ensure()
        limit = min(self.final_limit_amount(), int(self.data.apply_principal))
        return limit, period, self.f_score


if __name__ == '__main__':
    ce_data = pd.read_csv(r'C:\Users\bjwangchao1\Desktop\20181010_钱到到\数据\ce_data.csv', encoding='utf-8',
                          engine='python')
    ce_data['volume_avg'] = 152308
    ce_data['first_class_count_12m'] = 4
    qdd_credit_limit = QddCreditLimit(ce_data)
    print(qdd_credit_limit.credit_output())
