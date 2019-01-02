import flask
from flask import request
import json
import pandas as pd
import numpy as np
from datetime import datetime
from flask import jsonify

from Credit_Limit.credit_limit_model import QddCreditLimit
from Credit_Limit.apply_scorecard import QddApplyScorecard

server = flask.Flask(__name__)


@server.route('/qdd_limitApi', methods=['POST'])
def qdd_limit_api():
    try:
        # 1、获取数据
        rawdata = json.loads(request.data.decode())
        storeid = rawdata.get('StoreID')

        apply_data = pd.DataFrame(rawdata.get('apply_detail')).fillna(np.nan)
        cre_data = pd.DataFrame(rawdata.get('credit_detail')).fillna(np.nan)

        apply_data['StoreID'] = storeid
        apply_data.drop_duplicates(inplace=True)

        cre_data['StoreID'] = storeid
        cre_data.drop_duplicates(inplace=True)

        assert apply_data.shape[0] >= 1, 'data is empty!'

        # 2、计算申请得分
        qdd_apply_obj = QddApplyScorecard(data=apply_data)
        qdd_apply_score = qdd_apply_obj.dim_apply_sum_score()

        # 3、计算额度、期数
        cre_data['qdd_score_b'] = qdd_apply_score
        qdd_limit_obj = QddCreditLimit(cre_data)
        qdd_limit, qdd_period, qdd_score = qdd_limit_obj.credit_output()

        if qdd_score < 40:
            qdd_limit, qdd_period, qdd_score = 0, 0, -1

        # 4、返回结果
        cur_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        res = {"code": 200, "code_message": "计算成功", "qdd_credit_limit": qdd_limit, "qdd_period": qdd_period,
               "qdd_score": qdd_score, "reqTime": cur_date, "reqID": storeid}

        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        return jsonify({"code": 502, "msg": "计算失败", "error_msg": str(e), "reqID": storeid})


if __name__ == '__main__':
    # server.run(debug=True, port=8010, host='0.0.0.0', processes=10)
    server.run(debug=True, port=9050, host='0.0.0.0')
