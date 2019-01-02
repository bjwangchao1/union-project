import flask
from flask import request
import json
import pandas as pd
import numpy as np
from datetime import datetime
from flask import jsonify

from Business_Scorecard.business_scorecard import QddScorecard
from Business_Scorecard.compute_variable import QddVariable
from Business_Scorecard.ccx_filter_rules import QddFILTER

server = flask.Flask(__name__)


@server.route('/qdd_businessApi', methods=['POST'])
def qdd_business_api():
    try:
        # 1、获取数据
        rawdata = json.loads(request.data.decode())
        storeid = rawdata.get('StoreID')
        t0 = rawdata.get('starttime')
        saas_time = rawdata.get('saas_time')
        idno = rawdata.get('id_no')

        if not t0:
            t0 = datetime.now().strftime('%Y-%m-%d')

        f_data = pd.DataFrame(rawdata.get('business_detail')).fillna(np.nan)

        assert f_data.shape[1] > 1, 'input data is null!'

        f_data['pass_mth'] = t0
        f_data['StoreID'] = storeid
        f_data.payment_method = f_data.payment_method.map(str)
        f_data.payment_method = f_data.payment_method.map(lambda x: x.replace("'", "\""))
        f_data.drop_duplicates(inplace=True)

        f_data.JieSuanTime = pd.to_datetime(f_data.JieSuanTime)
        f_data.pass_mth = pd.to_datetime(f_data.pass_mth)
        f_data = f_data.loc[f_data.JieSuanTime < f_data.pass_mth]

        assert f_data.shape[0] >= 1, 'data is empty before application time!'

        # 2、计算变量
        qdd_var_obj = QddVariable(f_data, start_time=t0, saas_time=saas_time)
        qdd_business_var = qdd_var_obj.variable_integration()

        # 3、筛选
        qdd_filter = QddFILTER(f_data, id_no=idno, start_time=t0)
        status, reason = qdd_filter.all_filter_condition()

        # 4、计算得分
        if status:
            qdd_b_score = QddScorecard(qdd_business_var)
            result, volume_avg_ = qdd_b_score.dim_score_sum_score()
        else:
            result, volume_avg_ = -1, 0

        # 5、返回结果
        cur_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        res = {"code": 200, 'code_message': "计算成功", 'qdd_score_a': result, "volume_avg": volume_avg_,
               "reqTime": cur_date, "reqID": storeid}

        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        return jsonify({"code": 502, "msg": "计算失败", "error_msg": str(e), "reqID": storeid})


if __name__ == '__main__':
    # server.run(debug=True, port=8010, host='0.0.0.0', processes=10)
    server.run(debug=True, port=8090, host='0.0.0.0')
