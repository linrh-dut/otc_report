import logging

import requests
import datetime
import json
import pandas as pd
import os
import utils

log = utils.get_log()
HEADERS = {
    'Content-Type': 'application/json;charset=UTF-8'
}


def job():
    """
    采集当日期权各报表基础数据（标准仓单 非标仓单 基差交易 商品互换 场外期权）
    :return: flag：True 成功，False 失败
    """
    date = datetime.datetime.now().strftime('%Y%m%d')
    print(wbill_match(date))


def wbill_match(date):
    """
    标准仓单信息采集服务
    标准仓单采集交易笔数、品种列表、成交量、成交额

    :param date: 采集信息日期
    :return:
    """
    data = {
        "wbillMatchQryData": {
            "startDate": date,
            "endDate": date,
            "varietyIdList": []},
        "page": 1,
        "limit": 10
    }

    resp = requests.post('http://otc.dce.com.cn/portal/data/app/wbillMatchList', data=json.dumps(data), headers=HEADERS)
    result = json.loads(resp.text)
    rows = result['data']['wbillMatchResultData']['rows']
    # 交易品种
    trade_variety_ids = ','.join([row['varietyId'] for row in rows])
    trade_variety_names = '、'.join([row['varietyName'] for row in rows])
    # 交易笔数
    trade_num = sum([len(row['children']) for row in rows])
    # 成交量 单位：吨
    volume = sum([row['matchTotWeight'] for row in rows])
    # 成交额 单位：元
    turnover = sum([row['turnover'] for row in rows])

    data_path = 'data/{year}.csv'.format(year=date[:4])
    if os.path.exists(data_path):
        df = pd.read_csv(data_path)
        # 修改date数据类型
        df['date'] = df['date'].astype('str')

        if len(df[(df['date'] == date) & (df['type'] == 'wbill')]) == 0:
            log.info('### wbill 新增 start')
            df.append([[date, 'wbill', trade_variety_ids, trade_variety_names, trade_num, volume, turnover]])
            print('bcz', df.head())
            log.info('### wbill 新增 end')
        else:
            # 对比文件进行更新操作
            log.info('### wbill 更新 start')
            df[(df['date'] == date) & (df['type'] == 'wbill')]['variety_ids'] = trade_variety_ids
            df[(df['date'] == date) & (df['type'] == 'wbill')]['variety_names'] = trade_variety_names
            df[(df['date'] == date) & (df['type'] == 'wbill')]['trade_num'] = trade_num
            df[(df['date'] == date) & (df['type'] == 'wbill')]['volume'] = volume
            df[(df['date'] == date) & (df['type'] == 'wbill')]['turnover'] = 1

            df = df.set_index(['date', 'type'])
            df.loc[(date, 'wbill'), 'variety_ids'] = trade_variety_ids
            df.loc[(date, 'wbill'), 'variety_names'] = trade_variety_names
            df.loc[(date, 'wbill'), 'trade_num'] = trade_num
            df.loc[(date, 'wbill'), 'volume'] = volume
            df.loc[(date, 'wbill'), 'turnover'] = turnover
            log.info('### wbill 更新 end')
        df.to_csv(data_path)

    else:
        df = pd.DataFrame([[date, 'wbill', trade_variety_ids, trade_variety_names, trade_num, volume, turnover]],
                     columns=['date', 'type', 'variety_ids', 'variety_names', 'trade_num', 'volume', 'turnover'])
        df.set_index(['date', 'type'])
        df.to_csv(data_path, index=False)

