import logging

import requests
import datetime
import json
import pandas as pd
import csv
import os
import utils

log = utils.get_log()
HEADERS = {
    'Content-Type': 'application/json;charset=UTF-8',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36DCE@jcb202207otc",
}

# 品种排序
variety_order = {
    '豆一': 1,
    '豆粕': 2,
    '豆油': 3,
    '棕榈油': 4,
    '玉米': 5,
    '玉米淀粉': 6,
    '淀粉': 7,
    '大连商品交易所猪饲料': 8,
    '鸡蛋': 9,
    '乙二醇': 10,
    '苯乙烯': 11,
    '聚乙烯': 12,
    '聚丙烯': 13,
    '聚氯乙烯': 14,
    '铁矿石': 15
}


def sorted_variety(l):
    """
    品种排序方法
    :param l: 排序列表
    :return:
    """
    return sorted(l, key=lambda x: variety_order[x] if x in variety_order else 999)


def job(date=None):
    """
    采集当日期权各报表基础数据（标准仓单 非标仓单 基差交易 商品互换 场外期权）
    :return: flag：True 成功，False 失败
    """
    # date = '20220225'
    if date is None:
        date = datetime.datetime.now().strftime('%Y%m%d')
    trade_dates = []
    with open('data/trade_date.csv', 'r') as f:
        reader = csv.reader(f)
        for line in reader:
            trade_dates.append(line[0])
    if date in trade_dates:
        wbill_match(date)
        non_wbill_match(date)
        index_basis(date)
        swap_match(date)
        opt_match(date)


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
        "limit": 100
    }

    data2 = {
        "wbillMatchQryData": {
            "varietyIdList": []
        },
        "page": 1,
        "limit": 1000}

    try:
        api_url = 'http://otc.dce.com.cn/portal/data/app/wbillMatchList'
        log.info('### 标准仓单信息采集服务 API:' + api_url)
        api2_url = 'http://otc.dce.com.cn/portal/data/app/wbillApplyList'
        resp = requests.post(api_url, data=json.dumps(data), headers=HEADERS, timeout=5)
        resp2 = requests.post(api2_url, data=json.dumps(data2), headers=HEADERS, timeout=5)
        result = json.loads(resp.text)
        result2 = json.loads(resp2.text)
    except Exception as e:
        log.error('### 标准仓单信息采集服务 API:' + api_url + ' 接口异常：' + e)

    rows = result['data']['wbillMatchResultData']['rows']
    rows2 = result2['data']['wbillMatchResultData']['rows']
    log.info('### 标准仓单信息采集服务 采集' + str(len(rows)) + '条结果')
    if len(rows) > 0:
        log.info('### 标准仓单信息采集服务 处理流程 start')
        # 交易品种
        variety_ids = ','.join(set([row['varietyId'] for row in rows]))
        variety_names = '、'.join(sorted_variety(set([row['varietyName'] for row in rows])))
        # 交易笔数
        trade_num = len([row for row in rows2 if row['opDate'] == date])
        # 成交量 单位：吨
        volume = sum([float(row['matchTotWeight']) for row in rows])
        # 成交额 单位：元
        turnover = sum([float(row['turnover']) for row in rows])

        save_data(date, 'wbill', variety_ids, variety_names, trade_num, volume, turnover)
        log.info('### 标准仓单信息采集服务 处理流程 end')
    else:
        save_data(date, 'wbill', '', '', 0, 0, 0)


def non_wbill_match(date):
    """
    非标准仓单信息采集服务
    非标准仓单采集交易笔数、品种列表、成交量、成交额

    :param date: 采集信息日期
    :return:
    """
    data = {
        "spotQryData": {
            "startDate": date,
            "endDate": date,
            "varietyIdList": []},
        "page": 1,
        "limit": 100
    }

    try:
        api_url = 'http://otc.dce.com.cn/portal/data/app/nonWbillMatchList'
        log.info('### 非标准仓单信息采集服务 API:' + api_url)
        resp = requests.post(api_url, data=json.dumps(data), headers=HEADERS, timeout=5)
        result = json.loads(resp.text)
    except Exception as e:
        log.error('### 非标准仓单信息采集服务 API:' + api_url + ' 接口异常：' + e)

    rows = result['data']['spotResultData']['rows']
    log.info('### 非标准仓单信息采集服务 采集' + str(len(rows)) + '条结果')
    if len(rows) > 0:
        log.info('### 非标准仓单信息采集服务 处理流程 start')
        # 交易品种
        variety_ids = ','.join(set([row['varietyId'] for row in rows]))
        variety_names = '、'.join(sorted_variety(set([row['varietyName'] for row in rows])))
        # 交易笔数
        trade_num = len(rows)
        # 成交量 单位：吨
        volume = sum([float(row['applyWeight']) for row in rows])
        # 成交额 单位：元
        turnover = sum([float(row['applyWeight']) * float(row['price']) for row in rows])

        save_data(date, 'nonwbill', variety_ids, variety_names, trade_num, volume, turnover)
        log.info('### 非标准仓单信息采集服务 处理流程 end')
    else:
        save_data(date, 'nonwbill', '', '', 0, 0, 0)


def index_basis(date):
    """
        基差交易信息采集服务
        基差交易采集交易笔数、品种列表、成交量、成交额

        :param date: 采集信息日期
        :return:
        """
    data = {
        "basisQryData": {
            "orderStatus": "c",
            "startDate": date,
            "endDate": date,
            "varietyIdList": []
        },
        "page": 1,
        "limit": 100
    }

    try:
        api_url = 'http://otc.dce.com.cn/portal/data/app/indexBasis'
        log.info('### 基差交易信息采集服务 API:' + api_url)
        resp = requests.post(api_url, data=json.dumps(data), headers=HEADERS, timeout=5)
        result = json.loads(resp.text)
    except Exception as e:
        log.error('### 基差交易信息采集服务 API:' + api_url + ' 接口异常：' + e)

    rows = result['data']['basisResultData']['rows']
    log.info('### 基差交易信息采集服务 采集' + str(len(rows)) + '条结果')
    if len(rows) > 0:
        log.info('### 基差交易信息采集服务 处理流程 start')
        # 交易品种
        variety_ids = ','.join(set([row['varietyId'] for row in rows]))
        variety_names = '、'.join(sorted_variety(set([row['varietyName'] for row in rows])))
        # 交易笔数
        trade_num = len(rows)
        # 成交量 单位：吨
        volume = sum([float(row['qty']) for row in rows])
        # 成交额 单位：元
        turnover = round(sum([float(row['nominalMatchAmt']) for row in rows]) * 10000, 0)

        save_data(date, 'basis', variety_ids, variety_names, trade_num, volume, turnover)
        log.info('### 基差交易信息采集服务 处理流程 end')
    else:
        save_data(date, 'basis', '', '', 0, 0, 0)


def swap_match(date):
    """
        商品互换信息采集服务
        商品互换采集交易笔数、品种列表、成交量、成交额

        :param date: 采集信息日期
        :return:
        """
    data = {
        "swapQryData": {
            "startDate": date,
            "endDate": date,
            "varietyIdList": [],
            "contractType": ""
        },
        "page": 1,
        "limit": 100
    }

    try:
        api_url = 'http://otc.dce.com.cn/portal/data/app/swapMatch'
        log.info('### 商品互换信息采集服务 API:' + api_url)
        resp = requests.post(api_url, data=json.dumps(data), headers=HEADERS, timeout=5)
        result = json.loads(resp.text)
    except Exception as e:
        log.error('### 商品互换信息采集服务 API:' + api_url + ' 接口异常：' + e)

    rows = result['data']['swapResultData']['rows']
    log.info('### 商品互换信息采集服务 采集' + str(len(rows)) + '条结果')
    if len(rows) > 0:
        log.info('### 商品互换信息采集服务 处理流程 start')
        # 交易品种
        variety_ids = None
        variety_name_set = set()
        for row in rows:
            # 1 单商品互换 2 指数交换 3 价差互换
            contract_type = row['contractType']
            if contract_type == '1':
                variety_name_set.add(row['subjectContractId'][0:-4])
            elif contract_type == '2':
                variety_name_set.add(row['subjectContractId'][7:].split('期货')[0])
            elif contract_type == '3':
                variety_name_set.add(row['subjectContractId'].split('-')[0][2:-4])

        variety_names = '、'.join(sorted_variety(variety_name_set))
        # 交易笔数
        trade_num = len(rows)
        # 成交量 单位：吨
        volume = None
        # 成交额 单位：万元
        turnover = None

        save_data(date, 'swap', variety_ids, variety_names, trade_num, volume, turnover)
        log.info('### 商品互换信息采集服务 处理流程 end')
    else:
        save_data(date, 'swap', '', '', 0, 0, 0)


def opt_match(date):
    """
            场外期权信息采集服务
            场外期权采集交易笔数、品种列表、成交量、成交额

            :param date: 采集信息日期
            :return:
            """
    data = {
        "optQryData": {
            "startDate": date,
            "endDate": date,
            "varietyIdList": [],
            "contractType": ""
        },
        "page": 1,
        "limit": 100}

    try:
        api_url = 'http://otc.dce.com.cn/portal/data/app/optMatch'
        log.info('### 场外期权信息采集服务 API:' + api_url)
        resp = requests.post(api_url, data=json.dumps(data), headers=HEADERS, timeout=5)
        result = json.loads(resp.text)
    except Exception as e:
        log.error('### 场外期权信息采集服务 API:' + api_url + ' 接口异常：' + e)

    rows = result['data']['optResultData']['rows']
    log.info('### 场外期权信息采集服务 采集' + str(len(rows)) + '条结果')
    if len(rows) > 0:
        log.info('### 场外期权信息采集服务 处理流程 start')
        # 交易品种
        variety_ids = None
        variety_names = '、'.join(sorted_variety(set([row['subjectContractId'][0:-4] for row in rows])))
        # 交易笔数
        trade_num = len(rows)
        # 成交量 单位：吨
        volume = None
        # 成交额 单位：万元
        turnover = None

        # save_data(date, 'opt', variety_ids, variety_names, trade_num, volume, turnover)
        # 根据业务部门要求，暂时不记录场外期权数据
        save_data(date, 'opt', '', '', 0, 0, 0)
        log.info('### 场外期权信息采集服务 处理流程 end')
    else:
        save_data(date, 'opt', '', '', 0, 0, 0)


def save_data(date, ctype, variety_ids, variety_names, trade_num, volume, turnover):
    """
    采集数据保存

    :param date: 日期 yyyymmdd
    :param ctype: 类型[wbill ...]
    :param variety_ids: 品种ID列表
    :param variety_names: 品种名称列表
    :param trade_num: 成交笔数
    :param volume: 成交量
    :param turnover: 成交金额
    :return:
    """
    data_path = 'data/{year}.csv'.format(year=date[:4])
    if os.path.exists(data_path):
        df = pd.read_csv(data_path)
        # 修改date数据类型
        df['date'] = df['date'].astype('str')

        if len(df[(df['date'] == date) & (df['type'] == ctype)]) == 0:
            log.info('### 新增 start')
            df = df.append({
                'date': date,
                'type': ctype,
                'variety_ids': variety_ids,
                'variety_names': variety_names,
                'trade_num': trade_num,
                'volume': volume,
                'turnover': turnover,
            }, ignore_index=True)
            df.to_csv(data_path, index=False)
            log.info('### 新增 end')
        else:
            # 对比文件进行更新操作
            log.info('### 更新 start')
            df = df.set_index(['date', 'type'])
            df.loc[(date, ctype), 'variety_ids'] = variety_ids
            df.loc[(date, ctype), 'variety_names'] = variety_names
            df.loc[(date, ctype), 'trade_num'] = trade_num
            df.loc[(date, ctype), 'volume'] = volume
            if ctype != 'swap':
                df.loc[(date, ctype), 'turnover'] = turnover
            log.info('### 更新 end')
            df.to_csv(data_path)

    else:
        log.info('### 新建 start')
        df = pd.DataFrame([{
            'date': date,
            'type': ctype,
            'variety_ids': variety_ids,
            'variety_names': variety_names,
            'trade_num': trade_num,
            'volume': volume,
            'turnover': turnover,
        }])
        df.set_index(['date', 'type'])
        df.to_csv(data_path, index=False)
        log.info('### 新建 end')