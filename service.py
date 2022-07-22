import pandas as pd

turnover_unit = 10000


def query_swap_info(date):
    """
    查询交换业务当日swap数据
    :param date: 当日日期
    :return: float swap数据[交易笔数, 名义本金]，若没有记录，则返回[0, 0]
    """
    data_path = 'data/{year}.csv'.format(year=date[:4])
    df = pd.read_csv(data_path)
    df['date'] = df['date'].astype('str')
    swap_info = df[(df['date'] == date) & (df['type'] == 'swap')][['trade_num', 'turnover']].fillna(0)
    swap_info['turnover'] = swap_info['turnover'] / turnover_unit
    if len(swap_info.values) == 0:
        raise Exception('no data')
    else:
        return swap_info.values[0]


def update_swap_turnover(date, turnover):
    """
    更新交换业务当日turnover数据
    :param date: 当日日期
    :param turnover: turnover数据
    :return: 更新标志 True：更新成功，False：更新失败
    """
    data_path = 'data/{year}.csv'.format(year=date[:4])
    df = pd.read_csv(data_path)
    df['date'] = df['date'].astype('str')
    if not df[(df['date'] == date) & (df['type'] == 'swap')].empty:
        # 判断该条记录是否存在
        df = df.set_index(['date', 'type'])
        df.loc[(date, 'swap'), 'turnover'] = float(turnover) * turnover_unit
        df.to_csv(data_path)
        return True
    else:
        return False


def query_opt_info(date):
    """
    查询交换业务当日opt数据
    :param date: 当日日期
    :return: float opt数据[交易笔数, 名义本金]，若没有记录，则返回[0, 0]
    """
    data_path = 'data/{year}.csv'.format(year=date[:4])
    df = pd.read_csv(data_path)
    df['date'] = df['date'].astype('str')
    opt_info = df[(df['date'] == date) & (df['type'] == 'opt')][['trade_num', 'turnover', 'variety_names']].fillna(0)
    opt_info['turnover'] = opt_info['turnover'] / turnover_unit
    if len(opt_info.values) == 0:
        raise Exception('no data')
    else:
        return opt_info.values[0]


def update_opt_turnover(date, turnover, opt_num, opt_varieties):
    """
    更新交换业务当日turnover数据
    :param date: 当日日期
    :param turnover: turnover数据
    :return: 更新标志 True：更新成功，False：更新失败
    """
    data_path = 'data/{year}.csv'.format(year=date[:4])
    df = pd.read_csv(data_path)
    df['date'] = df['date'].astype('str')
    if not df[(df['date'] == date) & (df['type'] == 'opt')].empty:
        # 判断该条记录是否存在
        df = df.set_index(['date', 'type'])
        if opt_num == '' or opt_num == '0':
            turnover = 0
            opt_num = 0
        df.loc[(date, 'opt'), 'turnover'] = float(turnover) * turnover_unit
        df.loc[(date, 'opt'), 'trade_num'] = int(opt_num)
        df.loc[(date, 'opt'), 'variety_names'] = opt_varieties.strip()
        df.to_csv(data_path)
        return True
    else:
        return False


def query_daily_rept(date):
    """
    获得当日整体查询数据
    :param date:
    :return:
    """
    data_path = 'data/{year}.csv'.format(year=date[:4])
    df = pd.read_csv(data_path)
    # 获得当日数据
    df['date'] = df['date'].astype('str')
    daily_data = df[(df['date'] == date)]
    # 获得前5日数据
    last5_date = df[(df['date'] <= date) & (df['trade_num'] > 0)]['date'].drop_duplicates().sort_values(ascending=False).head(5).values
    last5_data = df[df['date'].isin(last5_date)].groupby(['date', 'type']).sum()['turnover'].sort_index(ascending=True)
    # 获得当月数据
    month_data = df[(df['date'] <= date) & (df['date'] >= (date[:6]+'01'))].groupby('type').sum()
    # 获得全年汇总数据
    year_data = df[df['date'] <= date].groupby('type').sum()

    return {
        'daily_data': daily_data,
        'last5_data': last5_data,
        'month_data': month_data,
        'year_data': year_data
    }


if __name__ == '__main__':
    print(query_swap_info('20220223')[0])
    # print(update_swap_turnover('20220223', 201))
    # print(query_daily_rept('20220225'))