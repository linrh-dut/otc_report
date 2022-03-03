import pandas as pd


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
    swap_info['turnover'] = swap_info['turnover'] / 100000000
    if len(swap_info.values) == 0:
        return [0, 0]
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
        df.loc[(date, 'swap'), 'turnover'] = float(turnover) * 100000000
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
    last5_data = df[df['date'].isin(last5_date)].groupby(['date', 'type']).sum()['turnover'].sort_index(ascending=False)
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