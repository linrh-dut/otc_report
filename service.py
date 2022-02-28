import pandas as pd


def query_swap_turnover(date):
    """
    查询交换业务当日turnover数据
    :param date: 当日日期
    :return: float turnover数据，若没有设置，则返回None
    """
    data_path = 'data/{year}.csv'.format(year=date[:4])
    df = pd.read_csv(data_path)
    df['date'] = df['date'].astype('str')
    turnover = df[(df['date'] == date) & (df['type'] == 'swap')]['turnover']
    if len(turnover.isnull().values) > 0 and not turnover.isnull().values[0]:
        return float(turnover.values[0])
    else:
        return None


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
        df.loc[(date, 'swap'), 'turnover'] = turnover
        df.to_csv(data_path)
        return True
    else:
        return False


def query_daily_rept(date):
    pass


if __name__ == '__main__':
    # print(query_swap_turnover('20220223'))
    print(update_swap_turnover('20220223', 201))