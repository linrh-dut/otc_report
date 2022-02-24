import logging
import datetime


def get_log():
    """
    获取日志输出对象
    :return: 日志输出对象
    """
    today = datetime.date.today()
    today_date = today.strftime('%Y%m%d')
    file = open('logs/log' + '_' + today_date + '.log', encoding="utf-8", mode="a")
    logging.basicConfig(
        stream=file,
        level=logging.INFO,
        format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
        datefmt='%Y-%m-%d %A %H:%M:%S'
    )
    return logging.getLogger()
