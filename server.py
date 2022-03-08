from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html

from apscheduler.schedulers.background import BackgroundScheduler
import json
import datetime
import uvicorn
import utils

from schedule_service import otc_daily_rept
import service as sv

from fastapi.middleware.cors import CORSMiddleware

origins = [
    "*"
]

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/docs")
def get_documentation():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Swagger UI"
    )


@app.get("/tradeInfo")
async def get_trade_info():
    date = datetime.datetime.now().strftime('%Y%m%d')
    swap_info = sv.query_swap_info(date)
    opt_info = sv.query_opt_info(date)
    return {
        "Status": 200,
        "swap_num": swap_info[0],
        "swap_turnover": swap_info[1],
        "opt_num": opt_info[0],
        "opt_turnover": opt_info[1],
    }


@app.get("/getOptReport")
async def get_opt_report(swap_turnover=None, opt_turnover=None):
    date = datetime.datetime.now().strftime('%Y%m%d')
    # 更新交换业务名义本金
    if swap_turnover is not None:
        sv.update_swap_turnover(date, swap_turnover)

    # 更新期权业务名义本金
    if opt_turnover is not None:
        sv.update_opt_turnover(date, opt_turnover)

    volume_unit = 10000
    turnover_unit = 100000000
    data = sv.query_daily_rept(date)
    data['daily_data']['date'] = data['daily_data']['date'].astype('str')
    daily_data = data['daily_data'].set_index(['date', 'type'])
    last5_data_type = data['last5_data'].reset_index().set_index('type')
    last5_data = data['last5_data'].reset_index()
    month_data = data['month_data']
    year_data = data['year_data']

    # 日数据计算
    day_wbill_num = int(daily_data['trade_num'][(date, 'wbill')])
    day_wbill_varietys = str(daily_data['variety_names'][(date, 'wbill')])
    day_wbill_volume = round(daily_data['volume'][(date, 'wbill')] / volume_unit, 2)
    day_wbill_turnover = round(daily_data['turnover'][(date, 'wbill')] / turnover_unit, 2)

    day_nonwbill_num = int(daily_data['trade_num'][(date, 'nonwbill')])
    day_nonwbill_varietys = daily_data['variety_names'].fillna('')[(date, 'nonwbill')]
    day_nonwbill_volume = round(daily_data['volume'][(date, 'nonwbill')] / volume_unit, 2)
    day_nonwbill_turnover = round(daily_data['turnover'][(date, 'nonwbill')] / turnover_unit, 2)

    day_basis_num = int(daily_data['trade_num'][(date, 'basis')])
    day_basis_varietys = daily_data['variety_names'].fillna('')[(date, 'basis')]
    day_basis_volume = round(daily_data['volume'][(date, 'basis')] / volume_unit, 2)
    day_basis_turnover = round(daily_data['turnover'][(date, 'basis')] / turnover_unit, 2)

    day_swap_num = int(daily_data['trade_num'][(date, 'swap')])
    day_swap_varietys = daily_data['variety_names'].fillna('')[(date, 'swap')]
    day_swap_turnover = round(daily_data['turnover'][(date, 'swap')] / turnover_unit, 2)

    day_opt_num = int(daily_data['trade_num'][(date, 'opt')])
    day_opt_varietys = daily_data['variety_names'].fillna('')[(date, 'opt')]
    day_opt_turnover = round(daily_data['turnover'][(date, 'opt')] / turnover_unit, 2)

    daily_sum_turnover = round(day_wbill_turnover + day_nonwbill_turnover + day_basis_turnover + day_swap_turnover + day_opt_turnover, 2)
    month_sum_turnover = round(month_data['turnover'].sum() / turnover_unit, 2)

    # last5数据计算
    last5_wbill = (last5_data_type['turnover']['wbill'] / turnover_unit).round(2).values.tolist()
    last5_nonwbill = (last5_data_type['turnover']['nonwbill'] / turnover_unit).round(2).values.tolist()
    last5_basis = (last5_data_type['turnover']['basis'] / turnover_unit).round(2).values.tolist()
    last5_swap = (last5_data_type['turnover']['swap'] / turnover_unit).round(2).values.tolist()
    last5_opt = (last5_data_type['turnover']['opt'] / turnover_unit).round(2).values.tolist()
    last5_sum = (last5_data.groupby('date').sum()['turnover'] / turnover_unit).sort_index(ascending=False).round(2).values.tolist()
    last5_sum[0] = daily_sum_turnover

    # 年数据计算
    year_wbill_num = int(year_data['trade_num']['wbill'])
    year_wbill_volume = (year_data['volume']['wbill'] / volume_unit).round(2)
    year_wbill_turnover = (year_data['turnover']['wbill'] / turnover_unit).round(2)

    year_nonwbill_num = int(year_data['trade_num']['nonwbill'])
    year_nonwbill_volume = (year_data['volume']['nonwbill'] / volume_unit).round(2)
    year_nonwbill_turnover = (year_data['turnover']['nonwbill'] / turnover_unit).round(2)

    year_basis_num = int(year_data['trade_num']['basis'])
    year_basis_volume = (year_data['volume']['basis'] / volume_unit).round(2)
    year_basis_turnover = (year_data['turnover']['basis'] / turnover_unit).round(2)

    year_swap_num = int(year_data['trade_num']['swap'])
    year_swap_turnover = (year_data['turnover']['swap'] / turnover_unit).round(2)

    year_opt_num = int(year_data['trade_num']['opt'])
    year_opt_turnover = (year_data['turnover']['opt'] / turnover_unit).round(2)

    year_sum_volume = (year_wbill_volume + year_nonwbill_volume + year_basis_volume).round(2)
    year_sum_turnover = (year_wbill_turnover + year_nonwbill_turnover + year_basis_turnover + year_swap_turnover + year_opt_turnover).round(2)

    daily = {
        "daily_sum": daily_sum_turnover,
        "month_sum": month_sum_turnover,
        "year_sum": year_sum_turnover,
        "wbill": {
            "num": day_wbill_num,
            "varietys": day_wbill_varietys,
            "volume": day_wbill_volume,
            "turnover": day_wbill_turnover
        },
        "non_wbill": {
            "num": day_nonwbill_num,
            "varietys": day_nonwbill_varietys,
            "volume": day_nonwbill_volume,
            "turnover": day_nonwbill_turnover
        },
        "index_basis": {
            "num": day_basis_num,
            "varietys": day_basis_varietys,
            "volume": day_basis_volume,
            "turnover": day_basis_turnover
        },
        "swap": {
            "num": day_swap_num,
            "varietys": day_swap_varietys,
            "turnover": day_swap_turnover
        },
        "opt": {
            "num": day_opt_num,
            "varietys": day_opt_varietys,
            "turnover": day_opt_turnover
        }
    }

    weekly = {
        "dates": [str(int(date[4:6])) + '月' + str(int(date[6:8])) + '日' for date in
                  last5_data['date'].drop_duplicates().values.tolist()],
        "wbill": last5_wbill,
        "non_wbill": last5_nonwbill,
        "index_basis": last5_basis,
        "swap": last5_swap,
        "opt": last5_opt,
        "sum": last5_sum
    }

    yearly = {
        "wbill": {
            "num": year_wbill_num,
            "volume": year_wbill_volume,
            "turnover": year_wbill_turnover
        },
        "non_wbill": {
            "num": year_nonwbill_num,
            "volume": year_nonwbill_volume,
            "turnover": year_nonwbill_turnover
        },
        "index_basis": {
            "num": year_basis_num,
            "volume": year_basis_volume,
            "turnover": year_basis_turnover
        },
        "swap": {
            "num": year_swap_num,
            "turnover": year_swap_turnover
        },
        "opt": {
            "num": year_opt_num,
            "turnover": year_opt_turnover
        },
        "sum": {
            "num": int(year_data['trade_num'].sum()),
            "volume": year_sum_volume,
            "turnover": year_sum_turnover
        }
    }

    return json.loads(json.dumps({
        "Status": 200,
        "daily": daily,
        "weekly": weekly,
        "yearly": yearly,
    }))


if __name__ == '__main__':
    log = utils.get_log()
    # 启动定时任务
    log.info('######### 启动自动任务 #########')
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    log.info('######### 启动OTC日报采集自动任务 #########')
    # 自动服务时间设置 周一至周五 16点至18点 每隔10分钟
    scheduler.add_job(otc_daily_rept.job, 'cron', day_of_week='0-4', hour='16-18', minute='*/10', args=['job2'])
    # scheduler.add_job(otc_daily_rept.job, 'cron', day_of_week='0-4', hour='8-22', minute='*/1')
    scheduler.start()
    log.info('######### 启动自动任务完成 #########')

    # 启动fastapi服务
    log.info('######### 启动fastapi服务 #########')
    uvicorn.run(app='server:app', host='0.0.0.0', port=8000, reload=True, workers=4)
    log.info('######### 关闭fastapi服务 #########')

