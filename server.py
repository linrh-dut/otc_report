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


@app.get("/swapTradeInfo")
async def get_swap_trade_info():
    date = datetime.datetime.now().strftime('%Y%m%d')
    swap_info = sv.query_swap_info(date)
    return {
        "Status": 200,
        "num": swap_info[0],
        "turnover": swap_info[1]
    }


@app.get("/getOptReport")
async def get_opt_report(swap_turnover=None):
    date = datetime.datetime.now().strftime('%Y%m%d')
    # 更新交换业务名义本金
    if swap_turnover is not None:
        sv.update_swap_turnover(date, swap_turnover)

    volume_unit = 10000
    turnover_unit = 100000000
    data = sv.query_daily_rept(date)
    data['daily_data']['date'] = data['daily_data']['date'].astype('str')
    daily_data = data['daily_data'].set_index(['date', 'type'])
    last5_data_type = data['last5_data'].reset_index().set_index('type')
    last5_data = data['last5_data'].reset_index()
    month_data = data['month_data']
    year_data = data['year_data']

    daily = {
        "daily_sum": round(daily_data['turnover'].sum() / turnover_unit, 2),
        "month_sum": round(month_data['turnover'].sum() / turnover_unit, 2),
        "year_sum": round(year_data['turnover'].sum() / turnover_unit, 2),
        "wbill": {
            "num": int(daily_data['trade_num'][(date, 'wbill')]),
            "varietys": str(daily_data['variety_names'][(date, 'wbill')]),
            "volume": round(daily_data['volume'][(date, 'wbill')] / volume_unit, 2),
            "turnover": round(daily_data['turnover'][(date, 'wbill')] / turnover_unit, 2)
        },
        "non_wbill": {
            "num": int(daily_data['trade_num'][(date, 'nonwbill')]),
            "varietys": daily_data['variety_names'].fillna('')[(date, 'nonwbill')],
            "volume": round(daily_data['volume'][(date, 'nonwbill')] / volume_unit, 2),
            "turnover": round(daily_data['turnover'][(date, 'nonwbill')] / turnover_unit, 2)
        },
        "index_basis": {
            "num": int(daily_data['trade_num'][(date, 'basis')]),
            "varietys": daily_data['variety_names'].fillna('')[(date, 'basis')],
            "volume": round(daily_data['volume'][(date, 'basis')] / volume_unit, 2),
            "turnover": round(daily_data['turnover'][(date, 'basis')] / turnover_unit, 2)
        },
        "swap": {
            "num": int(daily_data['trade_num'][(date, 'swap')]),
            "varietys": daily_data['variety_names'].fillna('')[(date, 'swap')],
            "turnover": round(daily_data['turnover'][(date, 'swap')] / turnover_unit, 2)
        },
        "opt": {
            "num": int(daily_data['trade_num'][(date, 'opt')]),
            "varietys": daily_data['variety_names'].fillna('')[(date, 'opt')],
            "turnover": round(daily_data['turnover'][(date, 'opt')] / turnover_unit, 2)
        }
    }

    weekly = {
        "dates": [str(int(date[4:6])) + '月' + str(int(date[6:8])) + '日' for date in
                  last5_data['date'].drop_duplicates().values.tolist()],
        "wbill": (last5_data_type['turnover']['wbill'] / turnover_unit).round(2).values.tolist(),
        "non_wbill": (last5_data_type['turnover']['nonwbill'] / turnover_unit).round(2).values.tolist(),
        "index_basis": (last5_data_type['turnover']['basis'] / turnover_unit).round(2).values.tolist(),
        "swap": (last5_data_type['turnover']['swap'] / turnover_unit).round(2).values.tolist(),
        "opt": (last5_data_type['turnover']['opt'] / turnover_unit).round(2).values.tolist(),
        "sum": (last5_data.groupby('date').sum()['turnover'] / turnover_unit).sort_index(ascending=False).round(2).values.tolist()
    }

    yearly = {
        "wbill": {
            "num": int(year_data['trade_num']['wbill']),
            "volume": (year_data['volume']['wbill'] / volume_unit).round(2),
            "turnover": (year_data['turnover']['wbill'] / turnover_unit).round(2)
        },
        "non_wbill": {
            "num": int(year_data['trade_num']['nonwbill']),
            "volume": (year_data['volume']['nonwbill'] / volume_unit).round(2),
            "turnover": (year_data['turnover']['nonwbill'] / turnover_unit).round(2)
        },
        "index_basis": {
            "num": int(year_data['trade_num']['basis']),
            "volume": (year_data['volume']['basis'] / volume_unit).round(2),
            "turnover": (year_data['turnover']['basis'] / turnover_unit).round(2)
        },
        "swap": {
            "num": int(year_data['trade_num']['swap']),
            "turnover": (year_data['turnover']['swap'] / turnover_unit).round(2)
        },
        "opt": {
            "num": int(year_data['trade_num']['opt']),
            "turnover": (year_data['turnover']['opt'] / turnover_unit).round(2)
        },
        "sum": {
            "num": int(year_data['trade_num'].sum()),
            "volume": (year_data['volume'].sum() / volume_unit).round(2),
            "turnover": (year_data['turnover'].sum() / turnover_unit).round(2)
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
    # scheduler.add_job(otc_daily_rept.job, 'cron', day_of_week='0-4', hour='16-18', minute='*/10', args=['job2'])
    scheduler.add_job(otc_daily_rept.job, 'cron', day_of_week='0-4', hour='8-22', minute='*/1')
    scheduler.start()
    log.info('######### 启动自动任务完成 #########')

    # 启动fastapi服务
    log.info('######### 启动fastapi服务 #########')
    uvicorn.run(app='server:app', host='0.0.0.0', port=8000, reload=True, workers=4)
    log.info('######### 关闭fastapi服务 #########')

