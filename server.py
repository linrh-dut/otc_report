from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html

from apscheduler.schedulers.background import BackgroundScheduler
import time
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
    if swap_turnover is not None:
        sv.update_swap_turnover(date, swap_turnover)
    print(swap_turnover)
    # swap_info = sv.query_swap_info(date)
    return {
        "Status": 200,
        "daily": {
            "daily_sum": 2.35,
            "month_sum": 21.27,
            "year_sum": 38.76,
            "wbill": {
                "num": 34,
                "varietys": "玉米、豆油、豆粕、聚丙烯、聚氯乙烯",
                "volume": 1.32,
                "turnover": 0.85
            },
            "non_wbill": {
                "num": 34,
                "varietys": "玉米、豆油、豆粕、聚丙烯、聚氯乙烯",
                "volume": 1.32,
                "turnover": 0.85
            },
            "index_basis": {
                "num": 34,
                "varietys": "玉米、豆油、豆粕、聚丙烯、聚氯乙烯",
                "volume": 1.32,
                "turnover": 0.85
            },
            "swap": {
                "num": 34,
                "varietys": "玉米、豆油、豆粕、聚丙烯、聚氯乙烯",
                "turnover": 0.85
            },
            "opt": {
                "num": 34,
                "varietys": "玉米、豆油、豆粕、聚丙烯、聚氯乙烯",
                "turnover": 0.85
            }
        },
        "weekly": {
            "dates": ["2月25日", "2月24日", "2月23日", "2月22日", "2月21日"],
            "wbill": [16, 34, 50, 10, 20],
            "non_wbill": [46, 24, 38, 30, 40],
            "index_basis": [26, 24, 18, 50, 50],
            "swap": [26, 24, 18, 50, 50],
            "opt": [26, 24, 18, 50, 50],
            "sum": [140, 130, 142, 190, 210]
        },
        "yearly": {
            "wbill": {
                "num": 222,
                "volume": 6.81,
                "turnover": 5.14
            },
            "non_wbill": {
                "num": 6,
                "volume": 7.5,
                "turnover": 0.65
            },
            "index_basis": {
                "num": 225,
                "volume": 56.52,
                "turnover": 23.33
            },
            "swap": {
                "num": 88,
                "turnover": 9.63
            },
            "opt": {
                "num": "-",
                "turnover": "-"
            },
            "sum": {
                "num": 541,
                "volume": 70.83,
                "turnover": 38.57
            }
        }
    }


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

