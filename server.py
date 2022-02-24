from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html

from apscheduler.schedulers.background import BackgroundScheduler
import time
import uvicorn
import utils

from schedule_service import otc_daily_rept

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/docs")
def get_documentation():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Swagger UI"
    )


def job(text):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print('{} --- {}'.format(text, t))


if __name__ == '__main__':
    log = utils.get_log()
    # 启动定时任务
    log.info('######### 启动自动任务 #########')
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    log.info('######### 启动OTC日报采集自动任务 #########')
    # 自动服务时间设置 周一至周五 16点至18点 每隔10分钟
    # scheduler.add_job(job, 'cron', day_of_week='0-4', hour='16-18', minute='*/10', args=['job2'])
    scheduler.add_job(otc_daily_rept.job, 'cron', day_of_week='0-4', hour='8-22', minute='*/1')
    scheduler.start()
    log.info('######### 启动自动任务完成 #########')

    # 启动fastapi服务
    log.info('######### 启动fastapi服务 #########')
    uvicorn.run(app='server:app', host='0.0.0.0', port=8000, reload=True, workers=4)
    log.info('######### 关闭fastapi服务 #########')

