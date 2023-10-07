from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from pykoala.etl import DataSource

import json
import configparser
import os
import datetime


config = configparser.ConfigParser()
path = os.path.join((os.path.split(os.path.realpath(__file__))[0]),"config.ini")
config.read(path)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*",
                    "http://localhost",
                    "http://localhost:80",
                    "http://10.1.0.223:80",
                    # 本地客户端的源
                    "http://127.0.0.1:5500/db_monitor.html",
                    "http://127.0.0.1:5500/",
                    "http://127.0.0.1:5500/QYT02库POS表最新数据时间.html",
                    "https://aisuda.github.io/amis-editor-demo/#/downloads"
                   ],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials= True,
    expose_headers=["*"]
)


def get_time(db_address, sql_text):

    host = config.get(db_address, "host")
    username = config.get(db_address, "username")
    password = config.get(db_address, "password")
    port = config.get(db_address, "port")
    database = config.get(db_address, "db")
    ds = DataSource(host,username,password,port,database,'mysql')

    df = ds.get_sql(sql_text)
        
    return df.iloc[0,0]

def get_data(db_address,sql_text):

    host = config.get(db_address, "host")
    username = config.get(db_address, "username")
    password = config.get(db_address, "password")
    port = config.get(db_address, "port")
    database = config.get(db_address, "db")
    ds = DataSource(host,username,password,port,database,'mysql')

    df = ds.get_sql(sql_text)

    return df

@app.get("/line1")
async def line1():
    df = get_data('201_3306',"select DATE_FORMAT(fd_order_date_time,'%%m/%%d %%H') time, count(1) quantity from qyt02.arko_pos where fd_order_date_skid = (select max(fd_order_date_skid) from qyt02.arko_pos)GROUP BY time order by time")
    my_json_time = df['time'].to_json(orient='records')
    my_json_quantity = df['quantity'].to_json(orient='records')

    my_json = '{"status": 0,"msg": "ok","data": {"title": {"text": "QYT02库今天pos记录","link": "../QYT02库今天pos记录.html"},"tooltip": {},"xAxis": {"data": ' + my_json_time + '},"yAxis": {},"series": [{"name": "记录数","type": "line","data": ' + my_json_quantity + '}]}}'
    my_json = json.loads(my_json)

    return my_json

@app.get("/line2")
async def line2():
    df = get_data('206_3306',"select DATE_FORMAT(fd_order_date_time,'%%m/%%d %%H') time, count(1) quantity from med.arko_pos where fd_order_date_skid = (select max(fd_order_date_skid) from med.arko_pos)GROUP BY time order by time")
    my_json_time = df['time'].to_json(orient='records')
    my_json_quantity = df['quantity'].to_json(orient='records')

    my_json = '{"status": 0,"msg": "ok","data": {"title": {"text": "med库今天pos记录","link": "../med库今天pos记录.html"},"tooltip": {},"xAxis": {"data": ' + my_json_time + '},"yAxis": {},"series": [{"name": "记录数","type": "line","data": ' + my_json_quantity + '}]}}'
    my_json = json.loads(my_json)

    return my_json

@app.get("/crud1")
async def crud1(page : int, perPage : int):
    page = str((page-1)*perPage)
    perPage = str(perPage)

    try:
        df = get_data('201_3306',"select replace(fd_job_id,'RCRM_dashboard_','') as 报告, fd_status as 状态, DATE_FORMAT(fd_start_time,'%%m-%%d %%H:%%i') 开始时间, DATE_FORMAT(fd_end_time,'%%m-%%d %%H:%%i') 结束时间, fd_remark 备注 from qyt02.arko_sys_report_jobdetail order by fd_end_time desc limit " + page +',' + perPage)
        my_json = df.to_json(orient='records')
        total = len(get_data('201_3306',"select replace(fd_job_id,'RCRM_dashboard_','') as 报告, fd_status as 状态, DATE_FORMAT(fd_start_time,'%%m-%%d %%H:%%i') 开始时间, DATE_FORMAT(fd_end_time,'%%m-%%d %%H:%%i') 结束时间, fd_remark 备注 from qyt02.arko_sys_report_jobdetail order by fd_end_time desc"))

    except Exception as e:
        my_json = '[{"id":1,"报告": "无","状态": "访问失败","开始时间": "'+ datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') + '","结束时间": "","备注": "请检查数据库连接"}]'
        total = 1
    
    my_json = '{"status": 0,"msg": "","data": {"items": %s ,"total": %s }}' % (my_json, total)
    my_json = json.loads(my_json)
    return my_json

@app.get("/crud2")
async def crud2(page : int, perPage : int):
    page = str((page-1)*perPage)
    perPage = str(perPage)

    try:
        df = get_data('206_3306',"select replace(fd_job_id,'RCRM_dashboard_','') as 报告, fd_status as 状态, DATE_FORMAT(fd_start_time,'%%m-%%d %%H:%%i') 开始时间, DATE_FORMAT(fd_end_time,'%%m-%%d %%H:%%i') 结束时间, fd_remark 备注 from med02.arko_sys_report_jobdetail order by fd_end_time desc limit " + page +',' + perPage)
        my_json = df.to_json(orient='records')
        total = len(get_data('206_3306',"select replace(fd_job_id,'RCRM_dashboard_','') as 报告, fd_status as 状态, DATE_FORMAT(fd_start_time,'%%m-%%d %%H:%%i') 开始时间, DATE_FORMAT(fd_end_time,'%%m-%%d %%H:%%i') 结束时间, fd_remark 备注 from med02.arko_sys_report_jobdetail order by fd_end_time desc"))

    except Exception as e:
        my_json = '[{"id":1,"报告": "无","状态": "访问失败","开始时间": "'+ datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') + '","结束时间": "","备注": "请检查数据库连接"}]'
        total = 1
    
    my_json = '{"status": 0,"msg": "","data": {"items": %s ,"total": %d }}' % (my_json, total)
    my_json = json.loads(my_json)
    return my_json

@app.get("/crud3")
async def crud3(page : int, perPage : int):
    page = str((page-1)*perPage)
    perPage = str(perPage)
    
    try:
        df = get_data('201_3306',"SELECT jl.fd_report_name as 报告,DATE_FORMAT(jd.fd_create_time,'%%m-%%d %%H:%%i') as 创建时间,jl.fd_report_type as 报告类型,jd.fd_status as 状态, jd.fd_remark as 备注 FROM qytcube.arko_sys_report_jobdetail jd, qytcube.arko_sys_report_joblist jl WHERE jd.fd_report_id = jl.fd_report_id AND TO_DAYS(jd.fd_create_time) = TO_DAYS(NOW()) ORDER BY jd.fd_create_time DESC limit " + page +',' + perPage)
        my_json = df.to_json(orient='records')
        total = len(get_data('201_3306',"SELECT jl.fd_report_name as 报告,DATE_FORMAT(jd.fd_create_time,'%%m-%%d %%H:%%i') as 创建时间,jl.fd_report_type as 报告类型,jd.fd_status as 状态, jd.fd_remark as 备注 FROM qytcube.arko_sys_report_jobdetail jd, qytcube.arko_sys_report_joblist jl WHERE jd.fd_report_id = jl.fd_report_id AND TO_DAYS(jd.fd_create_time) = TO_DAYS(NOW()) ORDER BY jd.fd_create_time DESC"))
    
    except Exception as e:
        my_json = '[{"id":1,"报告": "无","状态": "访问失败","开始时间": "'+ datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') + '","结束时间": "","备注": "请检查数据库连接"}]'
        total = 1
    
    my_json = '{"status": 0,"msg": "","data": {"items": %s ,"total": %d }}' % (my_json, total)
    my_json = json.loads(my_json)

    return my_json

@app.get("/crud4")
async def crud4(page : int, perPage : int):
    page = str((page-1)*perPage)
    perPage = str(perPage)
    
    try:
        df = get_data('201_3306',"select replace(fd_job_id,'RCRM_dashboard_','') as 报告, fd_status as 状态, DATE_FORMAT(fd_start_time,'%%m-%%d %%H:%%i') 开始时间, DATE_FORMAT(fd_end_time,'%%m-%%d %%H:%%i') 结束时间, fd_remark 备注 from qyt_test.arko_sys_report_jobdetail order by fd_end_time desc limit " + page +',' + perPage)
        my_json = df.to_json(orient='records')
        total = len(get_data('201_3306',"select replace(fd_job_id,'RCRM_dashboard_','') as 报告, fd_status as 状态, DATE_FORMAT(fd_start_time,'%%m-%%d %%H:%%i') 开始时间, DATE_FORMAT(fd_end_time,'%%m-%%d %%H:%%i') 结束时间, fd_remark 备注 from qyt_test.arko_sys_report_jobdetail order by fd_end_time desc"))

    except Exception as e:
        my_json = '[{"报告": "无","状态": "访问失败","开始时间": "'+ datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') + '","结束时间": "","备注": "请检查数据库连接"}]'
        total = 1
    
    my_json = '{"status": 0,"msg": "","data": {"items": %s ,"total": %d }}' % (my_json, total)
    my_json = json.loads(my_json)

    return my_json

@app.get("/crud5")
async def crud5(page : int, perPage : int):
    page = str((page-1)*perPage)
    perPage = str(perPage)
    
    try:
        df = get_data('201_3306',"SELECT jl.fd_report_name as 报告, jd.fd_create_time as 创建时间, jl.fd_report_type as 报告类型, jd.fd_status as 状态, jd.fd_remark as 备注 FROM qytcube.arko_sys_report_jobdetail jd, qytcube.arko_sys_report_joblist jl WHERE jd.fd_report_id = jl.fd_report_id AND TO_DAYS(jd.fd_create_time) = TO_DAYS(NOW()) ORDER BY jd.fd_create_time DESC limit " + page +',' + perPage)
        my_json = df.to_json(orient='records')
        total = len(get_data('201_3306',"SELECT jl.fd_report_name as 报告, jd.fd_create_time as 创建时间, jl.fd_report_type as 报告类型, jd.fd_status as 状态, jd.fd_remark as 备注 FROM qytcube.arko_sys_report_jobdetail jd, qytcube.arko_sys_report_joblist jl WHERE jd.fd_report_id = jl.fd_report_id AND TO_DAYS(jd.fd_create_time) = TO_DAYS(NOW()) ORDER BY jd.fd_create_time DESC"))

    except Exception as e:
        my_json = '[{"报告": "无","状态": "访问失败","开始时间": "'+ datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') + '","结束时间": "","备注": "请检查数据库连接"}]'
        total = 1
    
    my_json = '{"status": 0,"msg": "","data": {"items": %s ,"total": %d }}' % (my_json, total)
    my_json = json.loads(my_json)

    return my_json

@app.get("/crud6")
async def crud6(page : int, perPage : int):
    page = str((page-1)*perPage)
    perPage = str(perPage)
    
    try:
        df = get_data('208_3306',"SELECT pybee_job_list.`name` as 任务, pybee_job_detail.start_time as 开始时间, pybee_job_detail.end_time as 结束时间, pybee_job_detail.cost_time 时长, pybee_job_detail.`status` as 状态, pybee_job_detail.error_message as 错误信息 FROM matrix.pybee_job_detail INNER JOIN matrix.pybee_job_list ON pybee_job_detail.job_id = pybee_job_list.id WHERE TO_DAYS(pybee_job_detail.start_time) = TO_DAYS(NOW()) ORDER BY pybee_job_detail.start_time DESC limit " + page +',' + perPage)
        my_json = df.to_json(orient='records')
        total = len(get_data('208_3306',"SELECT pybee_job_list.`name` as 任务, pybee_job_detail.start_time as 开始时间, pybee_job_detail.end_time as 结束时间, pybee_job_detail.cost_time 时长, pybee_job_detail.`status` as 状态, pybee_job_detail.error_message as 错误信息 FROM matrix.pybee_job_detail INNER JOIN matrix.pybee_job_list ON pybee_job_detail.job_id = pybee_job_list.id WHERE TO_DAYS(pybee_job_detail.start_time) = TO_DAYS(NOW()) ORDER BY pybee_job_detail.start_time DESC"))

    except Exception as e:
        my_json = '[{"报告": "无","状态": "访问失败","开始时间": "'+ datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') + '","结束时间": "","备注": "请检查数据库连接"}]'
        total = 1
    
    my_json = '{"status": 0,"msg": "","data": {"items": %s ,"total": %d }}' % (my_json, total)
    my_json = json.loads(my_json)

    return my_json

@app.get("/time")
async def time(): 
    qyt_02_updated_time = get_time('201_3306',"select max(fd_order_date_time) from arko_pos where fd_order_date_skid = (select max(fd_order_date_skid) from arko_pos)")
    med_updated_time = get_time('206_3306',"select max(fd_order_date_time) from med.arko_pos where fd_order_date_skid = (select max(fd_order_date_skid) from med.arko_pos)")
    return {
    "status": 0,
    "msg": "ok",
    "data": {
        "time1": qyt_02_updated_time,
        "time2": med_updated_time
        }
    }

if __name__ == "__main__":
    uvicorn.run(app="main:app",
                host="127.0.0.1",
                port=8000,
                workers=1,
                reload=True
                )
