from pykoala.etl import DataSource

from fastapi.encoders import jsonable_encoder

import json
import configparser
import os
import datetime


config = configparser.ConfigParser()
path = os.path.join((os.path.split(os.path.realpath(__file__))[0]),"config.ini")
config.read(path)

def get_data(db_address,sql_text):

    host = config.get(db_address, "host")
    username = config.get(db_address, "username")
    password = config.get(db_address, "password")
    port = config.get(db_address, "port")
    database = config.get(db_address, "db")
    ds = DataSource(host,username,password,port,database,'mysql')

    df = ds.get_sql(sql_text)

    return df

page = 1
perPage = 10
df = get_data('201_3306',"select replace(fd_job_id,'RCRM_dashboard_','') as 报告, fd_status as 状态, DATE_FORMAT(fd_start_time,'%%m-%%d %%H:%%i') 开始时间, DATE_FORMAT(fd_end_time,'%%m-%%d %%H:%%i') 结束时间, fd_remark 备注 from qyt02.arko_sys_report_jobdetail order by fd_end_time desc limit " + page +',' + perPage)

print(df)