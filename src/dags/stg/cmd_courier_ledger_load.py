import datetime
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom


#api_conn = BaseHook.get_connection('api_connection')
api_endpoint = 'd5d04q7d963eapoepsqr.apigw.yandexcloud.net' #api_conn.host
api_token = "25c27781-8fde-4b30-a22e-524044a7580f" #api_conn.password
nickname = "ZHENYOK"
cohort = "0"

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort }

#psql_conn = BaseHook.get_connection('api_connection')

#conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
#cur = conn.cursor()
#cur.close()
#onn.close()

today_date = datetime.datetime.now()
#to_date = datetime.strptime(today_date,'%Y-%m-%d %H:%M:%S')
to_date = today_date.strftime("%Y-%m-%d %H:%M:%S")
fr_date = today_date + datetime.timedelta(days=-7)
from_date = fr_date.strftime("%Y-%m-%d %H:%M:%S")
sort_field = 'id'
sort_direction = 'desc'
limit = 10


params = {'limit': limit, 'offset': '0','sort_field':sort_field,'sort_direction':sort_direction}

def get_request(base_url):    
    offset = 0
    while True:    
        couriers_rep = requests.get(f'https://{base_url}/couriers/?sort_field=_id&sort_direction=asc&limit={limit}&offset={offset}',
                            headers = headers).json()
        print(couriers_rep)
        if len(couriers_rep) == 0:
            break
 
        columns = ','.join([i for i in couriers_rep[0]])
        print(columns)
        values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]
        print(values)
        offset += len(couriers_rep)  

dag = DAG(
    dag_id='project_sprint5',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

api_request = PythonOperator(task_id='file_request', 
                                python_callable=get_request, 
                                op_kwargs={'base_url': api_endpoint},
                                dag=dag)
