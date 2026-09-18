import pandas as pd
import sqlalchemy as sa
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
#from notification_error import on_failure_callback
from airflow.models import Variable
import time
import requests
from airflow.operators.bash import BashOperator
from datetime import timedelta
from my_modules import api_1c, minio_module
from datetime import datetime


#mode = 'prod' # режим dev/prod


args = {'owner': 'airflow',
        'start_date': dt.datetime(2025, 12, 1, 0, 0), #dt.datetime.now(), dt.datetime(2024, 3, 30, 8, 0)
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=3),
    'depends_on_past': True,
    }

#импортируем мудуль для работы с monio
minio_client = minio_module.MinioBase(
        endpoint="10.1.11.65:9090",
        access_key="8Rr1cXRj7BlocpC2cSS1",
        secret_key="AEBCyC74YYDI4U86mmbhwpcwlZO1e8SCqnBcntzW",
        secure=False
    )

#импортируем модуль для работы с API 1С
client_1c = api_1c.ApiBase(url='http://10.1.11.46/1c-zup-pegas/hs/get/employees?translit&archived&deleted', auth=('api', 'GjkmpjdfntkmFGB'))

def to_stage_minio():
    # получаем данные из 1С в json формате
    data_json = client_1c.get_data_zup()
    #пишем json  в minio
    minio_client.upload_json(bucket="testbucket", object_name=f"airflow_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.json", data=data_json)
