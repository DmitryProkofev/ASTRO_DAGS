import pandas as pd
import requests
import sqlalchemy as sa
from config import api_auth
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
from sql.order_czch.query import query



args = {'owner': 'airflow',
        'start_date': datetime.now(),
        'retries': 3,  # Количество попыток
        'retry_delay': timedelta(minutes=1)  # Задержка между попытками
        }



con_data = Variable.get("postgres_prod")
engine = sa.create_engine(con_data, pool_pre_ping=True)


def erp_api(query):
    time_end = datetime.now().isoformat(timespec='seconds')
    t = '2025-04-24T00:00:00'
    url = f'http://192.168.0.112/1c-erp/hs/api/query?ДатаНачала^=2020-01-01T00:00:00&ДатаОкончания^={time_end}&Склад@Справочник.Склады=b6a23cecef7e8af811edaceaac61d61e&text={query}'
    response = requests.get(url, auth=api_auth)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        raise Exception(f'response.status_code != 200\nERROR:{response.text}')
    

with DAG(
    dag_id="order_szch",
    schedule_interval='*/30 * * * *', 
    catchup=False,
    default_args=args
) as dag:
    
    @task
    def stage_base():
        df = erp_api(query)

        df['DATEOR'] = pd.to_datetime(df['DATEOR'], errors='coerce')
        df['DATEWISH'] = pd.to_datetime(df['DATEWISH'], errors='coerce')
        df['DATEEXORFINAL'] = pd.to_datetime(df['DATEEXORFINAL'], errors='coerce')
        df['DATEEXOR'] = pd.to_datetime(df['DATEEXOR'], errors='coerce')
        df['DATESHIP'] = pd.to_datetime(df['DATESHIP'], errors='coerce')

        dtype = {'DATEOR': sa.TIMESTAMP,
         'DATEWISH': sa.TIMESTAMP,
         'DATEEXORFINAL': sa.TIMESTAMP,
         'DATEEXOR': sa.TIMESTAMP,
         'DATESHIP': sa.TIMESTAMP}

        df.to_sql('order_szch', engine, if_exists='replace', index=False, schema='stage',
                chunksize=5000, dtype=dtype)
        
    update_table = PostgresOperator(
            task_id='update_table',
            postgres_conn_id="postgres_prod",
            sql="sql/order_czch/order_czch.sql"
        )
        


    stage_base() >> update_table