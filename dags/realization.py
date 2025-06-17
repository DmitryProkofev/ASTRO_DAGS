import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import task
from datetime import datetime, timedelta
# from notification_error import on_failure_callback
import sqlalchemy
from config import api_auth
from airflow.models import Variable
from sql.realization.query import query
import sqlalchemy as sa

args = {'owner': 'airflow',
        'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False,
    }


con_data = Variable.get("postgres_prod")
engine = sa.create_engine(con_data, pool_pre_ping=True)



def erp_api(query):
    url = f'http://192.168.0.112/1c-erp/hs/api/query?text={query}'
    response = requests.get(url, auth=api_auth)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        raise Exception(f'response.status_code != 200\nERROR:{response.text}')




with DAG(
    dag_id='BI_up_realization',
    schedule_interval='20 */12 * * *',
    default_args=args,
    catchup=False,
    tags=['postgres']
    # on_failure_callback=on_failure_callback,

) as dag:

    @task
    def stage_base():

        df = erp_api(query)

        dtype = {'DATEWISH': sa.TIMESTAMP,
         'DATEFACT': sa.TIMESTAMP,
         'DATESHIP': sa.TIMESTAMP,
         'DATEEXOR': sa.TIMESTAMP,
         'DATEOR': sa.TIMESTAMP}
        

        df.to_sql('realization', engine, if_exists='replace', index=False, schema='stage',
                chunksize=5000, dtype=dtype)
        

    calc_task = PostgresOperator(
            task_id='calc_task',
            postgres_conn_id="postgres_prod",
            sql="sql/realization/query_sql.sql",
            retries=3,
            retry_delay=timedelta(minutes=1)
        )

    
    
    stage_base() >> calc_task