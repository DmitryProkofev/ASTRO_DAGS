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
from sql.order_cost.query import query




args = {'owner': 'airflow',
        'start_date': datetime.now()
        }



con_data = Variable.get("postgres_connection_pandas")
engine = sa.create_engine(con_data, pool_pre_ping=True)



def erp_api(query):
    url = f'http://192.168.0.112/1c-erp/hs/api/query?text={query}'
    response = requests.get(url, auth=api_auth)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        raise Exception(f'response.status_code != 200\nERROR:{response.text}')




with DAG(
    dag_id="order_cost",
    schedule_interval='*/30 * * * *', 
    catchup=False,
    default_args=args
) as dag:
    
    @task
    def stage_base():
        df = erp_api(query)

        dtype = {'DATEWISH': sa.TIMESTAMP,
         'DATEFACT': sa.TIMESTAMP,
         'DATESHIP': sa.TIMESTAMP,
         'DATEEXOR': sa.TIMESTAMP,
         'DATEOR': sa.TIMESTAMP}

        df.to_sql('order_cost', engine, if_exists='replace', index=False, schema='airflow_data',
                chunksize=5000, dtype=dtype)


    alter_key_table = PostgresOperator(
            task_id='alter_key_table',
            postgres_conn_id="postgres_user_con",
            sql="sql/order_cost/alter_key.sql"
        )
    
    update_data = PostgresOperator(
            task_id='update_data',
            postgres_conn_id="postgres_user_con",
            sql="sql/order_cost/update_table.sql"
        )

    
    
    stage_base() >> alter_key_table >> update_data