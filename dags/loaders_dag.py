from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from airflow.hooks.base import BaseHook
from datetime import datetime
import clickhouse_connect

def query_clickhouse():
    # Получаем подключение из Airflow
    conn = BaseHook.get_connection('click_connect')
    
    # Подключение к ClickHouse
    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login,
        password=conn.password,
        database=conn.schema or 'default'
    )

    # Выполняем запрос
    result = client.query('SELECT count(*) FROM staging.loaders_calls')
    count = result.result_rows[0][0]
    print(f"Количество записей: {count}")

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='clickhouse_connect_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['clickhouse'],
) as dag:

    run_query = PythonOperator(
        task_id='query_clickhouse',
        python_callable=query_clickhouse
    )

    run_query
