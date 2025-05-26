from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from datetime import datetime
from custom_utils.clickhouse_client import ClickHouseClient
from airflow.exceptions import AirflowException
import logging


def query_clickhouse():
    client = ClickHouseClient(conn_id='click_connect')
    sql = 'dags/sql/loaders/test.sql'

    # Выполнение запроса
    try:
        result = client.query(sql)
        logging.info("Результат запроса: %s", result)
    except AirflowException as e:
        logging.error("Ошибка выполнения запроса: %s", e)
        raise

    # ch = ClickHouseClient()
    # result = ch.query("SELECT count(*) FROM staging.loaders_calls")
    # count = result[0][0]
    # ch.logger.info(f"Количество записей: {count}")


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
        python_callable=query_clickhouse,
        provide_context=True,  # Передача контекста в kwargs
    )

    run_query
