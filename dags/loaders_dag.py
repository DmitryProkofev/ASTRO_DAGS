from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.exceptions import AirflowException
import logging
from airflow.utils.task_group import TaskGroup


def query_clickhouse(sql_path: str):
    from custom_utils.clickhouse_client import ClickHouseClient
    client = ClickHouseClient(conn_id='click_connect')

    # Выполнение запроса
    try:
        result = client.command(sql_path)
        logging.info("Результат запроса: %s", result)
    except AirflowException as e:
        logging.error("Ошибка выполнения запроса: %s", e)
        raise



cdm_2_query = """BEGIN
        TRUNCATE TABLE DATA_EX.EMPLOYES_ERP;

        INSERT INTO DATA_EX.EMPLOYES_ERP
        SELECT SURNAME, NAME, PATRONYMIC, FIO, BARCODE, UID_PHYS, CURRENT_TIME
        FROM (
        SELECT
        	eb.*,
        	ROW_NUMBER() OVER (PARTITION BY UID_PHYS ORDER BY CURRENT_TIME DESC) AS ROW_num
        FROM
        	DATA_EX.EMPLOYES_BARCODES eb)
        WHERE ROW_NUM = 1;

        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
            RAISE; END; """


default_args = {
    'start_date': datetime.now(),
}

with DAG(
    dag_id='loaders',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['clickhouse'],
) as dag:

    stage_base = PythonOperator(
        task_id='stage_base',
        python_callable=query_clickhouse,
        op_kwargs={'sql_path': 'dags/sql/loaders/stage_base.sql'},  # Передача контекста в kwargs
    )

    with TaskGroup("group_1", tooltip="Группа задач 1") as group_1:
        dim_loaders_employes = PythonOperator(
            task_id='dim_loaders_employes',
            python_callable=query_clickhouse,
            op_kwargs={'sql_path': 'dags/sql/loaders/dim_loaders_employes.sql'},  # Передача контекста в kwargs
        )

        dim_loaders_workshops = PythonOperator(
            task_id='dim_loaders_workshops',
            python_callable=query_clickhouse,
            op_kwargs={'sql_path': 'dags/sql/loaders/dim_loaders_workshops.sql'},  # Передача контекста в kwargs
        )

        dim_loaders_reasons = PythonOperator(
            task_id='dim_loaders_reasons',
            python_callable=query_clickhouse,
            op_kwargs={'sql_path': 'dags/sql/loaders/dim_loaders_reasons.sql'},  # Передача контекста в kwargs
        )

        dim_priority = PythonOperator(
            task_id='dim_priority',
            python_callable=query_clickhouse,
            op_kwargs={'sql_path': 'dags/sql/loaders/dim_priority.sql'},  # Передача контекста в kwargs
        )





    stage_base >> group_1



