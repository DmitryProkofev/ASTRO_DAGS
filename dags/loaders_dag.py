from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.operators.empty import EmptyOperator

def query_clickhouse(sql_path: str = None, sql: str = None):
    from custom_utils.clickhouse_client import ClickHouseClient
    client = ClickHouseClient(conn_id='click_connect')

    try:
        if sql_path:
            result = client.command(sql_path)
        else:
            result = client.command(sql)
        return result
    except Exception as e:
        import logging
        logging.error(f"Ошибка выполнения запроса: {e}")
        raise

def check_data_exists(sql_path: str):
    from custom_utils.clickhouse_client import ClickHouseClient
    client = ClickHouseClient(conn_id='click_connect')
    try:
        result = client.query(sql_path)
        return bool(result)
    except Exception as e:
        import logging
        logging.error(f"Ошибка выполнения запроса: {e}")
        raise

def create_tasks_for_table(table_name: str, exception_table=None):
    check = ShortCircuitOperator(
        task_id=f'check_{table_name}',
        python_callable=check_data_exists,
        op_kwargs={'sql_path': f'dags/sql/loaders/check_data/check_{table_name}.sql'},
    )

    bronze = PythonOperator(
        task_id=f'bronze_{table_name}',
        python_callable=query_clickhouse,
        op_kwargs={'sql_path': f'dags/sql/loaders/bronze/{table_name}_bronze.sql'},
    )

    bronze_drop_table = PythonOperator(
        task_id=f'bronze_{table_name}_drop',
        python_callable=query_clickhouse,
        op_kwargs={'sql': f'DROP TABLE IF EXISTS bronze_layer.{table_name}_old'},
    )

    bronze_rename_old = PythonOperator(
        task_id=f'bronze_{table_name}_rename_and_swap_1',
        python_callable=query_clickhouse,
        op_kwargs={'sql': f'RENAME TABLE bronze_layer.{table_name} TO bronze_layer.{table_name}_old'},
    )

    bronze_rename_new = PythonOperator(
        task_id=f'bronze_{table_name}_rename_and_swap_2',
        python_callable=query_clickhouse,
        op_kwargs={'sql': f'RENAME TABLE bronze_layer.{table_name}_new TO bronze_layer.{table_name}'},
    )

    silver = PythonOperator(
        task_id=f'silver_{table_name}',
        python_callable=query_clickhouse,
        op_kwargs={'sql_path': f'dags/sql/loaders/silver/{table_name}_silver.sql'},
    )

    silver_drop_table = PythonOperator(
        task_id=f'silver_{table_name}_drop',
        python_callable=query_clickhouse,
        op_kwargs={'sql': f'DROP TABLE IF EXISTS silver_layer.{table_name}_old'},
    )

    silver_rename_old = PythonOperator(
        task_id=f'silver_{table_name}_rename_and_swap_1',
        python_callable=query_clickhouse,
        op_kwargs={'sql': f'RENAME TABLE silver_layer.{table_name} TO silver_layer.{table_name}_old'},
    )

    silver_rename_new = PythonOperator(
        task_id=f'silver_{table_name}_rename_and_swap_2',
        python_callable=query_clickhouse,
        op_kwargs={'sql': f'RENAME TABLE silver_layer.{table_name}_new TO silver_layer.{table_name}'},
    )

    if table_name != exception_table:

        dimension = PythonOperator(
            task_id=f'dimenension_{table_name}',
            python_callable=query_clickhouse,
            op_kwargs={'sql_path': f'dags/sql/loaders/gold/dim_{table_name}.sql'},
        )

    else:

        dimension = EmptyOperator(task_id="next_task", dag=dag)


    # Определяем зависимости между задачами одной таблицы
    check >> bronze >> bronze_drop_table >> bronze_rename_old >> bronze_rename_new >> silver >> silver_drop_table >> silver_rename_old >> silver_rename_new >> dimension

    # Возвращаем все задачи, чтобы потом связать их между собой при необходимости
    return check, bronze, bronze_drop_table, bronze_rename_old, bronze_rename_new, silver, silver_drop_table, silver_rename_old , silver_rename_new, dimension


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

    with TaskGroup('extract_and_load') as extract_and_load:

        tables = [
            'loaders_calls',
            'loaders_call_priorities',
            'loaders_reasons',
            'loaders_workshops',
            'pa_oper',
        ]


        for table in tables:
            check, bronze, bronze_drop_table, bronze_rename_old, bronze_rename_new, silver, silver_drop_table, silver_rename_old, silver_rename_new, dimension = create_tasks_for_table(table, exception_table = 'loaders_calls')




    update_facts = PythonOperator(
        task_id=f'update_facts',
        python_callable=query_clickhouse,
        op_kwargs={'sql_path': f'dags/sql/loaders/gold/fct_loaders_calls.sql'},
    )


    next_dag = EmptyOperator(task_id="next_dag", dag=dag)


    extract_and_load >> update_facts >> next_dag