from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator




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

def skip_dag():
    return False

def create_tasks_for_table(table_name: str, exception_table=None, task_group_id: str = ""):
    
    def check_branch_func(**context):
        from custom_utils.clickhouse_client import ClickHouseClient
        client = ClickHouseClient(conn_id='click_connect')
        result = client.query(f'dags/sql/loaders/check_data/check_{table_name}.sql')
        if result:
            return f"{task_group_id}.bronze_{table_name}"
        else:
            return f"{task_group_id}.skip_path_{table_name}"

    branch = BranchPythonOperator(
        task_id=f"branch_{table_name}",
        python_callable=check_branch_func,
        provide_context=True
    )

    
    if table_name != exception_table:
        skip_path = EmptyOperator(task_id=f"skip_path_{table_name}")  
    else:
        skip_path = ShortCircuitOperator(
        task_id=f'skip_path_{table_name}',
        python_callable=skip_dag,
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
            task_id=f'dimension_{table_name}',
            python_callable=query_clickhouse,
            op_kwargs={'sql_path': f'dags/sql/loaders/gold/dim_{table_name}.sql'},
        )

        dimension_actual = PythonOperator(
        task_id=f'dim_{table_name}_actual',
        python_callable=query_clickhouse,
        op_kwargs={'sql_path': f'dags/sql/loaders/dimension_actual/dim_{table_name}_actual.sql'},
    )


        dimension_actual_drop_table = PythonOperator(
            task_id=f'dim_{table_name}_actual_drop',
            python_callable=query_clickhouse,
            op_kwargs={'sql': f'DROP TABLE IF EXISTS gold_layer.dim_{table_name}_actual_old'},
        )

        dimension_actual_rename_old = PythonOperator(
            task_id=f'dim_{table_name}_rename_and_swap_1',
            python_callable=query_clickhouse,
            op_kwargs={'sql': f'RENAME TABLE gold_layer.dim_{table_name}_actual TO gold_layer.dim_{table_name}_actual_old'},
        )


        dimension_actual_rename_new = PythonOperator(
            task_id=f'dim_{table_name}_rename_and_swap_2',
            python_callable=query_clickhouse,
            op_kwargs={'sql': f'RENAME TABLE gold_layer.dim_{table_name}_actual_new TO gold_layer.dim_{table_name}_actual'},
        )

    else:

        dimension = EmptyOperator(task_id=f"skip_dimension_{table_name}")
        dimension_actual = EmptyOperator(task_id=f"skip_dimension_actual_{table_name}")
        dimension_actual_drop_table = EmptyOperator(task_id=f"skip_dimension_actual_drop_table_{table_name}")
        dimension_actual_rename_old = EmptyOperator(task_id=f"skip_dimension_actual_rename_old_{table_name}")
        dimension_actual_rename_new = EmptyOperator(task_id=f"skip_dimension_actual_rename_new_{table_name}")



    end_task = EmptyOperator(
        task_id=f"end_task_{table_name}",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    

    # Ветвление
    (
    branch >> bronze >> bronze_drop_table >> bronze_rename_old >> bronze_rename_new >> 
    silver >> silver_drop_table >> silver_rename_old >> silver_rename_new >>  dimension >> dimension_actual >>
    dimension_actual_drop_table >> dimension_actual_rename_old >>dimension_actual_rename_new
    >> end_task
    )

    (branch >> skip_path >> end_task)


    return end_task


default_args = {
    'start_date': datetime.now(), #datetime(2025, 6, 27, 9, 0)
}

with DAG(
    dag_id='loaders',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
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


        final_tasks = []

        for table in tables:
            final = create_tasks_for_table(table, exception_table='loaders_calls', task_group_id='extract_and_load')
            final_tasks.append(final)



    update_facts = PythonOperator(
        task_id=f'update_facts',
        python_callable=query_clickhouse,
        op_kwargs={'sql_path': f'dags/sql/loaders/gold/fct_loaders_calls.sql'},
        
    )


    next_dag = EmptyOperator(task_id="next_dag", dag=dag)


  
    extract_and_load >> update_facts >> next_dag