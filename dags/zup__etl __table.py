"""
ETL по формированию табеля из 1С:ЗУП
"""
from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Импорт утилит из отдельного файла
from utils.zup_etl_functions import *

# Конфигурация DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 18, 9, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=0.5),
    'depends_on_past': False,
    'catchup': False
}

docstring = '''ETL по формированию табеля из 1С:ЗУП'''

with DAG(
    dag_id='zup__etl__table',
    schedule_interval='0 5 * * *',
    default_args=default_args,
    catchup=False,
    doc_md=docstring
) as dag:
    
    # TaskGroup: Получение данных из API
    with TaskGroup("get_data_api") as get_data_api:
        
        to_stage_current_month = PythonOperator(
            task_id='to_stage_current_month',
            python_callable=fetch_and_save_zup_data,
            op_kwargs={'period': 'current'},
            execution_timeout=timedelta(minutes=2),
        )
        
        to_stage_previous_month = PythonOperator(
            task_id='to_stage_previous_month',
            python_callable=fetch_and_save_zup_data,
            op_kwargs={'period': 'previous'},
            execution_timeout=timedelta(minutes=2),
        )
        
        [to_stage_current_month, to_stage_previous_month]
    

    # Ветвление: проверка необходимости создания партиции
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=get_partition_date,
    )

    detach_previous_month_task = PythonOperator(
            task_id='detach_previous_month_task',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'detach_partition'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )
    

    drop_table_task = PythonOperator(
            task_id='drop_table_task',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'drop_table',
                       'schema': 'stg_norm'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

    
    with TaskGroup("transform_data") as transform_data:

        transform_new_month = PythonOperator(
            task_id='transform_new_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'current',
                       'file_name': 'create_table'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

        transform_old_month = PythonOperator(
            task_id='transform_old_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'create_table'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )


    with TaskGroup("drop_tables") as drop_tables:

        drop_current_table = PythonOperator(
            task_id='drop_current_table',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'current',
                       'file_name': 'drop_table',
                       'schema': 'stg_norm'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

        drop_previous_table = PythonOperator(
            task_id='drop_previous_table',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'drop_table',
                       'schema': 'stg_norm'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )


    with TaskGroup("join_two_partition") as join_two_partition:

        join_new_month = PythonOperator(
            task_id='join_new_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'current',
                       'file_name': 'join_partitition'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

        join_old_month = PythonOperator(
            task_id='join_old_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'join_partitition'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

    with TaskGroup("attache_two_partition") as attache_two_partition:

        attach_new_month = PythonOperator(
            task_id='attach_new_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'current',
                       'file_name': 'join_partitition'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

        attach_old_month = PythonOperator(
            task_id='attach_old_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'join_partitition'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )


    with TaskGroup("detach_partition") as detach_partition:

        detach_current_month = PythonOperator(
            task_id='detach_current_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'current',
                       'file_name': 'detach_partition'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

        detach_previous_month = PythonOperator(
            task_id='detach_previous_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'detach_partition'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

    with TaskGroup("rename_tables") as rename_tables:

        rename_current_month = PythonOperator(
            task_id='rename_current_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'current',
                       'file_name': 'rename_tables'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

        rename_previous_month = PythonOperator(
            task_id='rename_previous_month',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'rename_tables'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )


    with TaskGroup("rename_tables_pattern") as rename_tables_pattern:

        rename_current_month_pattern = PythonOperator(
            task_id='rename_current_month_pattern',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'current',
                       'file_name': 'rename_table'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )

        rename_previous_month_pattern = PythonOperator(
            task_id='rename_previous_month_pattern',
            python_callable=execute_dynamic_query,
            op_kwargs={'partition': 'previous',
                       'file_name': 'rename_tables'},
            retries=1,
            retry_delay=timedelta(minutes=0.1),
        )


    get_data_api >> transform_data >> branch_task 
    
    branch_task  >> detach_partition >> rename_tables >> join_two_partition >> drop_tables
    branch_task >> detach_previous_month_task >> rename_tables_pattern >> attache_two_partition >> drop_table_task
