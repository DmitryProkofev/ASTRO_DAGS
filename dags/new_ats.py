from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models import DAG
import datetime as dt
# import sqlalchemy as sa
# import urllib.request
# import xml.etree.cElementTree as xml
# from lxml import etree
# from oracle_model import Oracle
# from my_modules import minio_module

CONN_ID = "oracle_test_conn"
# --------------------------------------------------

# -----------------------------------------------------------

args = {'owner': 'airflow',
        'start_date': dt.datetime(2025, 3, 31, 17, 0), #dt.datetime.now(), dt.datetime(2025, 3, 31, 16, 0)
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=0.3),
    'depends_on_past': True,
    }


def get_minio_client():
    """Создает и возвращает клиент MinIO."""
    from minio import Minio


    #creditionals minio
    MINIO_ACCESS_KEY = Variable.get(f"minio_access_key")
    MINIO_SECRET_KEY = Variable.get(f"minio_secret_key")

    return Minio(
        endpoint='10.1.11.65:9090',
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False 
    )

def get_xml():
    """Получаем по API xml из АТС"""
    import requests
    url = f'http://10.1.12.253/tftpboot/3e891ee8dbe6447e/contactspbook/Yealink/Company_Contacts.xml'
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception('response.status_code != 200')


def task_to_stage_minio():
    """Запись сырых данных в raw слой"""

    from airflow.providers.http.hooks.http import HttpHook
    from datetime import timedelta, datetime
    from my_modules import minio_module


    #creditionals minio
    minio_access_key = Variable.get(f"minio_access_key")
    minio_secret_key = Variable.get(f"minio_secret_key")


    bucket = 'ats'

    #импортируем модуль для работы с monio
    minio_client = minio_module.MinioBase(
            endpoint="10.1.11.65:9090",
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )

    xml_data = get_xml()

    # Генерируем имя файла
    xcom = f"ats__{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
    filename = f"{xcom}.xml"
    
    minio_client.upload_raw(bucket='ats',
                            object_name=filename,
                            data=xml_data)
    
    print(f"✅ Файл {filename} успешно загружен в бакет {bucket}")
    
    # 6. Передаем в XCom
    return xcom


def flatten_dict(x):
    """Превращает словарь в строку"""
    if isinstance(x, dict):
        # Берём значение из поля "Наименование", если есть, иначе из "Ссылка"
        return x.get("Наименование") or x.get("Ссылка", str(x))
    return x


def get_new_data(ti):
    """
    Извлекает данные из MinIO, преобразует и пишет в raw Oracle
    """

    from my_modules import minio_module

    # 1. Получаем имя файла из XCom предыдущей задачи
    filename = ti.xcom_pull(task_ids="to_stage_minio_ats", key='return_value')
    
    if not filename:
        raise ValueError("Filename not found in XCom from task 'to_lake_professions'")

    object_name = f"{filename}.xml"
    
    try:

        BUCKET_NAME = 'ats'

        #импортируем модуль для работы с monio
        client = get_minio_client()
        # 3. Читаем объект из MinIO
        # get_object возвращает поток, читаем его содержимое
        response = client.get_object(BUCKET_NAME, object_name)
        data = response.read()
        response.close()
        response.release_conn()
    
        
        if isinstance(data, dict):
            print("Dictionary is empty. No data to process.")
            return

        return data
        
    except Exception as e:
        raise Exception(f"Error processing file {object_name}: {str(e)}")


def insert_stage_db(ti):
    """Пишет в raw слой"""
    from my_modules.transform_data import parse
    from my_modules.ora_think_activate import OracleDataWriter

    data = get_new_data(ti)
    
    data_parse = parse(data)

    
    writer = OracleDataWriter(conn_id=CONN_ID)
            
    target_fields = ["NAME", "MAIL", "WORK_PHONE", "MOBILE", "UUID"]
            
    # 3. Вызываем метод записи (валидация и Thick mode произойдут внутри автоматически!)
    writer.insert_multiple_rows(
                table="AIRFLOW.ATS_NEW",
                rows=data_parse,
                target_fields=target_fields,
                commit_every=100
            )


docstring = """Данный DAG обновляет таблицу АТС c номерами телефонов и почтой"""


with DAG(
    dag_id='ATS_NEW',
    schedule_interval='0 5 * * *',
    default_args=args,
    # on_failure_callback=on_failure_callback,
    doc_md=docstring,
    catchup=False

) as dag:


    def run_sql_task(sql_filename, **kwargs):
            from my_modules.ora_think_activate import OracleDataWriter
            import os
            writer = OracleDataWriter(conn_id=CONN_ID)
            dag_folder = os.path.dirname(os.path.abspath(__file__))
            sql_dir = os.path.join(dag_folder, "sql", "ats")
            writer.execute_sql_from_file(sql_filename, sql_dir=sql_dir)


    get_data = PythonOperator(
        task_id='to_stage_minio_ats',
        python_callable=task_to_stage_minio,
        dag=dag
    )

    truncate_stage = PythonOperator(
        task_id='trunc_stage',
        python_callable=run_sql_task,
        op_kwargs={'sql_filename': 'truncate_raw.sql'},
        dag=dag
    )

    insert_stage = PythonOperator(
        task_id='insert_stage_db',
        python_callable=insert_stage_db,
        dag=dag,
        provide_context=True
    )

    check_quality = PythonOperator(
        task_id='check_quality',
        python_callable=run_sql_task,
        op_kwargs={'sql_filename': 'quality.sql'},
        dag=dag
    )

    update_data = PythonOperator(
        task_id='update_ats',
        python_callable=run_sql_task,
        op_kwargs={'sql_filename': 'merge_ats_data.sql'},
        dag=dag
    )


    get_data >> truncate_stage >> insert_stage >> check_quality >> update_data


