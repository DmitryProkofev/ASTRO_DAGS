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

#TODO сделать норм обновление через PK


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


def get_new_data():
    """
    Извлекает данные из MinIO, преобразует и пишет в raw Oracle
    """

    from my_modules import minio_module

    # 1. Получаем имя файла из XCom предыдущей задачи
    #TODO подставить XCOM
    filename = 'ats__20260918043924475900'
    
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


def insert_stage_db():
    from my_modules.transform_data import parse
    from my_modules.ora_think_activate import OracleDataWriter

    data = get_new_data()
    
    data_parse = parse(data)

    
    writer = OracleDataWriter(conn_id=CONN_ID)
            
    target_fields = ["NAME", "MAIL", "WORK_PHONE", "MOBILE", "TABEL"]
            
            # 3. Вызываем метод записи (валидация и Thick mode произойдут внутри автоматически!)
    writer.insert_multiple_rows(
                table="AIRFLOW.TEST_DATA_ATS",
                rows=data_parse,
                target_fields=target_fields,
                commit_every=100
            )



docstring = """Данный DAG обновляет таблицу АТС c номерами телефонов и почтой"""

query_key_airflow = """ALTER TABLE AIRFLOW.ATS_NEW ADD CONSTRAINT ats_key UNIQUE ("WORK_PHONE")"""
query_update = """DECLARE 
    v_error_msg VARCHAR2(4000);
BEGIN
    -- MERGE (обновление + вставка)
    MERGE INTO DATA_EX.EMPLOYES_ATS_NEW dst
    USING AIRFLOW.ATS_NEW src
    ON (dst."WORK_PHONE" = src."WORK_PHONE")
    WHEN MATCHED THEN
        UPDATE SET 
            dst."NAME" = src."NAME",
            dst."MAIL" = src."MAIL",
            dst."MOBILE" = src."MOBILE"
    WHEN NOT MATCHED THEN
        INSERT ("WORK_PHONE", "NAME", "MAIL", "MOBILE")
        VALUES (src."WORK_PHONE", src."NAME", src."MAIL", src."MOBILE");

    -- DELETE (удаление записей, которых нет в источнике)
    DELETE FROM DATA_EX.EMPLOYES_ATS_NEW dst
    WHERE NOT EXISTS (
        SELECT 1 FROM AIRFLOW.ATS_NEW src 
        WHERE src."WORK_PHONE" = dst."WORK_PHONE"
    );

    -- Фиксируем изменения
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_msg);
END;
"""


with DAG(
    dag_id='ATS_NEW',
    schedule_interval='0 5 * * *',
    default_args=args,
    # on_failure_callback=on_failure_callback,
    doc_md=docstring,
    catchup=False

) as dag:

    # get_data = PythonOperator(
    #     task_id='to_stage_minio_ats',
    #     python_callable=task_to_stage_minio,
    #     dag=dag
    # )

    insert_stage = PythonOperator(
        task_id='insert_stage_db',
        python_callable=insert_stage_db,
        dag=dag
    )


    # create_uniq_key = PythonOperator(
    #     task_id='create_key',
    #     python_callable=query_to_db,
    #     op_args=[query_key_airflow],
    #     dag=dag
    # )

    # update_data = PythonOperator(
    #     task_id='update_ats',
    #     python_callable=query_to_db,
    #     op_args=[query_update],
    #     dag=dag
    # )

    insert_stage


