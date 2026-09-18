import json
import pandas as pd
from minio import Minio
from dags.my_modules.transform_data import parse
from dags.my_modules.ora_think_activate import OracleDataWriter


# Предполагаем, что эти значения берутся из конфигурации или переменных окружения
MINIO_ENDPOINT = "10.1.11.65:9090"
MINIO_ACCESS_KEY = '8Rr1cXRj7BlocpC2cSS1' 
MINIO_SECRET_KEY = 'AEBCyC74YYDI4U86mmbhwpcwlZO1e8SCqnBcntzW'
BUCKET_NAME = "ats"  


def get_minio_client():
    """Создает и возвращает клиент MinIO."""
    return Minio(
        endpoint='10.1.11.65:9090',
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False 
    )


def get_new_data():
    """
    Извлекает данные из MinIO, преобразует и пишет в raw Oracle
    """
    # 1. Получаем имя файла из XCom предыдущей задачи
    filename = 'ats__20260918043924475900'
    
    if not filename:
        raise ValueError("Filename not found in XCom from task 'to_lake_professions'")

    object_name = f"{filename}.xml"
    
    try:
        # 2. Инициализируем клиент
        client = get_minio_client()
        
        # 3. Читаем объект из MinIO
        # get_object возвращает поток, читаем его содержимое
        response = client.get_object(BUCKET_NAME, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        # 4. Парсим XML
        data = parse(data)

        
        if isinstance(data, dict):
            print("Dictionary is empty. No data to process.")
            return

        return data
        
    except Exception as e:
        raise Exception(f"Error processing file {object_name}: {str(e)}")


def into_db_stage():
    """Пишем df в raw слой"""
    
    data = get_new_data()

    data_parse = parse(data)

    writer = OracleDataWriter(conn_id=CONN_ID)
        
    target_fields = ["ID", "FIELD_NAME", "DESCRIPTION"]
        
        # 3. Вызываем метод записи (валидация и Thick mode произойдут внутри автоматически!)
    writer.insert_multiple_rows(
            table="AIRFLOW.TEST_DATA",
            rows=data_parse,
            target_fields=target_fields,
            commit_every=100
        )


    


into_db_stage()