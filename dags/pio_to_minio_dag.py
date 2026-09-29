"""
PIO to MinIO DAG — финальная версия
1. Получаем MAX(ingestion_time) из Postgres (первая задача)
2. Передаём дату через XCom
3. Выгружаем данные из PIO API
4. Сохраняем в MinIO и обновляем watermark
"""

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging

from my_modules.pio_client import PioClient
from my_modules.postgres_watermark import PostgresWatermarkManager

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=0.5),
    'email_on_failure': True,
}

with DAG(
    dag_id='pio_to_minio',
    default_args=default_args,
    description='Выгрузка документов из PIO API → MinIO с watermark из Postgres',
    schedule_interval='0 6 * * *',        # каждый день в 06:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['pio', 'minio', 'etl', 'watermark'],
    max_active_runs=1,
) as dag:

    @task(task_id="get_last_ingestion_time")
    def get_last_ingestion_time():
        """
        Первая задача DAG.
        Получает максимальную дату загрузки (ingestion_time) из Postgres таблицы.
        Результат передаётся через XCom в следующую задачу.
        """
        manager = PostgresWatermarkManager(postgres_conn_id="postgres_default")
        
        last_date = manager.get_last_ingestion_date(
            table_name="raw_pio_documents",      # ← ИЗМЕНИТЕ НА ВАШУ ТАБЛИЦУ
            schema="public",
            variable_name="pio_last_ingestion_date",
            default_days_ago=7,
            column_name="ingestion_time"
        )
        
        logger.info(f"Получена последняя дата ingestion_time из Postgres: {last_date}")
        return last_date


    @task(task_id="extract_from_pio")
    def extract_from_pio(last_ingestion_date: str):
        """Выгружает документы из API Пио начиная с переданной даты"""
        client = PioClient()
        
        logger.info(f"🚀 Запуск выгрузки из PIO API с даты: {last_ingestion_date}")
        
        documents = client.fetch_documents(
            from_date=last_ingestion_date,
            page_size=100
        )
        
        logger.info(f"✅ Получено {len(documents)} документов из PIO API")
        return documents


    @task(task_id="load_to_minio_and_update_watermark")
    def load_to_minio_and_update_watermark(documents: list, previous_watermark: str):
        """Сохраняет данные в MinIO и обновляет watermark"""
        if not documents:
            logger.info("Нет новых документов для сохранения")
            return {"status": "no_data", "records": 0}
        
        client = PioClient()
        key = client.save_to_minio(documents, bucket="pio-raw")
        
        # Обновляем watermark
        manager = PostgresWatermarkManager()
        new_watermark = datetime.utcnow().isoformat() + "Z"
        manager.update_watermark("pio_last_ingestion_date", new_watermark)
        
        logger.info(f"✅ Успешно сохранено {len(documents)} документов в MinIO. Файл: {key}")
        
        return {
            "status": "success",
            "records": len(documents),
            "minio_key": key,
            "previous_watermark": previous_watermark,
            "new_watermark": new_watermark
        }


    # === Порядок выполнения (явный) ===
    last_ingestion_date = get_last_ingestion_time()
    documents = extract_from_pio(last_ingestion_date)
    load_to_minio = load_to_minio_and_update_watermark(documents, last_ingestion_date)

    # Зависимости
    last_ingestion_date >> documents >> load_to_minio

    # Документация
    dag.doc_md = """
    ### PIO → MinIO Pipeline (с watermark из Postgres)

    **Порядок выполнения:**
    1. `get_last_ingestion_time` — получает `MAX(ingestion_time)` из таблицы Postgres
    2. `extract_from_pio` — выгружает данные из PIO API начиная с этой даты (через XCom)
    3. `load_to_minio_and_update_watermark` — сохраняет в MinIO и обновляет watermark

    **Важно:** Измените `table_name="raw_pio_documents"` на реальное имя вашей таблицы.
    """
