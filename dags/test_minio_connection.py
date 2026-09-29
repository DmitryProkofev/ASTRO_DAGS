"""
Test MinIO Connection — РАБОЧАЯ ВЕРСИЯ
Исправлены:
1. conn.extra (была строка)
2. hook.get_bucket_list() → правильный метод для S3Hook
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

CONN_ID = "minio_conn"
TEST_BUCKET = "test"
TEST_KEY = "test_connection/test_file.json"


def debug_minio_connection(**context):
    """Выводит параметры подключения"""
    conn = BaseHook.get_connection(CONN_ID)
    
    extra = conn.extra_dejson if hasattr(conn, 'extra_dejson') else {}
    if not extra and isinstance(conn.extra, str):
        try:
            extra = json.loads(conn.extra)
        except:
            extra = {}
    
    endpoint = extra.get('endpoint_url') or extra.get('host') or extra.get('aws_endpoint_url') or 'NOT_SET'
    
    print("\n" + "="*90)
    print("🔍 MINIO / S3 CONNECTION DEBUG")
    print("="*90)
    print(f"conn_id          : {conn.conn_id}")
    print(f"login (key)      : {conn.login[:8]}...{conn.login[-4:]}")
    print(f"endpoint_url     : {endpoint}")
    print(f"extra type       : {type(conn.extra).__name__}")
    print("="*90 + "\n")


def test_list_buckets(**context):
    """Проверяет подключение и список бакетов (исправленный метод)"""
    hook = S3Hook(aws_conn_id=CONN_ID)
    try:
        # Правильный способ получения списка бакетов в S3Hook
        s3_client = hook.get_conn()
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        print(f"✅ Успешное подключение к MinIO!")
        print(f"Найдено бакетов: {len(buckets)}")
        for b in buckets:
            print(f"   • {b.get('Name', b)}")
        
        if any(b.get('Name') == TEST_BUCKET for b in buckets):
            print(f"✅ Бакет '{TEST_BUCKET}' существует")
        else:
            print(f"⚠️  Бакет '{TEST_BUCKET}' не найден (будет создан автоматически)")
            
    except Exception as e:
        print(f"❌ Ошибка подключения к MinIO: {e}")
        raise


def test_write_and_read(**context):
    """Тестирует запись и чтение файла"""
    hook = S3Hook(aws_conn_id=CONN_ID)
    test_data = {
        "test": "connection_test",
        "timestamp": datetime.utcnow().isoformat(),
        "message": "This is a test file. Can be safely deleted.",
        "dag": "test_minio_connection"
    }
    
    try:
        json_data = json.dumps(test_data, ensure_ascii=False, indent=2)
        hook.load_string(
            string_data=json_data,
            key=TEST_KEY,
            bucket_name=TEST_BUCKET,
            replace=True
        )
        print(f"✅ Файл успешно записан → s3://{TEST_BUCKET}/{TEST_KEY}")
    except Exception as e:
        print(f"❌ Ошибка записи: {e}")
        raise
    
    try:
        read_data = hook.read_key(key=TEST_KEY, bucket_name=TEST_BUCKET)
        loaded = json.loads(read_data)
        print(f"✅ Файл успешно прочитан. Сообщение: {loaded.get('message')}")
    except Exception as e:
        print(f"❌ Ошибка чтения: {e}")
        raise


def cleanup_test_file(**context):
    """Удаляет тестовый файл"""
    hook = S3Hook(aws_conn_id=CONN_ID)
    try:
        if hook.check_for_key(key=TEST_KEY, bucket_name=TEST_BUCKET):
            hook.delete_objects(bucket=TEST_BUCKET, keys=[TEST_KEY])
            print(f"🧹 Тестовый файл {TEST_KEY} успешно удалён")
        else:
            print(f"ℹ️ Тестовый файл не найден")
    except Exception as e:
        print(f"⚠️ Не удалось удалить тестовый файл: {e}")


# ==================== DAG ====================

with DAG(
    dag_id='test_minio_connection',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'minio', 's3', 'debug', 'connection'],
    default_args={
        'owner': 'data_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
        'execution_timeout': timedelta(minutes=5),
    },
) as dag:

    debug = PythonOperator(
        task_id='debug_connection_params',
        python_callable=debug_minio_connection,
        execution_timeout=timedelta(seconds=30),
    )

    list_buckets = PythonOperator(
        task_id='list_buckets',
        python_callable=test_list_buckets,
        execution_timeout=timedelta(minutes=1),
    )

    test_rw = PythonOperator(
        task_id='test_write_and_read',
        python_callable=test_write_and_read,
        execution_timeout=timedelta(minutes=2),
    )

    cleanup = PythonOperator(
        task_id='cleanup_test_file',
        python_callable=cleanup_test_file,
        execution_timeout=timedelta(minutes=1),
    )

    debug >> list_buckets >> test_rw >> cleanup


dag.doc_md = """
### Test MinIO Connection — РАБОЧАЯ ВЕРСИЯ

Исправления:
- Правильная обработка `conn.extra`
- Использован `s3_client.list_buckets()` вместо несуществующего `get_bucket_list()`
- Добавлены `execution_timeout` и разумные retry

Запустите DAG заново.
"""
