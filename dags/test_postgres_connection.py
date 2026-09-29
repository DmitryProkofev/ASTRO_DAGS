"""
Test Postgres Connection
DAG для диагностики и проверки подключения postgres_test_conn.
Запустите этот DAG, если есть проблемы с подключением, правами или данными.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

CONN_ID = "postgres_test_conn"

# ====================== ИЗМЕНИТЕ ЭТИ ЗНАЧЕНИЯ ======================
TABLE_NAME = "universal_raw"      # ← Замените на реальное имя таблицы
SCHEMA = "public"                        # ← Измените, если схема не public
# ===================================================================


def debug_connection_params(**context):
    """Выводит все параметры Connection для диагностики"""
    conn = BaseHook.get_connection(CONN_ID)
    print("\n" + "="*80)
    print("🔍 POSTGRES CONNECTION DEBUG")
    print("="*80)
    print(f"conn_id          : {conn.conn_id}")
    print(f"host             : {conn.host}")
    print(f"port             : {conn.port}")
    print(f"login (database) : {conn.login}     ← Это имя базы данных!")
    print(f"schema           : {conn.schema}     ← Это схема (обычно public)")
    print(f"password         : {'*' * len(str(conn.password)) if conn.password else 'None'}")
    print(f"extra            : {conn.extra}")
    print("="*80 + "\n")


def test_basic_connection(**context):
    """Простая проверка подключения"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    result = hook.get_first("SELECT 1 as test")
    print(f"✅ Базовое подключение к Postgres успешно! Результат: {result}")


def test_database_info(**context):
    """Показывает текущую базу данных, схему и версию PostgreSQL"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    sql = "SELECT current_database(), current_schema(), version()"
    result = hook.get_first(sql)
    print(f"✅ Текущая база данных : {result[0]}")
    print(f"✅ Текущая схема       : {result[1]}")
    print(f"✅ Версия PostgreSQL    : {result[2][:80]}...")


def test_max_ingestion_time(**context):
    """Проверяет наличие данных и максимальную дату ingestion_time"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    full_table = f"{SCHEMA}.{TABLE_NAME}"
    
    sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(ingestion_time) as rows_with_date,
            MAX(ingestion_time) as max_ingestion_time
        FROM {full_table}
    """
    
    try:
        result = hook.get_first(sql)
        if result:
            total, with_date, max_date = result
            print(f"\n✅ Таблица: {full_table}")
            print(f"   Всего строк                    : {total:,}")
            print(f"   Строк с ingestion_time         : {with_date:,}")
            print(f"   MAX(ingestion_time)            : {max_date}")
        else:
            print(f"⚠️ Таблица {full_table} вернула пустой результат")
    except Exception as e:
        print(f"❌ Ошибка при запросе к таблице {full_table}")
        print(f"   Ошибка: {str(e)}")
        print("   Возможные причины: неверное имя таблицы, нет прав, таблица не существует.")


# ==================== DAG ====================

with DAG(
    dag_id='test_postgres_connection',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'postgres', 'debug', 'connection'],
    default_args={
        'owner': 'data_team',
        'retries': 1,
        'execution_timeout': timedelta(minutes=5)
    },
) as dag:

    debug_conn = PythonOperator(
        task_id='debug_connection_params',
        python_callable=debug_connection_params,
    )

    test_conn = PythonOperator(
        task_id='test_basic_connection',
        python_callable=test_basic_connection,
        execution_timeout=timedelta(minutes=0.2)
    )

    test_info = PythonOperator(
        task_id='test_database_info',
        python_callable=test_database_info,
    )

    test_data = PythonOperator(
        task_id='test_max_ingestion_time',
        python_callable=test_max_ingestion_time,
    )

    # Последовательность
    debug_conn >> test_conn >> test_info >> test_data


dag.doc_md = """
### Тест подключения Postgres (postgres_test_conn)

**Как использовать:**
1. Запустите этот DAG
2. Посмотрите логи задачи `debug_connection_params` — там будут все параметры вашего коннекта
3. Проверьте задачу `test_max_ingestion_time` — она покажет реальные данные из таблицы

**Перед первым запуском:**
- Измените `TABLE_NAME` и `SCHEMA` в начале файла
- Убедитесь, что Connection `postgres_test_conn` правильно настроен в airflow_settings.yaml
"""
