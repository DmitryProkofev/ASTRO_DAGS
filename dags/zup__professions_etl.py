"""
ETL профессий из 1С ЗУП → MinIO → Oracle (STG → MART)
Использует централизованный OracleDataWriter (чистый Thin mode)
"""

import pandas as pd
from datetime import timedelta, datetime
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.oracle.hooks.oracle import OracleHook

from my_modules import minio_module
from my_modules.ora_think_activate import OracleDataWriter


# ==============================================================================
# CONFIGURATION
# ==============================================================================

DAG_ID = 'zup_professions_etl'
MINIO_BUCKET = 'zupprofessions'
MINIO_ENDPOINT = "10.1.11.65:9090"
ORACLE_CONN_ID = "oracle_con"         
STG_TABLE = "AIRFLOW.STG_ZUP_PROFESSION"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 18, 9, 30),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': True,
}

doc_md = """**ETL Профессий ЗУП (обновлённая версия 2026)**

1. Загрузка из API 1С в MinIO
2. Чтение из MinIO → трансформация → запись в STAGE через OracleDataWriter
3. Обновление витрины через PL/SQL

Используется чистый Thin-mode OracleDataWriter (без DPY-2017).
"""


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_minio_client() -> minio_module.MinioBase:
    """Создаёт и возвращает клиент MinIO."""
    access_key = Variable.get("minio_access_key")
    secret_key = Variable.get("minio_secret_key")

    return minio_module.MinioBase(
        endpoint=MINIO_ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )


def task_to_lake_professions(**context) -> str:
    """Получает данные из 1С ЗУП и сохраняет JSON в MinIO."""
    hook = HttpHook(method="GET", http_conn_id="zup_api")
    response = hook.run("1c-zup-pegas/hs/get/positions")
    data = response.json()

    filename = f"zup__professions__{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"

    minio_client = get_minio_client()
    minio_client.upload_json(
        bucket=MINIO_BUCKET,
        object_name=f"{filename}.json",
        data=data
    )

    print(f"✅ Загружено в MinIO: {MINIO_BUCKET}/{filename}.json")
    return filename


def task_to_stage_professions(ti) -> int:
    """Читает JSON из MinIO, трансформирует и записывает в STAGE."""
    filename = ti.xcom_pull(task_ids="to_lake_professions", key="return_value")
    if not filename:
        raise ValueError("Не получено имя файла из XCom")

    object_name = f"{filename}.json"

    # Получаем данные из MinIO
    minio_client = get_minio_client()
    data_json = minio_client.get_json(bucket=MINIO_BUCKET, object_name=object_name)

    # Трансформация
    df = pd.DataFrame(data_json)

    rename_map = {
        'Ссылка': 'UUID',
        'ВерсияДанных': 'VERSION',
        'ПометкаУдаления': 'DEL',
        'Наименование': 'NAME',
        'ДатаВвода': 'DATE_INPUT',
        'ДатаИсключения': 'DATE_INCLUSION',
        'ОКПДТРКод_2026': 'OKPDTR_CODE',
        'ОКПДТРКатегория_2026': 'OKPDTR_CODE_CATEGORY',
        'ОКЗКод_2026': 'OKZ_CODE',
    }
    df = df.rename(columns=rename_map)

    target_cols = [
        'UUID', 'VERSION', 'DEL', 'NAME', 'DATE_INPUT', 'DATE_INCLUSION',
        'OKPDTR_CODE', 'OKPDTR_CODE_CATEGORY', 'OKZ_CODE'
    ]
    df = df[[col for col in target_cols if col in df.columns]]

    rows = [tuple(x) for x in df.to_numpy()]

    # Используем централизованный чистый OracleDataWriter (Thin mode)
    # === Очистка STAGE перед загрузкой (как было в оригинальной версии) ===
    hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    hook.run(f"TRUNCATE TABLE {STG_TABLE}")
    print(f"🧹 Таблица {STG_TABLE} очищена (TRUNCATE)")

    # Используем централизованный чистый OracleDataWriter (Thin mode)
    writer = OracleDataWriter(conn_id=ORACLE_CONN_ID)
    inserted = writer.insert_multiple_rows(
        table=STG_TABLE,
        rows=rows,
        target_fields=target_cols,
        commit_every=500
    )

    print(f"✅ Записано {inserted} строк в {STG_TABLE}")
    return inserted


def task_refresh_mart():
    """Обновляет витрину через PL/SQL."""
    sql = f"""
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DATA_EX.ZUP__PROFESSIONS_CATALOG';
        
        INSERT INTO DATA_EX.ZUP__PROFESSIONS_CATALOG
        WITH actual AS (
            SELECT
                UUID AS UUID_ZUP,
                NAME,
                TO_NUMBER(OKPDTR_CODE)      AS OKPDTR_CODE,
                TO_NUMBER(OKPDTR_CODE_CATEGORY) AS OKPDTR_CODE_CATEGORY,
                TO_NUMBER(OKZ_CODE)         AS OKZ_CODE,
                SYSDATE AS CREATED_AT
            FROM {STG_TABLE}
            WHERE TO_NUMBER(DEL) <> 1
              AND OKPDTR_CODE IS NOT NULL
        )
        SELECT * FROM actual;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
            RAISE;
    END;
    """

    hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    hook.run(sql)
    print("✅ Витрина DATA_EX.ZUP__PROFESSIONS_CATALOG успешно обновлена")


# ==============================================================================
# DAG
# ==============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='0 */4 * * *',
    catchup=False,
    doc_md=doc_md,
    tags=['zup', 'professions', 'etl', 'minio'],
) as dag:

    to_lake = PythonOperator(
        task_id='to_lake_professions',
        python_callable=task_to_lake_professions,
        execution_timeout=timedelta(minutes=2),
    )

    to_stage = PythonOperator(
        task_id='to_stage_professions',
        python_callable=task_to_stage_professions,
        execution_timeout=timedelta(minutes=3),
    )

    refresh_mart = PythonOperator(
        task_id='refresh_mart',
        python_callable=task_refresh_mart,
        execution_timeout=timedelta(minutes=2),
    )

    to_lake >> to_stage >> refresh_mart
