import os
import datetime as dt
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from sqlalchemy import create_engine, TIMESTAMP
from config import api_auth
from sql.analogues.query import query as ANALOGUE_QUERY
import pandas as pd
from airflow.operators.oracle_operator import OracleOperator
import oracledb
from notification_error import on_failure_callback


# ----------------------------
# ENVIRONMENT CONFIGURATION
# ----------------------------
ORACLE_CONNECTION_URI = Variable.get("oracle_connection_pandas")
engine = create_engine(ORACLE_CONNECTION_URI)

path_xcOracle = Variable.get("path_cxOracle")
oracledb.init_oracle_client(lib_dir=path_xcOracle)

ERP_API_URL = "http://192.168.0.112/1c-erp/hs/api/query"

# ----------------------------
# FUNC
# ----------------------------
def get_data(query: str):
    import requests
    
    response = requests.get(f"{ERP_API_URL}?text={query}", auth=api_auth)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        raise Exception(f"ERP API error {response.status_code}: {response.text}")

# ----------------------------
# DAG DEFINITION
# ----------------------------
with DAG(
    dag_id='analogues',
    start_date=dt.datetime(2025, 7, 22, 9, 8),
    schedule_interval='0 */4 * * *',
    on_failure_callback=on_failure_callback,
    catchup=False,
    tags=['1C'],
) as dag:

    @task
    def stage_base_data():
        import sqlalchemy as sa
        """Таска для записи в staging слой."""
        df = get_data(ANALOGUE_QUERY)

        if df.empty:
            raise ValueError("Received empty DataFrame from ERP API")
        
        samara_offset = dt.timezone(dt.timedelta(hours=4))
        df['date_etl'] = dt.datetime.now(samara_offset)
        df['REG_DATE'] = pd.to_datetime(df['REG_DATE'])
        df['DATE_BEGIN'] = pd.to_datetime(df['DATE_BEGIN'])
        #заглушка для даты
        df['DATE_END'] = pd.to_datetime(df['DATE_END'], errors='coerce')
        default_date = dt.datetime(1900, 1, 1)
        df['DATE_END'] = df['DATE_END'].fillna(default_date)


        dtype = {
            "REG_DATE": sa.DATE,
            "DATE_BEGIN": sa.DATE,
            "DATE_END": sa.DATE,
            "REG_NUMBER": sa.VARCHAR(32),
            "CODE1C_MATERIAL": sa.VARCHAR(32),
            "COUNT_MATERIAL": sa.NUMERIC(16,6),
            "OKEI_MATERIAL": sa.VARCHAR(8),
            "CODE1C_ANALOG": sa.VARCHAR(32),
            "COUNT_ANALOG": sa.NUMERIC(16,6),
            "OKEI_ANALOG": sa.VARCHAR(8),
            "NPRIORITY": sa.NUMERIC(8,3),
            "IACTUAL": sa.NUMERIC(4,0)
            }

        df.to_sql(
            name='analogues',
            con=engine,
            if_exists='replace',
            index=False,
            dtype=dtype,
            schema='AIRFLOW',
            chunksize=5000
        )


    sql = """
    BEGIN
    -- Очищаем целевую таблицу
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DATA_EX.ANALOGUES';
    
    INSERT INTO DATA_EX.ANALOGUES (
        "REG_NUMBER",
        "REG_DATE",
        "DATE_BEGIN",
        "DATE_END",
        "CODE1C_MATERIAL",
        "COUNT_MATERIAL",
        "OKEI_MATERIAL",
        "CODE1C_ANALOG",
        "COUNT_ANALOG",
        "OKEI_ANALOG",
        "NPRIORITY",
        "IACTUAL",
        "DATE_ETL"
    )
    SELECT
        "REG_NUMBER",  
        "REG_DATE",
        "DATE_BEGIN",
        "DATE_END",
        "CODE1C_MATERIAL",
        "COUNT_MATERIAL",
        "OKEI_MATERIAL",
        "CODE1C_ANALOG",
        "COUNT_ANALOG",
        "OKEI_ANALOG",
        "NPRIORITY",
        "IACTUAL",
        SYSDATE  -- Текущая дата/время сервера
    FROM
        AIRFLOW.ANALOGUES;
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Ошибка при обновлении DATA_EX.ANALOGUES: ' || SQLERRM);
        RAISE;
END;
    """

    execute_oracle_transaction_analogues = OracleOperator(
            task_id='execute_oracle_transaction_analogues',
            oracle_conn_id='oracle_con',
            sql=sql,
            autocommit=True
        )


    # DAG Flow
    stage_base_data() >> execute_oracle_transaction_analogues
