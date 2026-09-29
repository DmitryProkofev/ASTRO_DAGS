import pandas as pd
import sqlalchemy as sa
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
from notification_error import on_failure_callback
from airflow.models import Variable
from oracle_model import Oracle
import time
from main_parent import zup_api_all
from config import q_erp, api_auth
import requests


args = {'owner': 'airflow',
        'start_date': dt.datetime(2024, 7, 18, 9, 30), #dt.datetime.now(), dt.datetime(2024, 3, 30, 8, 0)
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=3),
    'depends_on_past': True,
    }

con_data = Variable.get("oracle_connection_pandas")
path_xcOracle = Variable.get("path_cxOracle")
#cx_Oracle.init_oracle_client(lib_dir=path_xcOracle)
engine = sa.create_engine(con_data)

def new_data():
    url_new = 'http://10.1.11.46/1c-zup-pegas/hs/get/employees?translit&archived&deleted'
    response = requests.get(url_new, auth=api_auth)
    if response.status_code != 200:
      raise Exception(f"Error query: {response.status_code}")
    response = response.json()
    df = pd.DataFrame(response)
    df['updated_ad'] = pd.Timestamp.now()
    
    df.to_sql('stg_employes_zup', engine, if_exists='replace', index=False, schema='AIRFLOW', chunksize=500)
    
def query_to_db(sql_query):
    db = Oracle()
    db.connect_oracle()
    db.execute_sql(sql_query)
    db.disconnect_oracle()
    

query_norm = """ CREATE OR REPLACE VIEW AIRFLOW.V_EMPLOYES_ZUP AS
SELECT
    TO_CHAR("Sotrudnik") AS FIO,
    TO_CHAR("Podrazdelenie") AS DIVISION,
    TO_CHAR("Dolzhnost") AS POSITION,
    TO_CHAR("TabelnyyNomer") AS PERSONNEL_NUMBER,
    CASE
        WHEN TO_DATE("DataNachalaUcheta", 'YYYY-MM-DD"T"HH24:MI:SS')
             = DATE '0001-01-01'
        THEN DATE '2099-01-01'
        ELSE TO_DATE("DataNachalaUcheta", 'YYYY-MM-DD"T"HH24:MI:SS')
    END AS DATE_ACCOUNTING,
    CASE
        WHEN TO_DATE("DataPriema", 'YYYY-MM-DD"T"HH24:MI:SS')
             = DATE '0001-01-01'
        THEN DATE '2099-01-01'
        ELSE TO_DATE("DataPriema", 'YYYY-MM-DD"T"HH24:MI:SS')
    END AS DATE_ADMISSION,
    CASE
        WHEN TO_DATE("DataUvolneniya", 'YYYY-MM-DD"T"HH24:MI:SS')
             = DATE '0001-01-01'
        THEN DATE '2099-01-01'
        ELSE TO_DATE("DataUvolneniya", 'YYYY-MM-DD"T"HH24:MI:SS')
    END AS DATE_DISSMISSION,
    TO_CHAR("Familiya") AS SURNAME,
    TO_CHAR("Imya") AS NAME,
    TO_CHAR("Otchestvo") AS PATRONYMIC,
    TO_DATE("DataRozhdeniya", 'YYYY-MM-DD"T"HH24:MI:SS') AS BIRTHDAY,
    TO_CHAR("Sotrudnik_UID") AS UID_EMPLOYEE,
    TO_CHAR("FizicheskoeLitso_UID") AS UID_PHYS,
    TO_CHAR("Podrazdelenie_UID") AS UID_DIVISION,
    TO_CHAR("Dolzhnost_UID") AS UID_POSITION,
    TO_CHAR("PodrazdelenieRoditel") AS PARENT_DIVISION,
    TO_CHAR("PodrazdelenieRoditel_UID") AS UID_PARENT_DIVISION,
    "VArkhive" AS ARCHIVE,
    "PometkaUdaleniya" AS DEL,
    UPDATED_AD
FROM AIRFLOW.STG_EMPLOYES_ZUP
 """


query_update = """BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DATA_EX.EMPLOYES_ZUP';
    
    INSERT INTO DATA_EX.EMPLOYES_ZUP
    SELECT
	FIO,
	DIVISION,
	"POSITION",
	PERSONNEL_NUMBER,
	DATE_DISSMISSION,
	SURNAME,
	NAME,
	PATRONYMIC,
	BIRTHDAY,
	UID_EMPLOYEE,
	UID_PHYS,
	UID_DIVISION,
	UID_POSITION,
	PARENT_DIVISION,
	UID_PARENT_DIVISION,
	ARCHIVE,
	DEL,
	DATE_ACCOUNTING,
	DATE_ADMISSION
FROM
	AIRFLOW.V_EMPLOYES_ZUP;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;"""

docstring = """Данный DAG берет данные из 1С ЗУП по HTTP API и пишет в БД Oracle

Общая структура

1С ЗУП (HTTP API)
        ↓
AIRFLOW.STG_EMPLOYES_ZUP (staging)
        ↓
AIRFLOW.V_EMPLOYES_ZUP (view, нормализация)
        ↓
DATA_EX.EMPLOYES_ZUP 

Из EMPLOYES_ZUP уже формируются представления V_OPER и V_EMPLOYES на которых завязаны многие production процессы."""

with DAG(
    dag_id='update_zup',
    schedule_interval='0 */4 * * *',
    default_args=args,
    on_failure_callback=on_failure_callback,
    catchup=False,
    doc_md=docstring

) as dag:

    get_new_data = PythonOperator(
            task_id='new_data',
            python_callable=new_data,
            dag=dag
        )
    
    start_query_norm = PythonOperator(
        task_id='start_query_norm',
        python_callable=query_to_db,
        op_args=[query_norm],
        dag=dag
    )

    update_data = PythonOperator(
        task_id='update_data',
        python_callable=query_to_db,
        op_args=[query_update],
        dag=dag
    )

    get_new_data >> start_query_norm >> update_data

    #