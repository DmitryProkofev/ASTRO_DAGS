import pandas as pd
import sqlalchemy as sa
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
from notification_error import on_failure_callback
from airflow.models import Variable
from oracle_model import Oracle
import time
from main_parent import erp_api_all
from config import q_erp


args = {'owner': 'airflow',
        'start_date': dt.datetime(2024, 10, 8, 8, 0), #dt.datetime.now(), dt.datetime(2024, 3, 30, 8, 0)
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': True,
    }

con_data = Variable.get("oracle_connection_pandas")
path_xcOracle = Variable.get("path_cxOracle")
#cx_Oracle.init_oracle_client(lib_dir=path_xcOracle)
engine = sa.create_engine(con_data)

def new_data():
    api_data = erp_api_all(q_erp)
    api_data.columns = ['SURNAME', 'NAME', 'PATRONYMIC', 'FIO', 'BARCODE', 'UID_PHYS']

    df = api_data[~api_data['UID_PHYS'].isna() & (api_data['UID_PHYS'] != '')]

    column_data_types_sheet = {'SURNAME': sa.VARCHAR(255),
                               'NAME': sa.VARCHAR(255),
                               'PATRONYMIC': sa.VARCHAR(255),
                               'FIO': sa.VARCHAR(255),
                               'UID_PHYS': sa.VARCHAR(255)}
    df.to_sql('employes_erp', engine, if_exists='replace', index=False, schema='AIRFLOW',
              chunksize=5000, dtype=column_data_types_sheet)
    
def query_to_db(sql_query):
    db = Oracle()
    db.connect_oracle()
    db.execute_sql(sql_query)
    db.disconnect_oracle()



cdm_1_query = """INSERT INTO DATA_EX.EMPLOYES_BARCODES
          SELECT nt.*, SYSTIMESTAMP AS current_time
          FROM AIRFLOW.EMPLOYES_ERP nt
          LEFT JOIN DATA_EX.EMPLOYES_BARCODES ot
          ON ot.BARCODE = nt.BARCODE
          WHERE ot.BARCODE IS NULL"""


cdm_2_query = """BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DATA_EX.EMPLOYES_ERP';
    
    INSERT INTO DATA_EX.EMPLOYES_ERP
    SELECT SURNAME, NAME, PATRONYMIC, FIO, BARCODE, UID_PHYS, CURRENT_TIME
    FROM (
        SELECT
            eb.*,
            ROW_NUMBER() OVER (PARTITION BY UID_PHYS ORDER BY CURRENT_TIME DESC) AS ROW_num
        FROM
            DATA_EX.EMPLOYES_BARCODES eb
    )
    WHERE ROW_NUM = 1;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;"""

with DAG(
    dag_id='update_erp',
    schedule_interval='0 */4 * * *',
    default_args=args,
    on_failure_callback=on_failure_callback,
    catchup=False

) as dag:

    get_new_data = PythonOperator(
            task_id='new_data',
            python_callable=new_data,
            dag=dag
        )

    cdm_1 = PythonOperator(
        task_id='cdm_1',
        python_callable=query_to_db,
        op_args=[cdm_1_query],
        dag=dag
    )

    cdm_2 = PythonOperator(
        task_id='cdm_2',
        python_callable=query_to_db,
        op_args=[cdm_2_query],
        dag=dag
    )

    get_new_data >> cdm_1 >> cdm_2

