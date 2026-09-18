from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.hooks.base import BaseHook
from datetime import datetime


CONN_ID = "oracle_test_conn"


# --- Таски ---
def debug_connection(**context):
    """Печатает параметры Connection."""
    from my_modules.ora_think_activate import OracleThickModeManager
    oracle_manager = OracleThickModeManager()
    oracle_manager.ensure()

    
    conn = BaseHook.get_connection(CONN_ID)
    print("🔍 Параметры Connection:")
    print(f"   conn_id : {conn.conn_id}")
    print(f"   host    : {conn.host}")
    print(f"   port    : {conn.port}")
    print(f"   schema  : {conn.schema}  <-- это service_name")
    print(f"   login   : {conn.login}")
    print(f"   password: {'*' * len(conn.password) if conn.password else 'None'}")
    print(f"   extra   : {conn.extra}")


def test_dual(**context):
    """SELECT 1 FROM DUAL."""
    from my_modules.ora_think_activate import OracleThickModeManager
    oracle_manager = OracleThickModeManager()
    oracle_manager.ensure()
    
    hook = OracleHook(oracle_conn_id=CONN_ID)
    result = hook.get_first("SELECT 1 FROM DUAL")
    print(f"✅ Результат SELECT 1 FROM DUAL: {result}")


def test_target_query(**context):
    """Целевой запрос к AGRO.PA_COMPL_PARSE."""
    from my_modules.ora_think_activate import OracleThickModeManager
    oracle_manager = OracleThickModeManager()
    oracle_manager.ensure()
    
    hook = OracleHook(oracle_conn_id=CONN_ID)
    sql = 'SELECT ID, "IDPlanP" FROM AGRO.PA_COMPL_PARSE WHERE "IDPlanP" = :p'
    rows = hook.get_records(sql=sql, parameters={"p": 21461})
    print(f"✅ Получено строк: {len(rows)}")
    for r in rows[:10]:
        print(f"   {r}")


def debug_connection(**context):
    from my_modules.ora_think_activate import OracleThickModeManager
    oracle_manager = OracleThickModeManager()
    oracle_manager.ensure()
    
    conn = BaseHook.get_connection(CONN_ID)
    print("🔍 Параметры Connection:")
    print(f"   conn_id : {conn.conn_id}")
    print(f"   host    : {conn.host}")
    print(f"   port    : {conn.port}")
    print(f"   schema  : {conn.schema}")
    print(f"   login   : {conn.login}")
    print(f"   extra   : {conn.extra}")
    
    # Покажем, какой DSN сформирует Hook
    import oracledb
    dsn = oracledb.makedsn(conn.host, conn.port, service_name=conn.schema)
    print(f"\n📡 Сформированный DSN: {dsn}")


def insert_multiple_rows(**context):
    """Вставка нескольких записей через insert_rows."""
    from my_modules.ora_think_activate import OracleThickModeManager
    oracle_manager = OracleThickModeManager()
    oracle_manager.ensure()
    
    hook = OracleHook(oracle_conn_id=CONN_ID)
    
    # Подготавливаем данные
    rows_to_insert = [
        (1, "Value1", "Description1"),
        (2, "Value2", "Description2"),
        (3, "Value3", "Description3"),
    ]
    
    target_fields = ["ID", "FIELD_NAME", "DESCRIPTION"]
    
    try:
        hook.insert_rows(
            table="AIRFLOW.TEST_DATA",
            rows=rows_to_insert,
            target_fields=target_fields,
            commit_every=100  # Коммитить каждые 100 строк
        )
        print(f"✅ Успешно вставлено {len(rows_to_insert)} записей")
    except Exception as e:
        print(f"❌ Ошибка пакетной вставки: {e}")
        raise

def insert_multiple_rows_class(**context):
    from my_modules.ora_think_activate import OracleDataWriter

    writer = OracleDataWriter(conn_id=CONN_ID)
    
    # 2. Готовим данные
    rows_to_insert = [
        (1, "Value1", "Description1"),
        (2, "Value2", "Description2"),
        (3, "Value3", "Description3"),
    ]
    
    target_fields = ["ID", "FIELD_NAME", "DESCRIPTION"]
    
    # 3. Вызываем метод записи (валидация и Thick mode произойдут внутри автоматически!)
    writer.insert_multiple_rows(
        table="AIRFLOW.TEST_DATA",
        rows=rows_to_insert,
        target_fields=target_fields,
        commit_every=100
    )



# --- DAG ---
with DAG(
    dag_id="test_oracle_simple",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["oracle", "test"],
) as dag:

    t1 = PythonOperator(task_id="debug_connection", python_callable=debug_connection)
    t2 = PythonOperator(task_id="debug", python_callable=debug_connection)
    t3 = PythonOperator(task_id="select_1_from_dual", python_callable=test_dual)
    t4 = PythonOperator(task_id="target_query", python_callable=test_target_query)
    t5 = PythonOperator(task_id="insert_multiple_rows", python_callable=insert_multiple_rows)
    t6 = PythonOperator(task_id="insert_multiple_rows_class", python_callable=insert_multiple_rows_class)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6