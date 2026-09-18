import oracledb

# ----------------------------
# ENVIRONMENT CONFIGURATION
# ----------------------------
ORACLE_CONNECTION_URI = Variable.get("oracle_connection_pandas")
engine = create_engine(ORACLE_CONNECTION_URI)

path_xcOracle = Variable.get("path_cxOracle")
oracledb.init_oracle_client(lib_dir=path_xcOracle)


def hello():
    x = 'Hello'
    return x


def check_data_exists(sql_path: str):
    from custom_utils.clickhouse_client import ClickHouseClient
    client = ClickHouseClient(conn_id='click_connect')
    try:
        result = client.query(sql_path)
        return bool(result)
    except Exception as e:
        import logging
        logging.error(f"Ошибка выполнения запроса: {e}")
        raise


def test_click():
    bronze = PythonOperator(
        task_id=f'default_table',
        python_callable=check_data_exists,
        op_kwargs={'sql_path': 'select 1'},
    )

    result = bronze.execute(context={})
    assert result is True


def test_check_operator():
    check = SQLCheckOperator(
            task_id="test_check",
            sql="select 1",
            conn_id='click_connect',
        )
    
    result = check.execute(context={})
    assert result is True


def test_zoller_data():
    task = PythonOperator(
        task_id='new_data',
        python_callable=new_data,
        dag=dag
    )

    result = task.execute(context={})
    assert result is True


