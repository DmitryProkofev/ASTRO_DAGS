from lxml import etree
import oracledb
import pandas as pd
import requests
import sqlalchemy as sa
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
from notification_error import on_failure_callback
from airflow.models import Variable
from airflow.operators.oracle_operator import OracleOperator
from loguru import logger
import time
from airflow.utils.task_group import TaskGroup


args = {'owner': 'airflow',
        'start_date': dt.datetime.now(), #dt.datetime.now(), dt.datetime(2024, 3, 30, 8, 0)
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=15),
    'depends_on_past': False,
    }


con_data = Variable.get("oracle_connection_pandas")
path_xcOracle = Variable.get("path_cxOracle")
oracledb.init_oracle_client(lib_dir=path_xcOracle)
engine = sa.create_engine(con_data)



def get_data_NL():
    """
    Получаем по api список наладочных листов и записываем в схему airflow_data
    :return:
    """
    response = requests.get('http://pa-srv41/ZollerDbService/SettingSheet')
    root = etree.fromstring(response.text)
    elements = root.xpath("//SettingSheet")
    data = []
    for el in elements:
        data_dict = {}
        data_dict['id_NL'] = el.xpath("SettingSheet.SettingSheetId")[0].text
        articleNo = el.xpath("SettingSheet.ArticleNo")
        if len(articleNo) > 0:
            data_dict['articleNo'] = articleNo[0].text
        else:
            data_dict['articleNo'] = None
        data.append(data_dict)

    df = pd.DataFrame(data)

    column_data_types_sheet = {'index': sa.Integer,
                               'id_NL': sa.VARCHAR(64),
                               'articleNo': sa.VARCHAR(64)}

    df.to_sql('zlr_setting_sheets_new', engine, if_exists='replace', index=False, schema='AIRFLOW',
              chunksize=5000, dtype=column_data_types_sheet)


def new_data():
    df = pd.read_sql('''select "id_NL" from AIRFLOW.ZLR_SETTING_SHEETS_NEW''', engine)
    list_NL = list(df['id_NL'])
    data_1 = []
    data_2 = []
    data_3 = {}
    count = 0
    for el in list_NL:
        if el != 'test':
            data_dict_1 = {'SettingSheetId': None,
                           'DrawingNo': None,
                           "MachineId": None,
                           "Name": None}

            # Установите максимальное количество попыток
            max_retries = 3

            # Счетчик повторов
            retry_count = 0

            while retry_count < max_retries:
                try:
                    # Выполните запрос
                    logger.debug(f"Выполняем запрос к {el}")
                    response = requests.get(f'http://pa-srv41/ZollerDbService/SettingSheet/{el}')

                    # Проверьте успешность запроса (код состояния 200)
                    if response.status_code == 200:
                        print(f"list number {el} is done")
                        # Обработайте ответ при необходимости
                        break  # Выйдите из цикла, если запрос был успешным
                    else:
                        # Если запрос не удался, вызовите исключение
                        print(f"list number {el} {response.status_code}, {response.text}")
                        response.raise_for_status()
                except ConnectionError as e:
                    logger.debug(f"Ошибка подключения: {e}")
                    retry_count += 1
                    time.sleep(30)
                    if retry_count < max_retries:
                        print(f"Повторная попытка ({retry_count}/{max_retries})...")
                    else:
                        print("Достигнуто максимальное количество попыток. Завершение.")
                        break
                except requests.RequestException as e:
                    print(f"Ошибка запроса: {e}")
                    break  # Выйдите из цикла для других связанных с запросом ошибок
                except Exception as err:
                    print(f"ERR: Запрос к {el} вызвал ошибку: {err}")
                    break

            root = etree.fromstring(response.text)

            SettingSheetId = root.xpath("//SettingSheetId")[0].text
            data_dict_1['SettingSheetId'] = SettingSheetId
            DrawingNo = root.xpath("//DrawingNo")
            MachineId = root.xpath("//MachineId")
            Name = root.xpath("//Name")
            ToolId = root.xpath("//Tool")

            # получение данных по износу для отдельной таблицы
            data_none = root.xpath("//*[starts-with(name(), 'ID')]")

            for tld in range(len(ToolId)):
                data_dict_2 = {}
                data_dict_2['ToolId'] = ToolId[tld].xpath('ToolId')[0].text
                data_dict_2['ToolInv'] = int(ToolId[tld].xpath('ToolInv')[0].text.replace('}', '').replace('{', ''))
                data_dict_2['Description'] = None
                data_dict_2['GraphicGroup'] = None
                if len(ToolId[tld].xpath('GraphicGroup')) > 0:
                    data_dict_2['GraphicGroup'] = ToolId[tld].xpath('GraphicGroup')[0].text
                if len(ToolId[tld].xpath('Description')) > 0:
                    data_dict_2['Description'] = ToolId[tld].xpath('Description')[0].text
                data_dict_2['SettingSheetId'] = SettingSheetId

                try:
                    if data_none:
                        data_dict_2['EndurZoll'] = data_none[tld].text
                    else:
                        data_dict_2['EndurZoll'] = 0
                except Exception as err:
                    if 'list index out of range' in str(err):
                        data_dict_2['EndurZoll'] = 0
                        print('Ошибка: list index out of range')
                    else:
                        print(f'Произошла другая ошибка: {err}')
                        raise

                data_2.append(data_dict_2)


                for tld in range(len(ToolId)):
                    componentIDdata = ToolId[tld].xpath('.//ComponentId')
                    for el in componentIDdata:
                        data_dict_3 = {}
                        ToolInv = int(ToolId[tld].xpath('ToolInv')[0].text.replace('}', '').replace('{', ''))
                        data_dict_3['nomen'] = el.text
                        data_dict_3['nomen_name'] = el.xpath('../Description')[0].text
                        key_data = f"{ToolInv}&&&{data_dict_3['nomen']}"
                        res_data = data_3.get(key_data, 1)
                        if res_data == 1:
                            data_3[key_data] = data_dict_3

            ComponentId = root.xpath("//ComponentId")
            Description = root.xpath("//Description")
            if len(DrawingNo) > 0:
                data_dict_1['DrawingNo'] = root.xpath("//DrawingNo")[0].text
            if len(MachineId) > 0:
                data_dict_1['MachineId'] = root.xpath("//MachineId")[0].text
            if len(Name) > 0:
                data_dict_1['Name'] = root.xpath("//Name")[0].text
            name_Machine = root.xpath('//Machine/Name')
            if len(name_Machine) > 0:
                data_dict_1['name_Machine'] = name_Machine[0].text
            machine_description = root.xpath('//Machine/Description')
            if len(machine_description) > 0:
                data_dict_1['machine_description'] = machine_description[0].text
            comment_data = root.xpath("//CostCenter")
            if len(comment_data) > 0:
                data_dict_1['IDSKUWPJOB'] = comment_data[0].text

            data_1.append(data_dict_1)

            count += 1

    df_1 = pd.DataFrame(data_1)
    df_1.set_index('SettingSheetId', inplace=True)

    df_2 = pd.DataFrame(data_2)
    #создание уникального ключа для таблицы zlr_instrument
    df_2['key1'] = df_2['ToolId'].str.replace('РИ-', '').astype(int).astype(str)
    df_2['key2'] = df_2['SettingSheetId'].str.replace('НЛ-', '').astype(int).astype(str)
    df_2['key3'] = df_2['ToolInv'].astype(str)
    df_2['id_key'] = df_2['key1'] + df_2['key2'] + df_2['key3']
    df_2['id_key'] = df_2['id_key'].astype(int)
    df_2['EndurZoll'] = df_2['EndurZoll'].astype(int)
    df_2.drop(columns=['key1'], inplace=True), df_2.drop(columns=['key2'], inplace=True), df_2.drop(columns=['key3'],
                                                                                                    inplace=True)
    df_2.set_index('id_key', inplace=True)


    df_3 = pd.DataFrame.from_dict(data_3, orient='index')
    df_3.reset_index(drop=False, inplace=True)
    df_3['index'] = df_3['index'].str.split('&&&').str[0]
    df_3 = df_3.rename(columns={'index': 'ToolInv'})
    # создание уникального ключа для таблицы zlr_component
    df_3['key1'] = df_3['nomen'].str.replace('ЦБ-', '').astype(int).astype(str)
    df_3['id_key'] = df_3['key1'] + df_3['ToolInv']
    df_3['id_key'] = df_3['id_key'].astype(int)
    df_3.drop(columns=['key1'], inplace=True)
    df_3.set_index('id_key', inplace=True)


    column_data_types_list = {
        'SettingSheetId': sa.VARCHAR(64),
        'DrawingNo': sa.VARCHAR(64),
        'MachineId': sa.VARCHAR(32),
        'Name': sa.VARCHAR(64),
        'name_Machine': sa.VARCHAR(64),
        'machine_description': sa.VARCHAR(255),
        'IDSKUWPJOB': sa.Integer}

    column_data_types_instrument = {'id_key': sa.Integer,
                                    'ToolId': sa.VARCHAR(64),
                                    'ToolInv': sa.Integer,
                                    'Description': sa.VARCHAR(64),
                                    'GraphicGroup': sa.VARCHAR(64),
                                    'SettingSheetId': sa.VARCHAR(64),
                                    'EndurZoll': sa.Integer}

    column_data_types_components = {'index': sa.Integer,
                                    'ToolInv': sa.Integer,
                                    'nomen': sa.VARCHAR(64),
                                    'nomen_name': sa.VARCHAR(64)}

    df_1.to_sql('zlr_list_new', engine, if_exists='replace', index=True, schema='AIRFLOW', chunksize=5000,
                dtype=column_data_types_list)

    df_2.to_sql('zlr_instrument_new', engine, if_exists='replace', index=True, schema='AIRFLOW', chunksize=5000,
                dtype=column_data_types_instrument)

    df_3.to_sql('zlr_components_new', engine, if_exists='replace', index=True, schema='AIRFLOW', chunksize=5000,
                dtype=column_data_types_components)


with DAG(
    dag_id='update_zoller',
    schedule_interval='0 */4 * * *',
    default_args=args,
    on_failure_callback=on_failure_callback

) as dag:

    get_new_NL = PythonOperator(
            task_id='get_data_NL',
            python_callable=get_data_NL,
            dag=dag
        )

    get_new_data = PythonOperator(
        task_id='new_data',
        python_callable=new_data,
        dag=dag
    )

    create_key = OracleOperator(
            task_id='create_key',
            oracle_conn_id='oracle_con',
            sql='sql/zoller/create_key.sql',
            autocommit=True
        )


    with TaskGroup("update_tables") as update_tables:
        update_ZLR_LIST = OracleOperator(
            task_id='update_list',
            oracle_conn_id='oracle_con',
            sql='sql/zoller/list.sql',
            autocommit=True
        )

        update_ZLT_COMPONENTS = OracleOperator(
            task_id='update_components',
            oracle_conn_id='oracle_con',
            sql='sql/zoller/components.sql',
            autocommit=True
        )

        update_ZLT_INSTRUMENT = OracleOperator(
            task_id='update_instrument',
            oracle_conn_id='oracle_con',
            sql='sql/zoller/instrument.sql',
            autocommit=True
        )

    get_new_NL >> get_new_data >> create_key >> update_tables






