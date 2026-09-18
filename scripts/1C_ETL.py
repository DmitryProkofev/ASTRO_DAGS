import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import ijson
import io
import clickhouse_connect
import time


# client = clickhouse_connect.get_client(
#     host='10.1.11.65', port=8123, username='default', password='pegas_warehouse2025'
# )


# TODO загрузить запрос частями по дате >> закинуть в кликхаус >> обновлять толко период (например 3 месяца)
api_auth = ('api', 'GjkmpjdfntkmFGB')

query_text1 = '''select * from РегистрНакопления.ТоварыКОтгрузке ГДЕ Период МЕЖДУ %26ДатаНачала И %26ДатаОкончания'''
query_text2 = """"""


def erp_api(query, time_start, time_end):
    url = f'http://192.168.0.112/1c-erp/hs/api/query?ДатаНачала^={time_start}&ДатаОкончания^={time_end}&text={query}'

    start_time = time.time()

    response = requests.get(url, auth=api_auth)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения запроса: {execution_time:.2f} секунд")

    if response.status_code == 200:
        start_time = time.time()
        result = pd.DataFrame(response.json())
        end_time = time.time()
        execution_time = end_time - start_time
        print(
            f"Время выполнения формирования Dataframe: {execution_time:.2f} секунд")
        return result

    else:
        raise Exception(f'response.status_code != 200\nERROR:{response.text}')


def load_to_clickhouse(query, start_date, table_name, days_per_chunk=10):
    start_date = pd.to_datetime(start_date)
    end_date = datetime.now()

    while start_date < end_date:
        chunk_end = min(start_date + timedelta(days=days_per_chunk), end_date)
        print(f"Загружаем {start_date.date()} — {chunk_end.date()}")

        df = erp_api(query, start_date.isoformat(), chunk_end.isoformat())

        # TODO добавить колонку created_at
        # TODO добавить обработку ошибок и перезапуск
        df.rename(columns={
            'Период': 'period',
            'Регистратор': 'registrator',
            'НомерСтроки': 'line_number',
            'Активность': 'is_active',
            'ВидДвижения': 'movement_type',
            'Склад': 'warehouse',
            'Получатель': 'receiver',
            'ДокументОтгрузки': 'shipment_document',
            'Номенклатура': 'item',
            'Характеристика': 'item_property',
            'Назначение': 'purpose',
            'Серия': 'series',
            'ВРезерве': 'reserved_qty',
            'КОтгрузке': 'to_ship_qty',
            'КОформлению': 'to_process_qty',
            'КСборке': 'to_assemble_qty',
            'Собирается': 'assembling_qty',
            'Собрано': 'assembled_qty',
            'ЗаданиеНаПеревозку': 'transport_task'
        }, inplace=True)

        if not df.empty:
            df['period'] = pd.to_datetime(
                df['period'], format='%Y-%m-%d', errors='coerce')
            df['period'] = df['period'].fillna(pd.Timestamp('1970-01-01'))
            df['created_at'] = datetime.now()

            client.insert_df(table=table_name, df=df)
            print(f"Загружено {len(df)} строк в {table_name}")
        else:
            print("Пустой фрейм, пропускаем.")

        start_date = chunk_end


load_to_clickhouse(query_text1, '2025-01-01T00:00:00', 'goods_to_ship')

