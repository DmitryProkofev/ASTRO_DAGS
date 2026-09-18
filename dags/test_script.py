import pandas as pd
import requests
import sqlalchemy as sa
from datetime import datetime, timedelta


con_data = 'postgresql://airflow_etl:airpegas@10.1.11.17:5432/AGRO'
engine = sa.create_engine(con_data, pool_pre_ping=True)
api_auth = ('api', 'GjkmpjdfntkmFGB')

query = """
ВЫБРАТЬ    
    УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ЗаказКлиента.Ссылка) КАК UID, 
    ЗаказКлиента.Ссылка.Номер КАК ORDER,
    ЗаказКлиента.Дата КАК DATEOR,
    ЗаказКлиента.ДатаОтгрузки КАК DATESHIP,
    ЗаказКлиента.ЖелаемаяДатаОтгрузки КАК DATEWISH,
    ЗаказКлиента.Статус КАК STATUS,
    ЗаказКлиента.Договор КАК DOCUMENT,`
    ЗаказКлиента.Подразделение КАК OFFICE
ИЗ
    Документ.ЗаказКлиента КАК ЗаказКлиента     
ГДЕ
    ЗаказКлиента.Ссылка.Проведен
    И ЗаказКлиента.Подразделение.Код В ("ЦБ-000018", "ЗП00-0062") 
"""

def erp_api(query):
    url = f'http://192.168.0.112/1c-erp/hs/api/query?text={query}'
    response = requests.get(url, auth=api_auth)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        raise Exception(f'HTTP Error: {response.status_code}, {response.text}')


df = erp_api(query, con=engine)


df.to_sql('clientorder_subdivision', engine, if_exists='replace', index=False, schema='stage',
                    chunksize=5000)
