import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Подключение к ClickHouse через SQLAlchemy
# Для использования clickhouse-sqlalchemy, создайте строку подключения
engine = create_engine('clickhouse+http://default:@localhost:8123/default')

# Функция для загрузки данных по дате
def load_data_for_date(start_date, end_date):
    # Замените этот запрос на свой для получения данных из API
    query = f"""
    SELECT * FROM your_table
    WHERE created_at >= '{start_date}' AND created_at < '{end_date}'
    """
    
    # Получаем данные в DataFrame
    df = pd.read_sql(query, engine)
    
    # Загружаем данные в таблицу с использованием pandas.to_sql
    df.to_sql('your_table_name', engine, if_exists='append', index=False)
    print(f"Данные за {start_date} - {end_date} загружены!")

# Задаём начальную и конечную дату для итеративной загрузки
start_date = datetime(2023, 1, 1)
end_date = start_date + timedelta(days=1)

# Период загрузки: например, по дням
while start_date < datetime(2023, 12, 31):  # До конца 2023 года
    # Загружаем данные за каждый день
    load_data_for_date(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    
    # Переход к следующему дню
    start_date = end_date
    end_date = start_date + timedelta(days=1)
