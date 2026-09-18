import requests
import time
import json


api_auth = ('api', 'GjkmpjdfntkmFGB')

query_text1 = '''select * from РегистрНакопления.ТоварыКОтгрузке ГДЕ Период МЕЖДУ %26ДатаНачала И %26ДатаОкончания'''

def erp_api(query, time_start, time_end, file_path='response.json'):
    url = f'http://192.168.0.112/1c-erp/hs/api/query?ДатаНачала^={time_start}&ДатаОкончания^={time_end}&text={query}'

    start_time = time.time()

    # Отправка запроса к API
    response = requests.get(url, auth=api_auth)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения запроса: {execution_time:.2f} секунд")

    # Проверяем статус ответа
    if response.status_code == 200:
        start_time = time.time()

        # Чтение и запись данных в JSON файл в потоке
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(response.json(), json_file, ensure_ascii=False, indent=4)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения записи в JSON файл: {execution_time:.2f} секунд")
        

    else:
        raise Exception(f'response.status_code != 200\nERROR:{response.text}')


erp_api(query_text1, '2025-01-01T00:00:00', '2025-01-11T00:00:00')
