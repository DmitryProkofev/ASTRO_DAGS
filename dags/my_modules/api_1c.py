import requests


class ApiBase:
    #TODO добавить запрос к ERP
    def __init__(self, url:str, auth:tuple):
        self.url = url
        self.auth = auth

    def get_data_zup(self):

        try:
            response = requests.get(url=self.url, auth=self.api_auth, timeout=10)
            response.raise_for_status()  
            
            return response.json()

        except requests.exceptions.Timeout:
            print("Ошибка: таймаут запроса API")
        except requests.exceptions.HTTPError as errh:
            print(f"HTTP ошибка: {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Ошибка соединения: {errc}")
        except requests.exceptions.RequestException as err:
            print(f"Другая ошибка: {err}")


