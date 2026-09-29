import json
from io import BytesIO
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import io


class MinioBase:
    """
    Базовый клиент для работы с MinIO (вариант A по SOLID).
    
    Улучшения:
    - Добавлен метод object_exists() — чёткое разделение ответственности
    - Единообразная и подробная обработка ошибок
    - Добавлены три метода получения данных
    - Улучшены type hints и документация
    - Сохранена обратная совместимость
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str, 
                 secure: bool = False, region: str = "us-west-2"):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )

    def object_exists(self, bucket: str, object_name: str) -> bool:
        """
        Проверяет существование объекта в бакете.
        """
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error as err:
            if err.code == 'NoSuchKey':
                return False
            print(f"Ошибка при проверке существования объекта {bucket}/{object_name}: {err}")
            raise
        except Exception as err:
            print(f"Неожиданная ошибка при проверке объекта {bucket}/{object_name}: {err}")
            raise

    def upload_json(self, bucket: str, object_name: str, data: dict) -> bool:
        """
        Загружает словарь data как JSON в указанный бакет и объект MinIO.
        """
        try:
            json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
            stream = BytesIO(json_bytes)

            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)

            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=stream,
                length=len(json_bytes),
                content_type="application/json"
            )
            return True
        
        except Exception as err:
            print(f"Ошибка при записи JSON в MinIO ({bucket}/{object_name}): {err}")
            raise

    def upload_raw(self, bucket: str, object_name: str, data: bytes, 
                   content_type: str = "application/octet-stream"):
        """
        Загружает сырые байты (XML, CSV, изображения и т.д.) в MinIO.
        """
        try:
            return self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type
            )
        except Exception as err:
            print(f"Ошибка при записи raw данных в MinIO ({bucket}/{object_name}): {err}")
            raise

    def get_json(self, bucket: str, object_name: str) -> dict:
        """
        Получает JSON-объект из MinIO и возвращает как словарь.
        """
        try:
            response = self.client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return json.loads(data.decode('utf-8'))
        except S3Error as err:
            if err.code == 'NoSuchKey':
                print(f"Объект не найден: {bucket}/{object_name}")
            else:
                print(f"Ошибка MinIO при получении JSON {bucket}/{object_name}: {err}")
            raise
        except json.JSONDecodeError as err:
            print(f"Ошибка парсинга JSON из {bucket}/{object_name}: {err}")
            raise
        except Exception as err:
            print(f"Неожиданная ошибка при получении JSON {bucket}/{object_name}: {err}")
            raise

    def get_object(self, bucket: str, object_name: str) -> bytes:
        """
        Возвращает содержимое объекта как bytes.
        Подходит для XML, CSV, parquet, изображений и других форматов.
        """
        try:
            response = self.client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as err:
            if err.code == 'NoSuchKey':
                print(f"Объект не найден: {bucket}/{object_name}")
            else:
                print(f"Ошибка MinIO при получении объекта {bucket}/{object_name}: {err}")
            raise
        except Exception as err:
            print(f"Неожиданная ошибка при получении объекта {bucket}/{object_name}: {err}")
            raise

    def get_presigned_url(self, bucket: str, object_name: str, expires: int = 3600) -> str:
        """
        Возвращает временную presigned URL для доступа к объекту без авторизации.
        """
        try:
            url = self.client.presigned_get_object(
                bucket_name=bucket,
                object_name=object_name,
                expires=expires
            )
            return url
        except S3Error as err:
            if err.code == 'NoSuchKey':
                print(f"Объект не найден: {bucket}/{object_name}")
            else:
                print(f"Ошибка при создании presigned URL для {bucket}/{object_name}: {err}")
            raise
        except Exception as err:
            print(f"Неожиданная ошибка при создании presigned URL: {err}")
            raise


# Пример использования:
# if __name__ == "__main__":
#     minio_client = MinioBase(
#         endpoint="10.1.11.65:9090",
#         access_key="8Rr1cXRj7BlocpC2cSS1",
#         secret_key="AEBCyC74YYDI4U86mmbhwpcwlZO1e8SCqnBcntzW",
#         secure=False
#     )

#     data = {"hello": "airflow", "timestamp": str(datetime.utcnow())}
#     minio_client.upload_json(
#         bucket="testbucket", 
#         object_name=f"airflow_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.json", 
#         data=data
#     )

#     print("MinioBase успешно инициализирован и готов к использованию.")

