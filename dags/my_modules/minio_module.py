import json
from io import BytesIO
from datetime import datetime
from minio import Minio
import io

class MinioBase:
    #TODO добавить метод получения
    def __init__(self, endpoint, access_key, secret_key, secure=False, region="us-west-2"):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )

    def upload_json(self, bucket: str, object_name: str, data: dict):
        """
        Загружает словарь data как JSON в указанный бакет и объект MinIO.
        """
        try:
            json_bytes = json.dumps(data).encode("utf-8")
            stream = BytesIO(json_bytes)

            # создаём бакет при необходимости
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)

            # загрузка объекта
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=stream,
                length=len(json_bytes),
                content_type="application/json"
            )
            return True
        
        except Exception as err:
            print(f"Ошибка при записи в MinIO: {err}")
            raise

    def upload_raw(self, bucket: str, object_name: str, data: bytes, content_type: str = "application/octet-stream"):
        """
        Загружает сырые байты (XML, CSV, изображения и т.д.) в MinIO.
        """
        return self.client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(data),  # Превращаем байты в файловый поток
            length=len(data),       # MinIO требует знать размер заранее
            content_type=content_type
        )

# # Пример использования:
# if __name__ == "__main__":
#     minio_client = MinioBase(
#         endpoint="10.1.11.65:9090",
#         access_key="8Rr1cXRj7BlocpC2cSS1",
#         secret_key="AEBCyC74YYDI4U86mmbhwpcwlZO1e8SCqnBcntzW",
#         secure=False
#     )

#     data = {"hello": "airflow", "timestamp": str(datetime.utcnow())}
#     minio_client.upload_json(bucket="testbucket", object_name=f"airflow_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.json", data=data)
