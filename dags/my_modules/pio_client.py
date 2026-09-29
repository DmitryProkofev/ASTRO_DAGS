"""PIO Client - модуль для работы с API Пио (Internal Acts / Quality Assurance)

Перенесён и улучшен из tests/pio_dev.py.
Поддерживает:
- OAuth2 client_credentials flow
- Автоматическое обновление токена
- Постраничную выгрузку документов
- Инкрементальную загрузку по дате
- Сохранение в MinIO через S3Hook
"""

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import json
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class PioClient:
    """Клиент для API Пио (https://actservice.qa.pegas-agro.ru)"""

    def __init__(self):
        self.auth_url = Variable.get("pio_auth_url")
        self.api_url = Variable.get("pio_api_url")
        self.client_id = Variable.get("pio_client_id")
        self.client_secret = Variable.get("pio_client_secret")
        self.token: Optional[str] = None
        self._token_file = "/tmp/pio_token.json"  # для локального кэширования (опционально)

    def get_token(self, force_refresh: bool = False) -> str:
        """Получает OAuth2 токен. Кэширует его."""
        if self.token and not force_refresh:
            return self.token

        try:
            logger.info("🔑 Запрашиваем новый токен от PIO Auth")
            response = requests.post(
                self.auth_url,
                auth=HTTPBasicAuth(self.client_id, self.client_secret),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data={
                    'grant_type': 'client_credentials',
                    'scope': 'internal_acts.read'
                },
                verify=False,
                timeout=30
            )
            response.raise_for_status()
            self.token = response.json().get('access_token')
            logger.info("✅ Токен успешно получен")
            return self.token
        except Exception as e:
            logger.error(f"❌ Ошибка получения токена: {e}")
            raise

    def fetch_documents(
        self,
        from_date: Optional[str] = None,
        page_size: int = 50,
        max_pages: Optional[int] = None
    ) -> List[Dict]:
        """
        Выгружает документы из PIO API с пагинацией.
        
        Args:
            from_date: Дата в формате ISO (пример: '2026-09-20T10:00:00.000Z')
            page_size: Размер страницы (рекомендуется 50-200)
            max_pages: Ограничение количества страниц (для тестов)
        """
        token = self.get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        if not from_date:
            from_date = "2025-01-01T00:00:00.000Z"

        all_docs = []
        page_number = 0
        total_pages = 1

        logger.info(f"🚀 Начало выгрузки документов PIO с даты {from_date}")

        while page_number < total_pages and (max_pages is None or page_number < max_pages):
            params = {
                "type": "updated_at",
                "from": from_date,
                "page": {
                    "number": page_number,
                    "size": page_size
                }
            }

            try:
                response = requests.post(
                    url=self.api_url,
                    headers=headers,
                    json=params,
                    verify=False,
                    timeout=60
                )

                if response.status_code == 401:
                    logger.warning("⚠️ Токен истёк. Обновляем...")
                    self.get_token(force_refresh=True)
                    headers['Authorization'] = f'Bearer {self.token}'
                    continue

                response.raise_for_status()
                data = response.json()

                docs = data.get('content', [])
                all_docs.extend(docs)

                total_pages = data.get('totalPages', 1)
                page_number += 1

                logger.info(f"📄 Страница {page_number}/{total_pages} загружена. "
                          f"Всего документов: {len(all_docs)}")

            except Exception as e:
                logger.error(f"❌ Ошибка на странице {page_number}: {e}")
                break

        logger.info(f"✅ Выгрузка завершена. Всего документов: {len(all_docs)}")
        return all_docs

    def save_to_minio(self, documents: List[Dict], bucket: str = "pio-raw") -> str:
        """Сохраняет документы в MinIO в формате JSON"""
        if not documents:
            logger.warning("Нет данных для сохранения в MinIO")
            return ""

        s3_hook = S3Hook(aws_conn_id="minio_conn")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"pio_documents_{timestamp}.json"

        try:
            json_data = json.dumps(documents, ensure_ascii=False, indent=2)
            s3_hook.load_string(
                string_data=json_data,
                key=key,
                bucket_name=bucket,
                replace=True
            )
            logger.info(f"✅ Данные успешно сохранены в MinIO: s3://{bucket}/{key}")
            return key
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в MinIO: {e}")
            raise


# Для удобного использования как standalone скрипта
if __name__ == "__main__":
    client = PioClient()
    docs = client.fetch_documents(page_size=20, max_pages=3)
    if docs:
        client.save_to_minio(docs)
        print(f"Загружено {len(docs)} документов")
