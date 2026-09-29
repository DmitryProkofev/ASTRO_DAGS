# PIO Dev - пример использования нового модуля pio_client.py
# Перенесено и улучшено из старой версии tests/pio_dev.py

from my_modules.pio_client import PioClient
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    client = PioClient()
    
    # Пример: выгрузка документов за последний месяц
    from_date = "2026-08-01T00:00:00.000Z"
    
    print(f"Запуск выгрузки документов PIO с {from_date}...")
    documents = client.fetch_documents(
        from_date=from_date,
        page_size=50,
        max_pages=None  # снять ограничение для полной выгрузки
    )
    
    if documents:
        key = client.save_to_minio(documents, bucket="pio-raw")
        print(f"\n✅ Успешно выгружено {len(documents)} документов!")
        print(f"Файл сохранён в MinIO: {key}")
    else:
        print("\n❌ Документы не найдены или произошла ошибка.")