"""
PostgresWatermarkManager - универсальный менеджер для получения и обновления
watermark по полю ingestion_time из любой таблицы Postgres.
Использует Variable для хранения последней даты.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class PostgresWatermarkManager:
    """
    Универсальный менеджер watermark на основе Postgres + Airflow Variables.
    """

    def __init__(self, postgres_conn_id: str = "postgres_default"):
        self.conn_id = postgres_conn_id

    def get_last_ingestion_date(
        self,
        table_name: str,
        schema: str = "public",
        variable_name: Optional[str] = None,
        default_days_ago: int = 30,
        column_name: str = "ingestion_time"
    ) -> str:
        """
        Возвращает последнюю дату из поля ingestion_time.
        Сначала проверяет Variable, затем делает запрос в Postgres.
        """
        if not variable_name:
            variable_name = f"last_ingestion_date_{table_name}"

        # 1. Пытаемся взять дату из Variable
        last_date = Variable.get(variable_name, default_var=None)
        
        if last_date:
            logger.info(f"✅ Watermark из Variable {variable_name}: {last_date}")
            return last_date

        # 2. Если в Variable нет — запрашиваем MAX() из таблицы
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        full_table = f"{schema}.{table_name}" if schema != "public" else table_name

        sql = f"""
            SELECT MAX({column_name}) 
            FROM {full_table}
            WHERE {column_name} IS NOT NULL
        """

        try:
            result = hook.get_first(sql)
            db_date = result[0] if result and result[0] else None

            if db_date:
                if isinstance(db_date, datetime):
                    iso_date = db_date.isoformat()
                    if not iso_date.endswith('Z'):
                        iso_date += 'Z'
                    Variable.set(variable_name, iso_date)
                    logger.info(f"✅ Получена дата из Postgres ({full_table}): {iso_date}")
                    return iso_date
                else:
                    str_date = str(db_date)
                    Variable.set(variable_name, str_date)
                    return str_date

            # 3. Если таблица пустая — возвращаем fallback
            fallback_date = (datetime.utcnow() - timedelta(days=default_days_ago)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ).isoformat() + "Z"
            
            Variable.set(variable_name, fallback_date)
            logger.info(f"⚠️ Таблица {full_table} пуста. Установлен fallback: {fallback_date}")
            return fallback_date

        except Exception as e:
            logger.error(f"❌ Ошибка получения MAX({column_name}) из {full_table}: {e}")
            fallback_date = (datetime.utcnow() - timedelta(days=default_days_ago)).isoformat() + "Z"
            Variable.set(variable_name, fallback_date)
            return fallback_date

    def update_watermark(self, variable_name: str, date_value: str) -> None:
        """Обновляет watermark в Variable"""
        Variable.set(variable_name, date_value)
        logger.info(f"✅ Watermark обновлён: {variable_name} = {date_value}")
