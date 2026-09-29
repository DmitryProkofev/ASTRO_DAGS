import os
import logging
from typing import List, Any, Optional
from airflow.providers.oracle.hooks.oracle import OracleHook

logger = logging.getLogger(__name__)


class OracleDataWriter:
    """
    Простой и надёжный класс для работы с Oracle в Airflow.
    Использует только OracleHook (Thin mode) — без init_oracle_client() и DPY-2017.
    """

    def __init__(self, conn_id: str = "oracle_test_conn"):
        self.conn_id = conn_id
        logger.info(f"OracleDataWriter initialized with conn_id={conn_id} (Thin mode)")

    def insert_multiple_rows(
        self,
        table: str,
        rows: List[tuple],
        target_fields: List[str],
        commit_every: int = 1000
    ) -> int:
        """Вставка множества строк через явный executemany() — самый стабильный способ для Oracle Thin mode.
        Избегает ORA-00984, который возникает при использовании hook.insert_rows().
        """
        if not rows:
            logger.warning("Пустой список rows, пропускаем вставку.")
            return 0

        if len(rows[0]) != len(target_fields):
            raise ValueError(f"Несоответствие колонок: {len(target_fields)} полей, но {len(rows[0])} значений в строке")

        hook = OracleHook(oracle_conn_id=self.conn_id)

        # Формируем SQL вручную
        columns = ', '.join(target_fields)
        placeholders = ', '.join([':' + str(i+1) for i in range(len(target_fields))])
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        try:
            logger.info(f"Вставка {len(rows)} строк в таблицу {table} (executemany)")
            conn = hook.get_conn()
            cur = conn.cursor()
            
            cur.executemany(sql, rows)
            
            if commit_every >= len(rows):
                conn.commit()
                logger.info(f"✅ Успешно вставлено {len(rows)} строк в {table}")
            else:
                # commit_every работает как batch size
                for i in range(0, len(rows), commit_every):
                    batch = rows[i:i + commit_every]
                    cur.executemany(sql, batch)
                    conn.commit()
                logger.info(f"✅ Успешно вставлено {len(rows)} строк в {table} (batched)")

            return len(rows)
        except Exception as e:
            err = str(e).upper()
            if "ORA-00984" in err:
                logger.error("ORA-00984: Возможно, в данных есть NULL или несовместимый тип. Проверьте наличие None в строках.")
            if "ORA-12543" in err or "UNREACHABLE" in err:
                logger.error("🔴 ORA-12543: База данных недоступна. Проверьте VPN, сеть или параметры подключения.")
            elif "DPY-2017" in err:
                logger.error("DPY-2017: Неожиданно вернулся. Сообщите полный лог.")
            logger.error(f"Ошибка вставки в {table}: {e}")
            raise
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

    def execute_sql_from_file(self, sql_filename: str, sql_dir: Optional[str] = None) -> None:
        """Выполнение SQL-скрипта из файла."""
        if sql_dir is None:
            sql_dir = os.path.join(os.path.dirname(__file__), "sql")

        filepath = os.path.join(sql_dir, sql_filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Файл не найден: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read()

        hook = OracleHook(oracle_conn_id=self.conn_id)
        logger.info(f"Выполнение SQL из файла: {sql_filename}")

        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
            logger.info(f"✅ SQL из {sql_filename} выполнен успешно")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка выполнения {sql_filename}: {e}")
            raise
        finally:
            cur.close()
            conn.close()


# Для совместимости со старым кодом
Oracle = OracleDataWriter


logger.info("✅ ora_think_activate.py успешно загружен в чистом Thin-режиме.")
