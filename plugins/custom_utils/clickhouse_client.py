import clickhouse_connect
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from jinja2 import Template
import logging
import os
from abc import ABC, abstractmethod


# 1. Класс для подключения к ClickHouse
class ClickHouseConnection:
    def __init__(self, conn_id: str = 'click_connect', database: str = None):
        self.logger = logging.getLogger("airflow.task")
        self.conn_id = conn_id
        self.database = database
        self.client = None
        self._connect()

    def _connect(self):
        try:
            conn = BaseHook.get_connection(self.conn_id)
            self.logger.info("Подключение к ClickHouse (%s:%s)", conn.host, conn.port or 8123)

            self.client = clickhouse_connect.get_client(
                host=conn.host,
                port=conn.port or 8123,
                username=conn.login,
                password=conn.password,
                database=self.database or conn.schema or 'default'
            )
        except Exception as e:
            self.logger.error("Ошибка при подключении к ClickHouse: %s", e)
            raise AirflowException(f"Ошибка подключения к ClickHouse: {e}")

    def get_client(self):
        return self.client


# 2. Класс для выполнения SQL-запросов
class SQLExecutor(ABC):
    @abstractmethod
    def query(self, sql: str, context: dict = None):
        pass

    @abstractmethod
    def command(self, sql: str, context: dict = None):
        pass


# 3. Класс для рендеринга SQL через Jinja
class SQLRenderer:
    @staticmethod
    def render(sql: str, context: dict = None) -> str:
        """
        Рендерит SQL строку с использованием Jinja-шаблонов.
        Если sql — это путь к файлу, загружает и рендерит файл.
        """
        context = context or {}

        # Если sql — путь к файлу
        if sql.strip().endswith('.sql') and os.path.isfile(sql):
            with open(sql, 'r') as file:
                sql_text = file.read()
        else:
            sql_text = sql

        # Jinja шаблон
        try:
            template = Template(sql_text)
            rendered = template.render(**context)
            return rendered
        except Exception as e:
            raise AirflowException(f"Ошибка рендеринга Jinja-шаблона: {e}")


# 4. Реализация работы с ClickHouse (с использованием вышеописанных классов)
class ClickHouseClient(SQLExecutor):
    def __init__(self, conn_id: str = 'click_connect', database: str = None):
        self.logger = logging.getLogger("airflow.task")
        self.connection_manager = ClickHouseConnection(conn_id, database)
        self.client = self.connection_manager.get_client()

    def query(self, sql: str, context: dict = None):
        try:
            rendered_sql = SQLRenderer.render(sql, context)
            self.logger.info("Выполнение ClickHouse query:\n%s", rendered_sql)
            result = self.client.query(rendered_sql)
            return result.result_rows
        except Exception as e:
            self.logger.error("Ошибка выполнения query: %s", e)
            raise AirflowException(f"Ошибка выполнения ClickHouse query: {e}")

    def command(self, sql: str, context: dict = None):
        try:
            rendered_sql = SQLRenderer.render(sql, context)
            self.logger.info("Выполнение ClickHouse command:\n%s", rendered_sql)
            self.client.command(rendered_sql)
            self.logger.info("Команда успешно выполнена.")
        except Exception as e:
            self.logger.error("Ошибка выполнения command: %s", e)
            raise AirflowException(f"Ошибка выполнения ClickHouse command: {e}")

