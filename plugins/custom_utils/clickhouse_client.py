import clickhouse_connect
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import logging


class ClickHouseClient:
    def __init__(self, conn_id: str = 'click_connect', database: str = None):
        self.logger = logging.getLogger("airflow.task")
        self.conn_id = conn_id
        self.database = database
        self.client = self._connect()

    def _connect(self):
        try:
            conn = BaseHook.get_connection(self.conn_id)
            self.logger.info("Подключение к ClickHouse (%s:%s)", conn.host, conn.port or 8123)

            client = clickhouse_connect.get_client(
                host=conn.host,
                port=conn.port or 8123,
                username=conn.login,
                password=conn.password,
                database=self.database or conn.schema or 'default'
            )

            return client

        except Exception as e:
            self.logger.error("Ошибка при подключении к ClickHouse: %s", e)
            raise AirflowException(f"Ошибка подключения к ClickHouse: {e}")

    def query(self, sql: str):
        try:
            self.logger.info("Выполнение запроса ClickHouse: %s", sql)
            result = self.client.query(sql)
            return result.result_rows
        except Exception as e:
            self.logger.error("Ошибка при выполнении запроса: %s", e)
            raise AirflowException(f"Ошибка выполнения ClickHouse query: {e}")

    def command(self, sql: str):
        try:
            self.logger.info("Выполнение команды ClickHouse: %s", sql)
            self.client.command(sql)
            self.logger.info("Команда успешно выполнена.")
        except Exception as e:
            self.logger.error("Ошибка при выполнении команды: %s", e)
            raise AirflowException(f"Ошибка выполнения ClickHouse command: {e}")
