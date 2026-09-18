from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
import logging

class DQCheckOperator(BaseOperator):
    """
    Кастомный оператор для проверки качества данных с записью ошибок в DQ-таблицу.
    
    :param sql: SQL-запрос для проверки (должен возвращать False при ошибке).
    :param dq_table: Таблица для записи ошибок (например, "monitoring.dq_errors").
    :param error_message: Описание ошибки для логов.
    :param mode: Режим работы ("WARN" или "FAIL").
    :param conn_id: Connection ID для БД.
    """
    
    def __init__(
        self,
        sql: str,
        dq_table: str,
        error_message: str,
        mode: str = "WARN",
        conn_id: str = "postgres_conn",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.dq_table = dq_table
        self.error_message = error_message
        self.mode = mode.upper()
        self.conn_id = conn_id

    def execute(self, context):
        # Проверяем данные через SQLCheckOperator
        check = SQLCheckOperator(
            task_id=self.task_id + "_check",
            sql=self.sql,
            conn_id=self.conn_id,
            dag=self.dag
        )
        
        try:
            check.execute(context)
            logging.info("DQ check passed")
        except Exception as e:
            self._handle_dq_failure(context, str(e))

    def _handle_dq_failure(self, context, error_details):
        """Обработка проваленной проверки."""
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        exec_date = context["execution_date"]
        
        # Запись ошибки в DQ-таблицу
        self._log_to_dq_table(
            dag_id=dag_id,
            task_id=task_id,
            exec_date=exec_date,
            error_message=self.error_message,
            error_details=error_details
        )
        
        # Реакция в зависимости от режима
        if self.mode == "FAIL":
            raise AirflowFailException(f"DQ check failed: {self.error_message}")
        else:
            logging.warning(f"DQ check warning: {self.error_message}")

    def _log_to_dq_table(self, dag_id, task_id, exec_date, error_message, error_details):
        """Запись ошибки в таблицу мониторинга."""
        sql = f"""
        INSERT INTO {self.dq_table} (
            dag_id, task_id, execution_date, error_message, error_details, timestamp
        ) VALUES (
            '{dag_id}', '{task_id}', '{exec_date}', '{error_message}', '{error_details}', NOW()
        )
        """
        hook = BaseHook.get_hook(self.conn_id)
        hook.run(sql)

    