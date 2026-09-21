import os
import logging
import oracledb
from typing import Optional, Tuple, List
from airflow.providers.oracle.hooks.oracle import OracleHook


logger = logging.getLogger(__name__)    

# ==============================================================================
# 1. МЕНЕДЖЕР ИНИЦИАЛИЗАЦИИ 
# ==============================================================================

class OracleThickModeManager:
    """
    Singleton-менеджер для инициализации Oracle Thick mode.
    Гарантирует, что init_oracle_client() будет вызван только один раз.
    """

    _instance: Optional["OracleThickModeManager"] = None
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, lib_dir: Optional[str] = None):
        # Предотвращаем повторную инициализацию атрибутов
        if hasattr(self, "_setup_done"):
            return
        self.lib_dir = lib_dir or os.environ.get(
            "ORACLE_CLIENT_DIR", "/opt/oracle/instantclient_19_32"
        )
        self._setup_done = True

    @property
    def is_initialized(self) -> bool:
        """Проверяет, активен ли Thick mode."""
        if self._initialized:
            return True
        try:
            oracledb.clientversion()
            OracleThickModeManager._initialized = True
            return True
        except oracledb.ProgrammingError:
            return False

    def get_client_version(self) -> Optional[Tuple[int, ...]]:
        """Возвращает версию Oracle Client."""
        if self.is_initialized:
            return oracledb.clientversion()
        return None

    def ensure(self) -> None:
        """Инициализирует Thick mode, если еще не инициализирован."""
        if self.is_initialized:
            version = self.get_client_version()
            logger.debug(f"✅ Oracle Client уже загружен (версия {version})")
            return

        if not os.path.isdir(self.lib_dir):
            raise RuntimeError(
                f"Папка Oracle Client не найдена: {self.lib_dir}"
            )

        try:
            logger.info(f"🔧 Инициализация Thick mode из: {self.lib_dir}")
            oracledb.init_oracle_client(lib_dir=self.lib_dir)
            OracleThickModeManager._initialized = True
            logger.info("✅ Thick mode активирован.")
        except oracledb.ProgrammingError as e:
            if "DPY-2021" not in str(e):
                raise
            raise RuntimeError(f"Не удалось инициализировать Thick mode: {e}")
        except oracledb.InterfaceError as e:
            raise RuntimeError(f"Oracle Client не поддерживается: {e}")

    @classmethod
    def reset(cls) -> None:
        """Сбрасывает состояние (только для тестов!)."""
        cls._initialized = False
        cls._instance = None


# ==============================================================================
# 2. НОВЫЙ КЛАСС ДЛЯ ЗАПИСИ ДАННЫХ (Использует менеджер выше)
# ==============================================================================
class OracleDataWriter:
    """
    Класс для записи данных в Oracle.
    Отвечает за валидацию данных и вызов Airflow OracleHook.
    """

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    def _validate_inputs(self, table: str, rows: List[Tuple], target_fields: List[str]) -> None:
            """
            Строгая валидация для предотвращения ORA-00936 и других SQL-ошибок.
            """
            if not isinstance(table, str) or not table.strip():
                raise ValueError("Имя таблицы (table) должно быть непустой строкой.")
                
            if not isinstance(target_fields, list) or len(target_fields) == 0:
                raise ValueError("target_fields должен быть непустым списком строк.")
                
            for i, field in enumerate(target_fields):
                if not isinstance(field, str) or not field.strip():
                    raise ValueError(f"Имя поля с индексом {i} в target_fields должно быть непустой строкой. Получено: {repr(field)}")
                    
            if not isinstance(rows, list):
                raise TypeError(f"Ожидался список (list) для rows, получено: {type(rows).__name__}")
                
            expected_len = len(target_fields)
            
            for i, row in enumerate(rows):
                if not isinstance(row, tuple):
                    raise TypeError(
                        f"Все элементы списка rows должны быть кортежами (tuple). "
                        f"Элемент с индексом {i} имеет тип {type(row).__name__}: {repr(row)}"
                    )
                if len(row) != expected_len:
                    raise ValueError(
                        f"Длина кортежа в строке {i} ({len(row)}) не совпадает с количеством "
                        f"полей target_fields ({expected_len}). Данные: {repr(row)}"
                    )

    def insert_multiple_rows(
            self, 
            table: str, 
            rows: List[Tuple], 
            target_fields: List[str], 
            commit_every: int = 1000
        ) -> int:
            """
            Вставляет список кортежей в таблицу Oracle.
            """
            # 1. Гарантируем, что Thick mode активирован
            OracleThickModeManager().ensure()

            # 2. Строгая валидация данных (защита от ORA-00936)
            self._validate_inputs(table, rows, target_fields)

            if not rows:
                logger.warning("Список rows пуст. Вставка не выполняется.")
                return 0

            # 3. Выполнение вставки через Airflow Hook
            hook = OracleHook(oracle_conn_id=self.conn_id)
            
            try:
                logger.info(f"🚀 Начало вставки {len(rows)} записей в таблицу {table}")

                logger.info(f'Парсинг вернул первую строку {rows[0]}, количество строк: {len(rows)}')
                hook.insert_rows(
                    table=table,
                    rows=rows,
                    target_fields=target_fields,
                    commit_every=commit_every
                )
                
                logger.info(f"✅ Успешно вставлено {len(rows)} записей в {table}")
                return len(rows)
                
            except Exception as e:
                logger.error(f"❌ Ошибка пакетной вставки в {table}: {e}")
                raise


    def execute_sql_from_file(self, sql_filename: str, sql_dir: str = None) -> None:
        """
        Читает SQL из файла и выполняет его.
        
        Args:
            sql_filename: Имя файла (например, 'merge_data.sql').
            sql_dir: Путь к папке с SQL. Если None, ищет в папке 'sql' рядом с файлом вызова.
        """
        # 1. Гарантируем работу драйвера
        OracleThickModeManager().ensure()

        # 2. Определяем путь к файлу
        if not sql_dir:
            import inspect
            caller_frame = inspect.stack()[1]
            caller_path = caller_frame.filename
            caller_dir = os.path.dirname(caller_path)
            sql_dir = os.path.join(caller_dir, "sql")
            
        file_path = os.path.join(sql_dir, sql_filename)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"SQL файл не найден: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()

        logger.info(f"📄 Выполнение SQL из файла: {sql_filename}")

        # 3. Выполняем через Hook
        hook = OracleHook(oracle_conn_id=self.conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql_query)
            conn.commit()
            logger.info(f"✅ SQL выполнен успешно: {sql_filename}")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Ошибка выполнения SQL: {e}")
            raise
        finally:
            cursor.close()