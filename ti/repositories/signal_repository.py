import pyodbc
from ti.config.database_config import DatabaseConfig

class SignalRepository:
    """交易信號儲存庫 - 負責與交易信號相關的資料庫操作"""

    def __init__(self):
        config = DatabaseConfig()
        self.conn_str = config.get_connection_string()
        self.conn = pyodbc.connect(self.conn_str)