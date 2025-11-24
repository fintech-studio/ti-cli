import pyodbc
from ti.config.database_config import DatabaseConfig

class DatabaseService:
    """資料庫管理服務"""
    
    def __init__(self):
        self.config = DatabaseConfig()

    def create_database_if_not_exists(self, database_name):
        try:
            master_conn_str = self.config.get_master_connection_string()
            with pyodbc.connect(master_conn_str, autocommit=True) as conn:
                cursor = conn.cursor()
                cursor.execute(f"IF DB_ID(N'{database_name}') IS NULL CREATE DATABASE [{database_name}]")
                conn.commit()
                return True, f"Database '{database_name}' ensured to exist."
        except Exception as e:
            return False, f"Failed to create database '{database_name}': {str(e)}"
        
    def test_connection(self):
        """測試資料庫連線"""
        try:
            conn_str = self.config.get_connection_string()
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]
                conn.commit()
                return True, f"connection successful!\nSQL Server version: {version}"
        except Exception as e:
            return False, f"connection failed: {str(e)}"
    
    def list_tables(self):
        """列出資料庫中的所有資料表"""
        try:
            conn_str = self.config.get_connection_string()
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """)
                tables = [row[0] for row in cursor.fetchall()]
                conn.commit()
                return True, tables
        except Exception as e:
            return False, str(e)
    
    def get_table_info(self, table_name):
        """取得資料表詳細資訊"""
        try:
            conn_str = self.config.get_connection_string()
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                
                cursor.execute(f"""
                    SELECT MAX(lastUpdate) FROM {table_name}
                    WHERE lastUpdate IS NOT NULL
                """)
                last_update = cursor.fetchone()[0]
                
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                """)
                columns = cursor.fetchall()
                conn.commit()
                return True, {
                    'count': count,
                    'last_update': last_update,
                    'columns': columns
                }
        except Exception as e:
            return False, str(e)