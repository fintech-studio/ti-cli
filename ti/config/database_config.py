from ti.config.config_manager import ConfigManager

class DatabaseConfig:
    """資料庫配置類 - 提供資料庫配置介面"""

    def __init__(self):
        self._manager = ConfigManager()

    @property
    def server(self):
        """取得資料庫伺服器位址"""
        return self._manager.get("db_server")
    
    @property
    def database(self):
        """取得資料庫名稱"""
        return self._manager.get("db_name")
    
    @property
    def username(self):
        """取得資料庫使用者名稱"""
        return self._manager.get("db_user")
    
    @property
    def password(self):
        """取得資料庫密碼"""
        return self._manager.get("db_password")
    
    @property
    def driver(self):
        """取得資料庫驅動程式"""
        return self._manager.get("db_driver", "ODBC Driver 17 for SQL Server")

    def get_connection_string(self):
        """取得資料庫連線字串"""
        self._manager.reload()
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password}"
        )

    def get_master_connection_string(self):
        """取得連接到 master 資料庫的連線字串"""
        self._manager.reload()
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE=master;"
            f"UID={self.username};"
            f"PWD={self.password}"
        )

    def update_database(self, server=None, database=None, username=None, password=None, driver=None):
        """更新資料庫配置"""
        updates = {}
        if server is not None:
            updates["db_server"] = server
        if database is not None:
            updates["db_name"] = database
        if username is not None:
            updates["db_user"] = username
        if password is not None:
            updates["db_password"] = password
        if driver is not None:
            updates["db_driver"] = driver
        
        self._manager.update(**updates)

    def clear_db_config(self):
        """清除資料庫配置"""
        self._manager.clear_prefix("db_")
        self._manager.set("db_driver", "ODBC Driver 17 for SQL Server")