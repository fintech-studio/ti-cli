from ti.config.database_config import DatabaseConfig

class ConfigService:
    """配置管理服務 - 提供配置的業務邏輯"""
    
    def __init__(self):
        self.db_config = DatabaseConfig()
    
    def show_db_config(self):
        """顯示資料庫配置"""
        return {
            'server': self.db_config.server or 'Not configured',
            'database': self.db_config.database or 'Not configured',
            'username': self.db_config.username or 'Not configured',
            'password': '***' if self.db_config.password else 'Not configured',
            'driver': self.db_config.driver or 'Not configured'
        }
    
    def update_db_config(self, server=None, database=None, username=None, password=None, driver=None):
        """更新資料庫配置"""
        self.db_config.update_database(server, database, username, password, driver)
        return "database configuration updated"
    
    def clear_db_config(self):
        """清除資料庫配置"""
        self.db_config.clear_db_config()
        return "Database configuration cleared"