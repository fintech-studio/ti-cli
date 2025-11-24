import os
import json

class ConfigManager:
    """配置檔案管理類 - 負責配置檔案的創建和 I/O 操作"""
    
    _instance = None
    
    def __new__(cls):
        """singleton"""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        config_dir = os.path.join(os.getcwd(), ".ti")
        os.makedirs(config_dir, exist_ok=True)
        self.config_path = os.path.join(config_dir, "config.json")
        self._config_data = self._load_config()
        self._initialized = True
    
    def _load_config(self):
        """載入配置檔案"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}
    
    def _save_config(self):
        """保存配置到 JSON 檔案"""
        # 移除空值
        config_data = {k: v for k, v in self._config_data.items() if v is not None}
        
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)
    
    def reload(self):
        """重新載入配置"""
        self._config_data = self._load_config()
    
    def get(self, key, default=None):
        """取得配置值"""
        return self._config_data.get(key, default)
    
    def set(self, key, value):
        """設定配置值"""
        self._config_data[key] = value
        self._save_config()
    
    def update(self, **kwargs):
        """批量更新配置"""
        for key, value in kwargs.items():
            if value is not None:
                self._config_data[key] = value
        self._save_config()
    
    def delete(self, key):
        """刪除配置項"""
        if key in self._config_data:
            del self._config_data[key]
            self._save_config()
    
    def clear_prefix(self, prefix):
        """清除特定前綴的配置項"""
        keys_to_delete = [k for k in self._config_data.keys() if k.startswith(prefix)]
        for key in keys_to_delete:
            del self._config_data[key]
        self._save_config()