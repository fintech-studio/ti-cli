import pyodbc
import pandas as pd
from ti.config.database_config import DatabaseConfig

class StockDataRepository:
    """股票數據儲存庫類 - 負責與股票數據相關的資料庫操作"""

    def __init__(self):
        config = DatabaseConfig()
        self.conn_str = config.get_connection_string()
        self.conn = pyodbc.connect(self.conn_str)
    
    def save_stock_data(self, symbol, stock_data, indicators, pattern_features, table):
        """保存股票數據和技術指標"""
        self._ensure_table(table)
        
        # 合併股票數據和技術指標
        combined_data = pd.concat([stock_data, indicators], axis=1)
        
        with self.conn:
            cursor = self.conn.cursor()
            
            for index, row in combined_data.iterrows():
                # 檢查記錄是否已存在
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE symbol = ? AND datetime = ?", 
                              (symbol, index))
                exists = cursor.fetchone()[0] > 0
                
                # 取得對應的型態特徵
                pattern_feature = pattern_features.loc[index] if index in pattern_features.index else ''
                
                if exists:
                    # 更新現有記錄
                    columns = list(combined_data.columns)
                    set_clause = ', '.join([f"[{col}]=?" if col in ['Open', 'Close'] else f"{col}=?" for col in columns])
                    set_clause += ', pattern_feature=?'
                    values = [None if pd.isna(val) else val for val in row.values]
                    values.extend([pattern_feature, symbol, index])
                    
                    cursor.execute(
                        f"UPDATE {table} SET {set_clause}, lastUpdate=GETDATE() WHERE symbol=? AND datetime=?",
                        *values
                    )
                else:
                    # 插入新記錄
                    columns = ['symbol', 'datetime'] + list(combined_data.columns) + ['pattern_feature']
                    column_names = ', '.join([f"[{col}]" if col in ['Open', 'Close'] else col for col in columns])
                    placeholders = ', '.join(['?' for _ in columns])
                    insert_sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
                    
                    values = [symbol, index] + [None if pd.isna(val) else val for val in row.values] + [pattern_feature]
                    cursor.execute(insert_sql, values)
            
            self.conn.commit()
    
    def _ensure_table(self, table: str):
        """確保股票數據表存在"""
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table}' AND xtype='U')
                CREATE TABLE {table} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    symbol NVARCHAR(20) NOT NULL,
                    datetime DATETIME NOT NULL,
                    [Open] DECIMAL(18,4),
                    High DECIMAL(18,4),
                    Low DECIMAL(18,4),
                    [Close] DECIMAL(18,4),
                    Volume BIGINT,
                    RSI_5 DECIMAL(18,4), RSI_7 DECIMAL(18,4), RSI_10 DECIMAL(18,4), RSI_14 DECIMAL(18,4), RSI_21 DECIMAL(18,4),
                    DIF DECIMAL(18,4), MACD DECIMAL(18,4), MACD_Histogram DECIMAL(18,4),
                    RSV DECIMAL(18,4), K_Value DECIMAL(18,4), D_Value DECIMAL(18,4), J_Value DECIMAL(18,4),
                    MA5 DECIMAL(18,4), MA10 DECIMAL(18,4), MA20 DECIMAL(18,4), MA60 DECIMAL(18,4),
                    EMA12 DECIMAL(18,4), EMA26 DECIMAL(18,4),
                    Bollinger_Upper DECIMAL(18,4), Bollinger_Middle DECIMAL(18,4), Bollinger_Lower DECIMAL(18,4),
                    ATR DECIMAL(18,4), CCI DECIMAL(18,4), Williams_R DECIMAL(18,4), Momentum DECIMAL(18,4),
                    pattern_feature NVARCHAR(500),
                    lastUpdate DATETIME DEFAULT GETDATE(),
                    UNIQUE(symbol, datetime)
                );
            """)
            self.conn.commit()