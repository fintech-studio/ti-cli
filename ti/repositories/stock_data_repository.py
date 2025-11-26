import pyodbc
import pandas as pd
from ti.config.database_config import DatabaseConfig

class StockDataRepository:
    """股票數據儲存庫類 - 負責與股票數據相關的資料庫操作"""

    def __init__(self):
        config = DatabaseConfig()
        self.conn_str = config.get_connection_string()
        self.conn = pyodbc.connect(self.conn_str)
    
    def save_stock_data(self, symbol, stock_data, indicators, table):
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
                
                if exists:
                    # 更新現有記錄
                    columns = list(combined_data.columns)
                    set_clause = ', '.join([f"[{col}]=?" if col in ['Open', 'Close'] else f"{col}=?" for col in columns])
                    values = [None if pd.isna(val) else val for val in row.values]
                    values.extend([symbol, index])
                    
                    cursor.execute(
                        f"UPDATE {table} SET {set_clause}, lastUpdate=GETDATE() WHERE symbol=? AND datetime=?",
                        *values
                    )
                else:
                    # 插入新記錄
                    columns = ['symbol', 'datetime'] + list(combined_data.columns)
                    column_names = ', '.join([f"[{col}]" if col in ['Open', 'Close'] else col for col in columns])
                    placeholders = ', '.join(['?' for _ in columns])
                    insert_sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
                    
                    values = [symbol, index] + [None if pd.isna(val) else val for val in row.values]
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
                    [Open] FLOAT,
                    High FLOAT,
                    Low FLOAT,
                    [Close] FLOAT,
                    Volume BIGINT,
                    RSI_5 FLOAT, RSI_7 FLOAT, RSI_10 FLOAT, RSI_14 FLOAT, RSI_21 FLOAT,
                    DIF FLOAT, MACD FLOAT, MACD_Histogram FLOAT,
                    RSV FLOAT, K_Value FLOAT, D_Value FLOAT, J_Value FLOAT,
                    MA5 FLOAT, MA10 FLOAT, MA20 FLOAT, MA60 FLOAT,
                    EMA12 FLOAT, EMA26 FLOAT,
                    Bollinger_Upper FLOAT, Bollinger_Middle FLOAT, Bollinger_Lower FLOAT,
                    ATR FLOAT, CCI FLOAT, Williams_R FLOAT, Momentum FLOAT,
                    lastUpdate DATETIME DEFAULT GETDATE(),
                    UNIQUE(symbol, datetime)
                );
            """)
            self.conn.commit()