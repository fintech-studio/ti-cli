from sqlmodel import SQLModel, Field, inspect
from datetime import datetime as dt
from typing import Optional
from ti.database.connection import engine

class MarketDataBaseModel(SQLModel):
    """數據基礎模型"""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    symbol: str = Field(max_length=20, index=True)
    interval: str = Field(max_length=10, index=True)
    datetime: dt = Field(index=True)
    
    # OHLCV 數據
    open: Optional[float] = Field(default=None, sa_column_kwargs={"name": "open"})
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = Field(default=None, sa_column_kwargs={"name": "close"})
    volume: Optional[int] = None
    
    # RSI 指標
    rsi_5: Optional[float] = None
    rsi_7: Optional[float] = None
    rsi_10: Optional[float] = None
    rsi_14: Optional[float] = None
    rsi_21: Optional[float] = None
    
    # MACD 指標
    dif: Optional[float] = None
    macd: Optional[float] = None
    macd_histogram: Optional[float] = None
    
    # KDJ 指標
    rsv: Optional[float] = None
    k_value: Optional[float] = None
    d_value: Optional[float] = None
    j_value: Optional[float] = None
    
    # MA 指標
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    
    # EMA 指標
    ema12: Optional[float] = None
    ema26: Optional[float] = None
    
    # 布林通道
    bollinger_upper: Optional[float] = None
    bollinger_middle: Optional[float] = None
    bollinger_lower: Optional[float] = None
    
    # 其他指標
    atr: Optional[float] = None
    cci: Optional[float] = None
    williams_r: Optional[float] = None
    momentum: Optional[float] = None
    
    # 型態特徵
    pattern_feature: Optional[str] = Field(default=None, max_length=500)
    
    # 最後更新時間
    last_update: Optional[dt] = None


class StockDataTW(MarketDataBaseModel, table=True):
    """台灣股市數據表"""
    __tablename__ = "stock_data_tw"

class StockDataUS(MarketDataBaseModel, table=True):
    """美國股市數據表"""
    __tablename__ = "stock_data_us"

class Crypto(MarketDataBaseModel, table=True):
    """加密貨幣數據表"""
    
class Index(MarketDataBaseModel, table=True):
    """指數數據表"""
    
class ETF(MarketDataBaseModel, table=True):
    """ETF數據表"""
    
class Forex(MarketDataBaseModel, table=True):
    """外匯數據表"""

class Futures(MarketDataBaseModel, table=True):
    """期貨數據表"""

# 市場映射字典
MARKET_MODELS = {
    "tw": StockDataTW,
    "us": StockDataUS,
    "crypto": Crypto,
    "index": Index,
    "etf": ETF,
    "forex": Forex,
    "futures": Futures,
}


def get_model_by_market(market: str):
    """根據市場代碼獲取對應的模型"""
    model = MARKET_MODELS.get(market.lower())
    if not model:
        raise ValueError(f"不支援的市場: {market}. 支援的市場: {', '.join(MARKET_MODELS.keys())}")
    return model

def create_tables():
    """創建所有資料表"""
    SQLModel.metadata.create_all(engine)

def create_table_by_market(market: str):
    """根據市場創建指定的資料表"""
    model = get_model_by_market(market)
    model.metadata.create_all(engine)

def drop_tables():
    """刪除所有資料表"""
    SQLModel.metadata.drop_all(engine)

def drop_table_by_market(market: str):
    """根據市場刪除指定的資料表"""
    model = get_model_by_market(market)
    model.metadata.drop_all(engine)

def list_tables():
    """列出資料庫中的所有資料表"""
    inspector = inspect(engine)
    return inspector.get_table_names()