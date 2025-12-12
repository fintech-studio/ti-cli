from sqlmodel import Session, select
from datetime import datetime
from typing import Optional, Type
from ti.database.tables import MarketDataBaseModel, get_model_by_market
from ti.database.connection import engine
import pandas as pd

class StockDataRepository:
    """股票數據儲存庫 - 負責資料的增刪查改"""
    
    def __init__(self, market: str):
        self.market = market.lower()
        self.model: Type[MarketDataBaseModel] = get_model_by_market(self.market)
    
    def save_dataframe(self, df: pd.DataFrame, symbol: str, interval: str) -> int:
        """儲存資料到資料庫"""
        saved_count = 0
        
        with Session(engine) as session:
            for index, row in df.iterrows():
                dt = index if isinstance(index, datetime) else pd.to_datetime(index)
                
                stock_data = self.model(
                    symbol=symbol,
                    interval=interval,
                    datetime=dt,
                    open=float(row.get('Open')) if pd.notna(row.get('Open')) else None,
                    high=float(row.get('High')) if pd.notna(row.get('High')) else None,
                    low=float(row.get('Low')) if pd.notna(row.get('Low')) else None,
                    close=float(row.get('Close')) if pd.notna(row.get('Close')) else None,
                    volume=int(row.get('Volume')) if pd.notna(row.get('Volume')) else None,
                    rsi_5=float(row.get('RSI_5')) if pd.notna(row.get('RSI_5')) else None,
                    rsi_7=float(row.get('RSI_7')) if pd.notna(row.get('RSI_7')) else None,
                    rsi_10=float(row.get('RSI_10')) if pd.notna(row.get('RSI_10')) else None,
                    rsi_14=float(row.get('RSI_14')) if pd.notna(row.get('RSI_14')) else None,
                    rsi_21=float(row.get('RSI_21')) if pd.notna(row.get('RSI_21')) else None,
                    dif=float(row.get('DIF')) if pd.notna(row.get('DIF')) else None,
                    macd=float(row.get('MACD')) if pd.notna(row.get('MACD')) else None,
                    macd_histogram=float(row.get('MACD_Histogram')) if pd.notna(row.get('MACD_Histogram')) else None,
                    rsv=float(row.get('RSV')) if pd.notna(row.get('RSV')) else None,
                    k_value=float(row.get('K_Value')) if pd.notna(row.get('K_Value')) else None,
                    d_value=float(row.get('D_Value')) if pd.notna(row.get('D_Value')) else None,
                    j_value=float(row.get('J_Value')) if pd.notna(row.get('J_Value')) else None,
                    ma5=float(row.get('MA5')) if pd.notna(row.get('MA5')) else None,
                    ma10=float(row.get('MA10')) if pd.notna(row.get('MA10')) else None,
                    ma20=float(row.get('MA20')) if pd.notna(row.get('MA20')) else None,
                    ma60=float(row.get('MA60')) if pd.notna(row.get('MA60')) else None,
                    ema12=float(row.get('EMA12')) if pd.notna(row.get('EMA12')) else None,
                    ema26=float(row.get('EMA26')) if pd.notna(row.get('EMA26')) else None,
                    bollinger_upper=float(row.get('Bollinger_Upper')) if pd.notna(row.get('Bollinger_Upper')) else None,
                    bollinger_middle=float(row.get('Bollinger_Middle')) if pd.notna(row.get('Bollinger_Middle')) else None,
                    bollinger_lower=float(row.get('Bollinger_Lower')) if pd.notna(row.get('Bollinger_Lower')) else None,
                    atr=float(row.get('ATR')) if pd.notna(row.get('ATR')) else None,
                    cci=float(row.get('CCI')) if pd.notna(row.get('CCI')) else None,
                    williams_r=float(row.get('Williams_R')) if pd.notna(row.get('Williams_R')) else None,
                    momentum=float(row.get('Momentum')) if pd.notna(row.get('Momentum')) else None,
                    pattern_feature=str(row.get('pattern_feature', '')) if pd.notna(row.get('pattern_feature')) else None
                )
                
                self.upsert(session, stock_data)
                saved_count += 1
        
        return saved_count
    
    def create(self, session: Session, stock_data: MarketDataBaseModel) -> MarketDataBaseModel:
        """新增單筆股票數據"""
        stock_data.last_update = datetime.now()
        session.add(stock_data)
        session.commit()
        session.refresh(stock_data)
        return stock_data
    
    def get_by_symbol_datetime(self, session: Session, symbol: str, dt: datetime, interval: str) -> Optional[MarketDataBaseModel]:
        """根據股票代碼、時間和間隔查詢"""
        statement = select(self.model).where(
            self.model.symbol == symbol,
            self.model.datetime == dt,
            self.model.interval == interval
        )
        return session.exec(statement).first()
    
    def upsert(self, session: Session, stock_data: MarketDataBaseModel) -> MarketDataBaseModel:
        """新增或更新股票數據"""
        existing = self.get_by_symbol_datetime(
            session,
            stock_data.symbol, 
            stock_data.datetime,
            stock_data.interval
        )
        
        if existing:
            # 更新現有記錄
            for key, value in stock_data.dict(exclude={'id'}).items():
                if value is not None:
                    setattr(existing, key, value)
            existing.last_update = datetime.now()
            session.commit()
            session.refresh(existing)
            return existing
        else:
            # 新增記錄
            return self.create(session, stock_data)