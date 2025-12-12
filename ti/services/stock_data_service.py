from ti.providers.stock_data_provider import StockDataProvider
from ti.analyzers.indicator_calc import TechnicalIndicatorCalculator
from ti.analyzers.candle_pattern import CandlePatternDetector
from ti.database.repositories import StockDataRepository
from ti.utils.helpers import get_ticker_with_suffix, get_period_by_interval
import pandas as pd

class StockDataService:
    """股票數據服務"""

    def __init__(self):
        pass
    
    def fetch_and_store(self, symbol: str, market: str, interval: str):
        """獲取並儲存股票數據和技術指標"""
        # 格式化股票代號
        formatted_symbol = get_ticker_with_suffix(symbol, market)
        period = get_period_by_interval(interval)

        # 獲取股票數據
        stock_data = StockDataProvider.get_stock_data(formatted_symbol, period, interval)
        
        # 計算技術指標
        indicators = TechnicalIndicatorCalculator.calculate_all_indicators(stock_data)
        
        # 檢測 K 線型態
        pattern_features = CandlePatternDetector.detect_and_combine(stock_data)
        pattern_features.name = 'pattern_feature'
        
        # 合併所有數據
        combined_data = pd.concat([stock_data, indicators, pattern_features], axis=1)
        
        # 保存數據到資料庫
        repo = StockDataRepository(market)
        saved_count = repo.save_dataframe(combined_data, symbol, interval)
        
        return {
            'symbol': symbol,
            'market': market,
            'interval': interval,
            'data_count': len(stock_data),
            'indicator_count': len(indicators.columns),
            'pattern_count': (pattern_features != '').sum(),
            'saved_count': saved_count
        }
    
    def fetch_and_store_range(self, symbol: str, market: str, interval: str, start_date: str, end_date: str):
        """根據日期範圍獲取並儲存股票數據和技術指標"""
        # 格式化股票代號
        formatted_symbol = get_ticker_with_suffix(symbol, market)

        # 獲取股票數據
        stock_data = StockDataProvider.get_stock_data_range(formatted_symbol, start_date, end_date, interval)
        
        # 計算技術指標
        indicators = TechnicalIndicatorCalculator.calculate_all_indicators(stock_data)
        
        # 檢測 K 線型態
        pattern_features = CandlePatternDetector.detect_and_combine(stock_data)
        pattern_features.name = 'pattern_feature'
        
        # 合併所有數據
        combined_data = pd.concat([stock_data, indicators, pattern_features], axis=1)
        
        # 保存數據到資料庫
        repo = StockDataRepository(market)
        saved_count = repo.save_dataframe(combined_data, symbol, interval)
        
        return {
            'symbol': symbol,
            'market': market,
            'interval': interval,
            'data_count': len(stock_data),
            'indicator_count': len(indicators.columns),
            'pattern_count': (pattern_features != '').sum(),
            'saved_count': saved_count
        }