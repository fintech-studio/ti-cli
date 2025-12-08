from ti.providers.stock_data_provider import StockDataProvider
from ti.repositories.stock_data_repository import StockDataRepository
from ti.analyzers.indicator_calc import TechnicalIndicatorCalculator
from ti.analyzers.candle_pattern import CandlePatternDetector

class StockDataService:
    """股票數據服務"""

    def __init__(self):
        self.repository = StockDataRepository()
    
    def _get_ticker_with_suffix(self, ticker: str, market: str):
        """根據市場格式化股票代號"""
        suffix_map = {
            'tw': '.TW',
            'us': '',
            'etf': '',
            'index': '',
            'crypto': '-USD',
            'forex': '=X',
            'futures': '',
        }
        suffix = suffix_map.get(market, '')
        if suffix and not ticker.endswith(suffix):
            return ticker + suffix
        return ticker
    
    def _get_period_by_interval(self, interval):
        """根據時間間隔設定獲取期間"""
        period_map = {
            '1m': '7d', '5m': '7d', '15m': '7d', '30m': '7d',
            '1h': '1mo', '1d': '1y', '1wk': '2y', '1mo': '5y'
        }
        return period_map.get(interval, '1y')
    
    def _get_table_name(self, interval: str):
        """取得資料表名稱"""
        return f'stock_data_{interval}'
    
    def fetch_and_store(self, symbol: str, market: str, interval: str):
        """獲取並儲存股票數據和技術指標"""
        # 格式化股票代號
        formatted_symbol = self._get_ticker_with_suffix(symbol, market)
        period = self._get_period_by_interval(interval)
        table = self._get_table_name(interval)

        # 獲取股票數據
        stock_data = StockDataProvider.get_stock_data(formatted_symbol, period, interval)
        
        # 計算技術指標
        indicators = TechnicalIndicatorCalculator.calculate_all_indicators(stock_data)
        
        # 檢測 K 線型態
        pattern_features = CandlePatternDetector.detect_and_combine(stock_data)
        
        # 保存數據到資料庫
        self.repository.save_stock_data(symbol, stock_data, indicators, pattern_features, table)
        
        return {
            'symbol': symbol,
            'market': market,
            'interval': interval,
            'data_count': len(stock_data),
            'indicator_count': len(indicators.columns),
            'pattern_count': (pattern_features != '').sum()
        }
    
    def fetch_and_store_range(self, symbol: str, market: str, interval: str, start_date: str, end_date: str):
        """根據日期範圍獲取並儲存股票數據和技術指標"""
        # 格式化股票代號
        formatted_symbol = self._get_ticker_with_suffix(symbol, market)
        table = self._get_table_name(interval)

        # 獲取股票數據
        stock_data = StockDataProvider.get_stock_data_range(formatted_symbol, start_date, end_date, interval)
        
        # 計算技術指標
        indicators = TechnicalIndicatorCalculator.calculate_all_indicators(stock_data)
        
        # 檢測 K 線型態
        pattern_features = CandlePatternDetector.detect_and_combine(stock_data)
        
        # 保存數據到資料庫
        self.repository.save_stock_data(symbol, stock_data, indicators, pattern_features, table)
        
        return {
            'symbol': symbol,
            'market': market,
            'interval': interval,
            'data_count': len(stock_data),
            'indicator_count': len(indicators.columns),
            'pattern_count': (pattern_features != '').sum()
        }