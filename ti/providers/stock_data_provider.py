import yfinance as yf

class StockDataProvider:
    """股票數據提供者 - 負責提供股票數據"""
    
    @staticmethod
    def get_stock_data(symbol, period, interval):
        """獲取股票數據"""
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period, interval=interval)
        
        return data[['Open', 'High', 'Low', 'Close', 'Volume']]
    
    @staticmethod
    def get_stock_data_range(symbol, start_date, end_date, interval):
        """根據日期範圍獲取股票數據"""
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start_date, end=end_date, interval=interval)
        
        return data[['Open', 'High', 'Low', 'Close', 'Volume']]