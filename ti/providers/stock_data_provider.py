import yfinance as yf

class StockDataProvider:
    """股票數據提供者 - 負責提供股票數據的邏輯"""
    
    def __init__(self):
        pass
    
    def get_stock_data(self, symbol, period, interval):
        """獲取股票數據"""
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period, interval=interval)
        
        return data[['Open', 'High', 'Low', 'Close', 'Volume']]