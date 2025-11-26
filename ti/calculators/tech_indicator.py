import talib
import pandas as pd

class TechnicalIndicatorCalculator:
    """技術指標計算器 - 負責計算各種技術指標"""
    
    def __init__(self):
        pass
    
    def calculate_all_indicators(self, data:pd.DataFrame) -> pd.DataFrame:
        """計算所有技術指標"""
        
        high = data['High'].values
        low = data['Low'].values
        close = data['Close'].values
        
        indicators = pd.DataFrame(index=data.index)
        
        
        # RSI 指標群組
        indicators['RSI_5'] = talib.RSI(close, timeperiod=5)
        indicators['RSI_7'] = talib.RSI(close, timeperiod=7)
        indicators['RSI_10'] = talib.RSI(close, timeperiod=10)
        indicators['RSI_14'] = talib.RSI(close, timeperiod=14)
        indicators['RSI_21'] = talib.RSI(close, timeperiod=21)
                
        # MACD 指標群組
        macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        indicators['DIF'] = macd
        indicators['MACD'] = macd_signal
        indicators['MACD_Histogram'] = macd_hist
                
        # KDJ 指標群組
        slowk, slowd = talib.STOCH(high, low, close, fastk_period=9, slowk_period=3, slowd_period=3)
        fastk, fastd = talib.STOCHF(high, low, close, fastk_period=9, fastd_period=1, fastd_matype=0)
        indicators['RSV'] = fastk    
        indicators['K_Value'] = slowk
        indicators['D_Value'] = slowd
        indicators['J_Value'] = 3 * slowk - 2 * slowd

        # MA 移動平均群組
        indicators['MA5'] = talib.SMA(close, timeperiod=5)
        indicators['MA10'] = talib.SMA(close, timeperiod=10)
        indicators['MA20'] = talib.SMA(close, timeperiod=20)
        indicators['MA60'] = talib.SMA(close, timeperiod=60)
                
        # EMA 指數移動平均群組
        indicators['EMA12'] = talib.EMA(close, timeperiod=12)
        indicators['EMA26'] = talib.EMA(close, timeperiod=26)
                
        # 布林帶群組
        upper, middle, lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
        indicators['Bollinger_Upper'] = upper
        indicators['Bollinger_Middle'] = middle
        indicators['Bollinger_Lower'] = lower
                
        # 其他技術指標
        indicators['ATR'] = talib.ATR(high, low, close, timeperiod=14)
        indicators['CCI'] = talib.CCI(high, low, close, timeperiod=14)
        indicators['Williams_R'] = talib.WILLR(high, low, close, timeperiod=14)
        indicators['Momentum'] = talib.MOM(close, timeperiod=10)
        
        return indicators