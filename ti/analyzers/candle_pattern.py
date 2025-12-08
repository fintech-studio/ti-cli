import pandas as pd
import talib
from ti.config.pattern_config import CANDLE_PATTERNS

class CandlePatternDetector:
    """K線形態檢測器 - 負責檢測K線圖中的特定形態"""
    
    @staticmethod
    def detect_patterns(df: pd.DataFrame) -> pd.DataFrame:
        """檢測所有 K 線型態"""

        open_ = df['Open'].values
        high_ = df['High'].values
        low_ = df['Low'].values
        close_ = df['Close'].values
        
        result_df = pd.DataFrame(index=df.index)
        
        for pattern_key, pattern_config in CANDLE_PATTERNS.items():
            if not hasattr(talib, pattern_config.ta_function):
                continue
                
            func = getattr(talib, pattern_config.ta_function)
            
            if pattern_config.needs_penetration:
                result_df[pattern_key] = func(open_, high_, low_, close_, penetration=0)
            else:
                result_df[pattern_key] = func(open_, high_, low_, close_)
        
        return result_df
    
    @staticmethod
    def combine_patterns(row: pd.Series) -> str:
        """將檢測到的型態組合成字串"""              

        signals = []
        
        for pattern_key, pattern_config in CANDLE_PATTERNS.items():
            val = row.get(pattern_key, 0)
            
            if val == 0:
                continue
            
            # 判斷是否有方向性
            if pattern_config.has_direction:
                if val > 0 and pattern_config.bullish_name:
                    signals.append(pattern_config.bullish_name)
                elif val < 0 and pattern_config.bearish_name:
                    signals.append(pattern_config.bearish_name)
                else:
                    signals.append(pattern_config.chinese_name)
            else:
                signals.append(pattern_config.chinese_name)
        
        return ','.join(signals) if signals else ''
    
    @staticmethod
    def detect_and_combine(df: pd.DataFrame) -> pd.Series:
        """檢測型態並組合成字串"""
        pattern_df = CandlePatternDetector.detect_patterns(df)
        return pattern_df.apply(CandlePatternDetector.combine_patterns, axis=1)