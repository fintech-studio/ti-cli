from typing import Dict, Optional
from enum import Enum
from dataclasses import dataclass

class PatternType(Enum):
    REVERSAL = "反轉型態"
    CONTINUATION = "持續型態"
    NEUTRAL = "中性型態"

@dataclass
class CandlePattern:
    ta_function: str
    chinese_name: str
    pattern_type: PatternType
    needs_penetration: bool = False
    has_direction: bool = False
    bullish_name: Optional[str] = None
    bearish_name: Optional[str] = None
    description: Optional[str] = None

# K線型態函數與中文註解對應表
CANDLE_PATTERNS: Dict[str, CandlePattern] = {
    # 反轉型態
    'CDLHAMMER': CandlePattern(
        'CDLHAMMER', '錘頭', PatternType.REVERSAL,
        description="底部反轉訊號，長下影線，小實體"
    ),
    'CDLHANGINGMAN': CandlePattern(
        'CDLHANGINGMAN', '上吊線', PatternType.REVERSAL,
        description="頂部反轉訊號，長下影線，小實體"
    ),
    'CDLINVERTEDHAMMER': CandlePattern(
        'CDLINVERTEDHAMMER', '倒錘頭', PatternType.REVERSAL,
        description="底部反轉訊號，長上影線，小實體"
    ),
    'CDLSHOOTINGSTAR': CandlePattern(
        'CDLSHOOTINGSTAR', '射擊星', PatternType.REVERSAL,
        description="頂部反轉訊號，長上影線，小實體"
    ),
    
    # 吞沒型態
    'CDLENGULFING': CandlePattern(
        'CDLENGULFING', '吞沒形態', PatternType.REVERSAL,
        has_direction=True, bullish_name='多頭吞沒', bearish_name='空頭吞沒',
        description="第二根K線完全包含第一根K線"
    ),
    
    # 晨星/暮星系列
    'CDLMORNINGSTAR': CandlePattern(
        'CDLMORNINGSTAR', '晨星', PatternType.REVERSAL,
        needs_penetration=True, description="三K線底部反轉型態"
    ),
    'CDLEVENINGSTAR': CandlePattern(
        'CDLEVENINGSTAR', '暮星', PatternType.REVERSAL,
        needs_penetration=True, description="三K線頂部反轉型態"
    ),
    'CDLMORNINGDOJISTAR': CandlePattern(
        'CDLMORNINGDOJISTAR', '十字晨星', PatternType.REVERSAL,
        needs_penetration=True, description="中間為十字線的晨星型態"
    ),
    'CDLEVENINGDOJISTAR': CandlePattern(
        'CDLEVENINGDOJISTAR', '十字暮星', PatternType.REVERSAL,
        needs_penetration=True, description="中間為十字線的暮星型態"
    ),
    
    # 十字線系列
    'CDLDOJI': CandlePattern(
        'CDLDOJI', '十字', PatternType.NEUTRAL,
        description="開盤價等於收盤價，市場猶豫"
    ),
    'CDLDOJISTAR': CandlePattern(
        'CDLDOJISTAR', '十字星', PatternType.REVERSAL,
        description="跳空的十字線"
    ),
    'CDLDRAGONFLYDOJI': CandlePattern(
        'CDLDRAGONFLYDOJI', '蜻蜓十字', PatternType.REVERSAL,
        description="只有下影線的十字線"
    ),
    'CDLGRAVESTONEDOJI': CandlePattern(
        'CDLGRAVESTONEDOJI', '墓碑十字', PatternType.REVERSAL,
        description="只有上影線的十字線"
    ),
    'CDLLONGLEGGEDDOJI': CandlePattern(
        'CDLLONGLEGGEDDOJI', '長腿十字', PatternType.NEUTRAL,
        description="上下都有長影線的十字線"
    ),
    
    # 三兵系列
    'CDL3WHITESOLDIERS': CandlePattern(
        'CDL3WHITESOLDIERS', '三白兵', PatternType.CONTINUATION,
        description="三根連續上漲的陽線"
    ),
    'CDL3BLACKCROWS': CandlePattern(
        'CDL3BLACKCROWS', '三隻烏鴉', PatternType.CONTINUATION,
        description="三根連續下跌的陰線"
    ),
    'CDLIDENTICAL3CROWS': CandlePattern(
        'CDLIDENTICAL3CROWS', '三胞胎烏鴉', PatternType.CONTINUATION,
        description="三根相似的連續下跌陰線"
    ),
    'CDL2CROWS': CandlePattern(
        'CDL2CROWS', '兩隻烏鴉', PatternType.REVERSAL,
        description="兩根下跌陰線，頂部反轉訊號"
    ),
    
    # 內外包型態
    'CDL3INSIDE': CandlePattern(
        'CDL3INSIDE', '三內部漲跌', PatternType.REVERSAL,
        has_direction=True, bullish_name='三內部上漲', bearish_name='三內部下跌',
        description="三K線內包型態"
    ),
    'CDL3OUTSIDE': CandlePattern(
        'CDL3OUTSIDE', '三外部漲跌', PatternType.REVERSAL,
        has_direction=True, bullish_name='三外部上漲', bearish_name='三外部下跌',
        description="三K線外包型態"
    ),
    
    # 孕線系列
    'CDLHARAMI': CandlePattern(
        'CDLHARAMI', '孕線', PatternType.REVERSAL,
        description="第二根K線被第一根完全包含"
    ),
    'CDLHARAMICROSS': CandlePattern(
        'CDLHARAMICROSS', '十字孕線', PatternType.REVERSAL,
        description="第二根為十字線的孕線型態"
    ),
    
    # 光頭光腳系列
    'CDLMARUBOZU': CandlePattern(
        'CDLMARUBOZU', '光頭光腳', PatternType.CONTINUATION,
        has_direction=True, bullish_name='上漲光頭光腳', bearish_name='下跌光頭光腳',
        description="沒有上下影線的K線"
    ),
    'CDLCLOSINGMARUBOZU': CandlePattern(
        'CDLCLOSINGMARUBOZU', '光頭光腳(單頭腳判定)', PatternType.CONTINUATION,
        has_direction=True, bullish_name='光頭', bearish_name='光腳',
        description="只有一端沒有影線"
    ),
    
    # 打擊系列
    'CDL3LINESTRIKE': CandlePattern(
        'CDL3LINESTRIKE', '三線打擊', PatternType.REVERSAL,
        has_direction=True, bullish_name='三線打擊上漲', bearish_name='三線打擊下跌',
        description="三根同向K線後的反轉型態"
    ),
    'CDLCOUNTERATTACK': CandlePattern(
        'CDLCOUNTERATTACK', '反擊線', PatternType.REVERSAL,
        description="收盤價相同的反向K線組合"
    ),
    
    # 三法系列
    'CDLRISEFALL3METHODS': CandlePattern(
        'CDLRISEFALL3METHODS', '上升/下降三法', PatternType.CONTINUATION,
        has_direction=True, bullish_name='上升三法', bearish_name='下降三法',
        description="趨勢中的整理型態"
    ),
    'CDLXSIDEGAP3METHODS': CandlePattern(
        'CDLXSIDEGAP3METHODS', '跳空三法', PatternType.CONTINUATION,
        has_direction=True, bullish_name='上升跳空三法', bearish_name='下降跳空三法',
        description="跳空後的整理型態"
    ),
    
    # 穿透型態
    'CDLPIERCING': CandlePattern(
        'CDLPIERCING', '刺穿形態', PatternType.REVERSAL,
        description="陽線向上穿透前一根陰線的一半以上"
    ),
    'CDLDARKCLOUDCOVER': CandlePattern(
        'CDLDARKCLOUDCOVER', '烏雲蓋頂', PatternType.REVERSAL,
        needs_penetration=True, description="陰線向下穿透前一根陽線的一半以上"
    ),
    
    # 缺口型態
    'CDLGAPSIDESIDEWHITE': CandlePattern(
        'CDLGAPSIDESIDEWHITE', '缺口上漲', PatternType.CONTINUATION,
        description="向上跳空的兩根陽線"
    ),
    'CDLUPSIDEGAP2CROWS': CandlePattern(
        'CDLUPSIDEGAP2CROWS', '向上跳空兩隻烏鴉', PatternType.REVERSAL,
        description="向上跳空後的兩根陰線"
    ),
    'CDLTASUKIGAP': CandlePattern(
        'CDLTASUKIGAP', '跳空並列(月缺)', PatternType.CONTINUATION,
        description="跳空後的並列K線"
    ),
    
    # 特殊型態
    'CDLABANDONEDBABY': CandlePattern(
        'CDLABANDONEDBABY', '棄嬰', PatternType.REVERSAL,
        has_direction=True, bullish_name='棄嬰上漲', bearish_name='棄嬰下跌',
        description="中間跳空的三K線反轉型態"
    ),
    'CDLBELTHOLD': CandlePattern(
        'CDLBELTHOLD', '捉腰帶線', PatternType.REVERSAL,
        description="長實體，一端沒有影線"
    ),
    'CDLBREAKAWAY': CandlePattern(
        'CDLBREAKAWAY', '脫離', PatternType.REVERSAL,
        description="五K線的突破型態"
    ),
    'CDLKICKING': CandlePattern(
        'CDLKICKING', '反沖形態', PatternType.REVERSAL,
        description="兩根跳空的光頭光腳K線"
    ),
    'CDLKICKINGBYLENGTH': CandlePattern(
        'CDLKICKINGBYLENGTH', '反沖-長短判斷', PatternType.REVERSAL,
        description="根據K線長度判斷的反沖型態"
    ),
    
    # 其他型態
    'CDLADVANCEBLOCK': CandlePattern(
        'CDLADVANCEBLOCK', '大敵當前', PatternType.REVERSAL,
        description="三根逐漸縮小的陽線"
    ),
    'CDLCONCEALBABYSWALL': CandlePattern(
        'CDLCONCEALBABYSWALL', '藏嬰吞沒', PatternType.REVERSAL,
        description="特殊的吞沒型態"
    ),
    'CDL3STARSINSOUTH': CandlePattern(
        'CDL3STARSINSOUTH', '南方三星', PatternType.REVERSAL,
        description="三K線的底部反轉型態"
    ),
    'CDLHIKKAKE': CandlePattern(
        'CDLHIKKAKE', 'Hikkake 陷阱', PatternType.REVERSAL,
        description="日本的陷阱型態"
    ),
    'CDLHIKKAKEMOD': CandlePattern(
        'CDLHIKKAKEMOD', 'Hikkake Modified', PatternType.REVERSAL,
        description="修正版的Hikkake型態"
    ),
    'CDLHOMINGPIGEON': CandlePattern(
        'CDLHOMINGPIGEON', '家鴿', PatternType.REVERSAL,
        description="特殊的孕線變化型態"
    ),
    'CDLINNECK': CandlePattern(
        'CDLINNECK', '頸內線', PatternType.CONTINUATION,
        description="頸線內的持續型態"
    ),
    'CDLONNECK': CandlePattern(
        'CDLONNECK', '頸上線', PatternType.CONTINUATION,
        description="頸線上的持續型態"
    ),
    'CDLLADDERBOTTOM': CandlePattern(
        'CDLLADDERBOTTOM', '梯形底部', PatternType.REVERSAL,
        description="階梯式的底部型態"
    ),
    'CDLMATCHINGLOW': CandlePattern(
        'CDLMATCHINGLOW', '匹配低點', PatternType.REVERSAL,
        description="相同低點的K線組合"
    ),
    'CDLMATHOLD': CandlePattern(
        'CDLMATHOLD', '鋪墊', PatternType.CONTINUATION,
        needs_penetration=True, description="趨勢中的整理型態"
    ),
    'CDLRICKSHAWMAN': CandlePattern(
        'CDLRICKSHAWMAN', '黃包車夫', PatternType.NEUTRAL,
        description="長上下影線的小實體K線"
    ),
    'CDLSEPARATINGLINES': CandlePattern(
        'CDLSEPARATINGLINES', '分離線', PatternType.CONTINUATION,
        description="相同開盤價的反向K線"
    ),
    'CDLSTALLEDPATTERN': CandlePattern(
        'CDLSTALLEDPATTERN', '停滯形態', PatternType.REVERSAL,
        description="上升趨勢中的停滯訊號"
    ),
    'CDLSTICKSANDWICH': CandlePattern(
        'CDLSTICKSANDWICH', '三明治', PatternType.REVERSAL,
        description="三K線的夾心型態"
    ),
    'CDLTAKURI': CandlePattern(
        'CDLTAKURI', '探水竿', PatternType.REVERSAL,
        description="長下影線的底部反轉訊號"
    ),
    'CDLTHRUSTING': CandlePattern(
        'CDLTHRUSTING', '向上突破', PatternType.CONTINUATION,
        description="向上突破的持續型態"
    ),
    'CDLTRISTAR': CandlePattern(
        'CDLTRISTAR', '三星', PatternType.REVERSAL,
        description="三根十字線組成的反轉型態"
    ),
    'CDLUNIQUE3RIVER': CandlePattern(
        'CDLUNIQUE3RIVER', '奇特三河床', PatternType.REVERSAL,
        description="特殊的三K線底部型態"
    ),
    'CDLHIGHWAVE': CandlePattern(
        'CDLHIGHWAVE', '風高浪大線', PatternType.NEUTRAL,
        description="長上下影線，市場不確定"
    ),
    'CDLLONGLINE': CandlePattern(
        'CDLLONGLINE', '長蠟燭', PatternType.CONTINUATION,
        description="長實體的K線"
    ),
    'CDLSHORTLINE': CandlePattern(
        'CDLSHORTLINE', '短蠟燭', PatternType.NEUTRAL,
        description="短實體的K線"
    ),
    'CDLSPINNINGTOP': CandlePattern(
        'CDLSPINNINGTOP', '紡錘線', PatternType.NEUTRAL,
        description="小實體，長上下影線"
    ),
}