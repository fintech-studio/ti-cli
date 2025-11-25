from ti.providers.stock_data_provider import StockDataProvider
from ti.repositories.stock_data_repository import StockDataRepository
from ti.calculators.technical_indicators import TechnicalIndicatorsCalculator
class StockDataService:
    """股票數據服務類"""

    def __init__(self):
        self.provider = StockDataProvider()
        self.repository = StockDataRepository()
        self.indicator_calculator = TechnicalIndicatorsCalculator()