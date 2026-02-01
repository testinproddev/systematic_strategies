from abc import ABC, abstractmethod
import pandas as pd
from config import cfg

class BaseStrategy(ABC):
    """
    Abstract base class for all strategies.
    """
    def __init__(self, coin_data: dict, config=cfg):
        """
        Parameters:
            coin_data (dict): {symbol: DataFrame} of OHLCV data
            config (Config): frozen config instance
        """
        self.coin_data = coin_data
        self.config = config

    @abstractmethod
    def generate_signals(self) -> dict:
        """
        Returns:
            dict[symbol -> pd.Series]
        """
        pass


