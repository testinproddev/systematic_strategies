import pandas as pd
import numpy as np
from strategies.base import BaseStrategy

class BreakoutStrategy(BaseStrategy):

    def __init__(
        self,
        short_window: int,
        long_window: int,
        config=None,
    ):
        super().__init__(config)
        self.short_window = short_window
        self.long_window = long_window
        self.signals = {}

    @property
    def name(self):
        return f"breakout_{self.short_window}_{self.long_window}"

    def generate_signals(self,coin_data_for_sim) -> dict:
        """
        Public method to generate signals for all coins.
        Returns:
            dict: {symbol: pd.Series} signals aligned with coin_data_for_sim index
        """
        self.signals = self._generate_breakout_signals(coin_data_for_sim)
        return self.signals

    def _generate_breakout_signals(self,coin_data_for_sim:dict) -> dict:
        """
        Private method doing the actual breakout calculation.
        Returns:
            dict: {symbol: pd.Series} signals aligned with coin_data_for_sim index
        """

        breakout_signals_dict = {}
       
        for sym, df in coin_data_for_sim.items():
            if df.empty:
                breakout_signals_dict[sym] = pd.Series(
                    dtype=float, name="breakout_signal"
                )
                continue

            if "high" not in df.columns:
                raise KeyError(f"'high' column missing for {sym}")

            highs_short = df['high'].rolling(self.short_window, min_periods=1).max()
            highs_long = df['high'].rolling(self.long_window, min_periods=1).max()
            # breakout_signals_dict[sym][f"signals_{self.name}"] = (highs_short >= highs_long).astype(float)
            signal = (highs_short >= highs_long).astype(float)
            signal.name = f"signals_{self.name}"

            breakout_signals_dict[sym] = signal

        return breakout_signals_dict