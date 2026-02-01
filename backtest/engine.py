from datetime import timedelta
import pandas as pd
import numpy as np
from config import cfg

class BacktestEngine:
    """
    Generic backtest engine for systematic strategies.
    """

    def __init__(self, coin_data, strategy, config):
        self.coin_data = coin_data
        self.strategy = strategy
        self.config = config
        self.all_dates = self._generate_all_dates()

    def _generate_all_dates(self):
        freq = self._granularity_to_pandas_freq(self.config.GRANULARITY)
        start = pd.Timestamp(self.config.START_DATE).normalize()
        end = pd.Timestamp(self.config.END_DATE).normalize()
        return pd.date_range(start=start, end=end, freq=freq)

    @staticmethod
    def _granularity_to_pandas_freq(gran: str) -> str:
        gran = gran.lower()
        if gran.endswith('d'): return f"{int(gran[:-1] or 1)}D"
        if gran.endswith('h'): return f"{int(gran[:-1] or 1)}H"
        if gran.endswith('m'): return f"{int(gran[:-1] or 1)}T"
        return gran

    def _initialize_dataframes(self):
        """
        Initialize per-symbol and portfolio DataFrames.
        """
        # Portfolio master
        self.strategy_data['strat'] = pd.DataFrame(index=self.all_dates).assign(
            nav=0.0,
            cash=self.initial_capital,
            nb_positions=0,
            opened_positions=0,
            closed_positions=0,
            total_purchases=0.0,
            total_sales=0.0,
            total_realized_pnl=0.0
        )

        # Per-symbol DataFrames
        for sym, df in self.coin_data.items():
            df_reindexed = df.reindex(self.all_dates).ffill()
            self.strategy_data[sym] = pd.DataFrame(index=self.all_dates).assign(
                close=df_reindexed['close'],
                units=0.0,
                purchase=0.0,
                sale=0.0,
                purchase_price=0.0,
                realized_pnl=0.0,
                signal=""
            )

    def run(self):
        """
        Run the backtest for all dates
        """
        rebalance_dates = pd.date_range(
            start=self.all_dates[0],
            end=self.all_dates[-1],
            freq=f"{cfg.FREQUENCY_DAYS}D"
        )

        coin_data_for_sim = {}
        shifted_signals={}
        logreturns_asset={}
        logreturns_strat={}

        all_dates = pd.DatetimeIndex(self.all_dates).sort_values()

        #creates coin_data_df with all_dates timestamps
        for sym, df in self.coin_data.items():

            # Reset index as explicit column to be able to usemerge_asof (index doesnot work)
            df_reset = df.reset_index().rename(columns={"date": "timestamp"})
            target = pd.DataFrame({"timestamp": all_dates})

            # As-of merge
            coin_data_for_sim_df = pd.merge_asof(
                target,
                df_reset,
                on="timestamp",
                direction="backward",   # ← latest past observation
                allow_exact_matches=True
            ).set_index("timestamp")
            
            strat_data_df = pd.DataFrame(
                {
                    "positions": 0,
                    "trade": 0,
                    "fee":0;
                },
                index=coin_data_for_sim_df.index,
            )

            coin_data_for_sim[sym] = coin_data_for_sim_df
            logreturns_asset[sym] = np.log(coin_data_for_sim_df['close']).diff()

            signals=self.strategy.generate_signals(coin_data_for_sim)
            # signals_rebalance_dates=
            shifted_signals[sym] = signals[sym].shift(1).fillna(0)
            logreturns_strat[sym] = logreturns_asset[sym] * shifted_signals[sym]

        print(logreturns_asset)
        print(logreturns_strat)
        print(signals)
        
        return logreturns_strat