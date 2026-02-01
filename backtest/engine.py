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
        strat_data={}
        all_dates = pd.DatetimeIndex(self.all_dates).sort_values()

        #creates coin_data_df with all_dates timestamps only
        for sym, df in self.coin_data.items():

            # Reset index as explicit column to be able to usemerge_asof (index doesnot work)
            df_reset = df.reset_index().rename(columns={"date": "timestamp"})
            target = pd.DataFrame({"timestamp": all_dates})

            # As-of merge
            # index name = 'timestamp'
            # original timestamps of df_reset are dropped
            coin_data_for_sim_df = pd.merge_asof(
                target,
                df_reset,
                on="timestamp",
                direction="backward",   # ← latest past observation
                allow_exact_matches=True
            ).set_index("timestamp")
            coin_data_for_sim[sym] = coin_data_for_sim_df

        signals=self.strategy.generate_signals(coin_data_for_sim)
        
        for sym in self.coin_data:

            strat_data_df = pd.DataFrame(
                {
                    "signals":0,
                    "position": 0,
                    "trade": 0,
                    "fee":0,                    
                    "nav":0
                },
                index=coin_data_for_sim_df.index,
            )
            coin_data_for_sim_df=coin_data_for_sim[sym]
            signals_df=signals[sym]
            positions = pd.Series(index=signals_df.index, dtype="float64")
            positions.iloc[0] = 0

            is_rebalance = signals_df.index.isin(rebalance_dates)
            prev_signal = signals_df.shift(1)

            for i in range(1, len(signals_df)):
                positions.iloc[i] = (
                    1 if (is_rebalance[i] and prev_signal.iloc[i] == 1)
                    else 0 if (is_rebalance[i] and prev_signal.iloc[i] == 0)
                    else positions.iloc[i - 1]
                )

            trade=positions.diff()
            fee=cfg.FEE*trade.abs()
            logreturns_asset=np.log(coin_data_for_sim_df['close']).diff()
            logreturns_strat=logreturns_asset*positions-fee
            nav = cfg.INITIAL_CAPITAL * np.exp(logreturns_strat.cumsum())

            strat_data_df = pd.concat(
                    [
                        nav,
                        signals_df,
                        positions,
                        fee,
                        logreturns_strat,
                        logreturns_asset
                    ],
                axis=1,
                keys=[
                    "nav",
                    "signals_df",
                    "positions",
                    "fee",                    
                    "logreturns_strat",
                    "logreturns_asset"
                ]
            )
            strat_data[sym]=strat_data_df



        print(strat_data)
        
        return logreturns_strat