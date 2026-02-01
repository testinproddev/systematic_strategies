
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from pathlib import Path

from data.fetch import get_coin_data
from config import cfg
from strategies.breakout import BreakoutStrategy
from backtest.engine import BacktestEngine
# from reporting import metrics, plot




def main():
    # ===================== LOAD DATA =====================
    print("Loading coin data...")
    coin_data = get_coin_data(config=cfg)

    # ===================== INIT STRATEGY =====================
    strategy = BreakoutStrategy(
        short_window=5,
        long_window=20,
        config=cfg,
    )

    # ===================== RUN BACKTEST =====================
    engine = BacktestEngine(coin_data=coin_data,strategy=strategy, config=cfg)
    strategy_data = engine.run()

if __name__ == "__main__":
    main()
