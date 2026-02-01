# ================= HELPERS =================
import pandas as pd
import numpy as np
import datetime
import requests
import time
import matplotlib.pyplot as plt
import sys
import os
from pathlib import Path

from config import cfg

def granularity_to_pandas_freq(g: str) -> str:
    g = str(g).lower()
    if g.endswith('d'):
        n = int(g[:-1]) if len(g) > 1 else 1
        return f"{n}D"
    if g.endswith('h'):
        n = int(g[:-1]) if len(g) > 1 else 1
        return f"{n}H"
    if g.endswith('m'):
        n = int(g[:-1]) if len(g) > 1 else 1
        return f"{n}T"
    return g

def is_stable_base(config, symbol: str) -> bool:
    if not isinstance(symbol, str):
        return False
    if symbol.endswith('USDT'):
        base = symbol[:-4].upper()
        return base in config.STABLE_BASE_ASSETS
    return False



def get_price_at_or_before(df, dt):
    if df is None or df.empty:
        return None
    dt = pd.to_datetime(dt)
    #converts into datetime
    try:
        return float(df['close'].asof(dt))
    except:
        return None

def get_ticker_price(config, symbol, retries=3, delay=1):
    url = f"{config.BINANCE_BASE}/ticker/price"
    for attempt in range(retries):
        try:
            r = requests.get(url, params={'symbol': symbol}, timeout=5, headers=config.HEADERS)
            r.raise_for_status()
            return float(r.json().get('price', 0.0))
        except requests.exceptions.HTTPError as e:
            if r.status_code == 418:
                time.sleep(delay)
            else:
                return None
        except Exception:
            time.sleep(delay)
    return None

def compute_nav(strategy_data, coin_data, dt):
    units = np.array([strategy_data[sym].loc[dt, 'units'] for sym in coin_data])
    closes = np.array([strategy_data[sym].loc[dt, 'close'] for sym in coin_data])
    positions_value = np.dot(units, closes)
    cash = strategy_data['strat'].loc[dt, 'cash']
    return positions_value + cash

def forward_state(df, state_cols, event_cols, prev_idx, curr_idx):
    df.loc[curr_idx, state_cols] = df.loc[prev_idx, state_cols]
    df.loc[curr_idx, event_cols] = 0
    



# ================= STRATEGY =================

def generate_signal(df, run_date):
    if df is None or df.empty:
        return 'FLAT'
    run_date = pd.to_datetime(run_date)
    if run_date not in df.index:
        return 'FLAT'
    idx = df.index.get_loc(run_date)
#     idx = int(df.index[df['date'] == run_date][0])
    if idx < 20:
        return 'FLAT'
    h5 = df['high'].iloc[idx-5:idx].max()
    h20 = df['high'].iloc[idx-20:idx].max()
    return 'LONG' if h5 >= h20 else 'FLAT'
    