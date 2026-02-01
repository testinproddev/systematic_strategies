from config import cfg
from utils.helpers import is_stable_base, get_price_at_or_before

import time
import requests
import pandas as pd
from typing import Optional
import os
from pathlib import Path

def fetch_klines(
    symbol: str,
    start_date,
    end_date,
    interval: str = "1d",
    max_retries: int = 3,
    sleep: float = 0.5,
) -> pd.DataFrame:
    """
    Fetch OHLCV data from Binance with robustness suitable for backtesting.
    """

    start_ts = int(pd.Timestamp(start_date).timestamp() * 1000)
    end_ts = int(pd.Timestamp(end_date).timestamp() * 1000)

    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ts,
        "endTime": end_ts,
        "limit": 1000,
    }

    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()

            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(
                data,
                columns=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "num_trades",
                    "taker_base",
                    "taker_quote",
                    "ignore",
                ],
            )

            idx = pd.to_datetime(df["close_time"], unit="ms", utc=True)
            df.index = idx  # keep full timestamp
            df = df.drop(columns="close_time", errors="ignore")

            cols = ["open", "high", "low", "close", "volume"]
            df[cols] = df[cols].astype("float64")

            df = df[cols]

            # Defensive checks
            df = df.sort_index()
            df = df[~df.index.duplicated(keep="first")]
            df = df[df["volume"] >= 0]

            return df

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"[ERROR] fetch_klines failed for {symbol}: {e}")
                return pd.DataFrame()
            time.sleep(sleep)

    return pd.DataFrame()




# OLD FETCH
# def fetch_klines(symbol, start_date, end_date, interval='1d'):
#     try:
#         start_ts = int(time.mktime(start_date.timetuple())) * 1000
#         end_ts = int(time.mktime(end_date.timetuple())) * 1000
#         url = f"{config.BINANCE_BASE}/klines"
#         params = {"symbol": symbol, "interval": interval, "startTime": start_ts, "endTime": end_ts}
#         r = requests.get(url, params=params, timeout=20)
#         r.raise_for_status()
#         data = r.json()
#         if not isinstance(data, list) or len(data) == 0:
#             return pd.DataFrame()
#         df = pd.DataFrame(data, columns=[
#             'open_time','open','high','low','close','volume',
#             'close_time','quote_asset_volume','num_trades','taker_base','taker_quote','ignore'
#         ])
#         df['date'] = pd.to_datetime(df['open_time'], unit='ms')
#         # date :panda datetime object with ms, ie timestamp
#         df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
        
#         return df[['date','open','high','low','close','volume']].reset_index(drop=True).set_index('date')
#     except Exception as e:
#         print(f"[WARN] fetch_klines({symbol}): {e}")
#         return pd.DataFrame()
    
# def fetch_binance_ticker_24h(retries=3, delay=2):
#     url = f"{config.BINANCE_BASE}/ticker/24hr"
#     for attempt in range(retries):
#         try:
#             r = requests.get(url, timeout=20, headers=config.HEADERS)
#             r.raise_for_status()
#             return r.json()
#         except requests.exceptions.HTTPError as e:
#             if r.status_code == 418:
#                 print(f"[WARN] Binance returned 418, retrying in {delay}s... (attempt {attempt+1})")
#                 time.sleep(delay)
#             else:
#                 raise
#         except Exception as e:
#             print(f"[ERROR] fetch_binance_ticker_24h: {e}")
#             time.sleep(delay)
#     return []

# def get_ticker_price(symbol, retries=3, delay=1):
#     url = f"{config.BINANCE_BASE}/ticker/price"
#     for attempt in range(retries):
#         try:
#             r = requests.get(url, params={'symbol': symbol}, timeout=5, headers=config.HEADERS)
#             r.raise_for_status()
#             return float(r.json().get('price', 0.0))
#         except requests.exceptions.HTTPError as e:
#             if r.status_code == 418:
#                 time.sleep(delay)
#             else:
#                 return None
#         except Exception:
#             time.sleep(delay)
#     return None


def cache_coin_data_dict(coin_data, cache_dir="coin_data_cache"):
    """Cache {coin: DataFrame} as separate parquet files"""
    Path(cache_dir).mkdir(exist_ok=True)
    
    for coin, df in coin_data.items():
        df.to_parquet(f"{cache_dir}/{coin}.parquet")
    print(f"✅ Cached {len(coin_data)} DataFrames to {cache_dir}/")

def load_coin_data_dict(cache_dir="coin_data_cache"):
    """Load {coin: DataFrame} from parquet files"""
    coin_data = {}
    for file in Path(cache_dir).glob("*.parquet"):
        coin = file.stem
        coin_data[coin] = pd.read_parquet(file)
    print(f"📂 Loaded {len(coin_data)} DataFrames from cache")
    return coin_data

def get_coin_data(config):
    """
    Return {symbol: OHLCV DataFrame} for selected coins.
    """

    cache_dir = "coin_data_cache"

    # ---------- LOAD FROM CACHE ----------
    if not config.FORCE_REFRESH and os.path.exists(cache_dir):
        coin_data = load_coin_data_dict(cache_dir)

        print(f"✅ Loaded {list(coin_data.keys())} from cache")
        print("📊 Coin Date Ranges:")
        print("-" * 50)

        for coin, df in coin_data.items():
            if df.empty:
                print(f"{coin:>10}: EMPTY DataFrame")
            else:
                print(
                    f"{coin:>10}: "
                    f"{df.index.min():%Y-%m-%d} → {df.index.max():%Y-%m-%d} "
                    f"({len(df)} rows)"
                )

        return coin_data

    # ---------- FETCH FROM BINANCE ----------
    print("Fetching coins data...")
    coin_data = {}

    for coin in config.COIN_SELECTION:

        if len(coin_data) >= len(config.COIN_SELECTION):
            break

        if is_stable_base(config, coin):
            continue

        df = fetch_klines(
            coin,
            config.START_DATE,
            config.END_DATE,
            interval=config.GRANULARITY,
        )

        time.sleep(config.REQUEST_SLEEP)

        if df is None or len(df) < 20:
            print(f"[DEBUG] {coin} skipped: insufficient history.")
            continue

        if get_price_at_or_before(df, config.START_DATE) is None:
            print(f"[DEBUG] {coin} skipped: no price on/before start date.")
            continue

        coin_data[coin] = df

    return coin_data

# OLD get_coin_data
# def get_coin_data(config):
    
#     #returns dict of coins with their ohlc
#     #input tickers list at function call in "coin_selection_override" to override default coin selection, eg 'BTCUSDT'
#     cache_dir = "coin_data_cache"
#     if not config.FORCE_REFRESH and os.path.exists(cache_dir):
#         coin_data = load_coin_data_dict(cache_dir) 
#         print(f"✅ Loaded {list(coin_data.keys())} from cache")
#         print("📊 Coin Date Ranges:")
#         print("-" * 50)
#         for coin, df in coin_data.items():
#             if not df.empty:
#                 start_date = df.index.min().strftime('%Y-%m-%d')
#                 end_date = df.index.max().strftime('%Y-%m-%d')
#                 print(f"{coin:>10}: {start_date} → {end_date} ({len(df)} rows)")
#             else:
#                 print(f"{coin:>10}: EMPTY DataFrame")
#         return coin_data
#     else:
#         coin_data={}
#         coin_fetched=[]
#         local_target_N=len(config.COIN_SELECTION)

#         print("Fetching coins data...")
#         for coin in config.COIN_SELECTION:
#             if len(coin_data) >= local_target_N:
#                 break
#             if is_stable_base(config, coin):
#                 continue
#             if coin not in coin_data:
#                 dfc = fetch_klines(coin, config.START_DATE, config.END_DATE, interval=config.GRANULARITY)
#                 time.sleep(config.REQUEST_SLEEP)
#                 if dfc is None or dfc.empty or len(dfc) < 20:
#                     print(f"[DEBUG] Candidate {coin} skipped: insufficient history ({0 if dfc is None else len(dfc)} rows).")
#                     continue
#                 coin_data[coin] = dfc
#             if get_price_at_or_before(coin_data[coin], config.START_DATE) is None:
#                 print(f"[DEBUG] Candidate {coin} skipped: no price on/before rebalance date.")
#                 continue
#             coin_fetched.append(coin)

        # # Cache the dictionary
        # cache_coin_data_dict(coin_data, cache_dir)
        # print(f"✅ Fetched & cached {len(coin_data)} coins")

        # if len(config.COIN_SELECTION) < local_target_N :
        #     print(f"[WARN] Only {len(config.COIN_SELECTION)} symbols available with sufficient history on {pd.to_datetime(config.START_DATE).date()} (needed {local_target_N}).")
        # else:
        #     print(f"[INFO] Top {len(config.COIN_SELECTION)} selected on {pd.to_datetime(config.START_DATE).date()}")
        #     print(f"[INFO] Coin selection : {config.COIN_SELECTION}")

        # print(f"Config: TARGET_N={local_target_N}, GRANULARITY={config.GRANULARITY}")

        # return coin_data