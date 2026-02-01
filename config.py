
# ================= CONFIG =================
from dataclasses import dataclass
from typing import Set, Dict
from datetime import date, timedelta


@dataclass(frozen=True)
class Config:
    COIN_SELECTION: Set[str]
    INITIAL_CAPITAL: float
    FREQUENCY_DAYS: int
    GRANULARITY: str
    FEE: float
    DAYS: int
    REBALANCING: str

    START_DATE: date
    END_DATE: date
    EXPORT_DATA: bool

    BINANCE_BASE: str
    STABLE_BASE_ASSETS: Set[str]
    MAX_WORKERS: int
    REQUEST_SLEEP: float
    HEADERS: Dict[str, str]

    COIN_DATA_CACHE_FILE: str
    FORCE_REFRESH: bool


cfg = Config(
    COIN_SELECTION={"BTCUSDT", "ETHUSDT", "SOLUSDT","BONKUSDT","PUMPUSDT"},
    INITIAL_CAPITAL=1000.0,
    FREQUENCY_DAYS=7,
    GRANULARITY="1d",
    FEE=0.001,
    DAYS=40,
    REBALANCING="prorata_active",

    # START_DATE=date.today() - timedelta(days=40),
    START_DATE=date(2025, 7, 21),
    END_DATE=date.today(),
    EXPORT_DATA=True,

    BINANCE_BASE="https://api.binance.com/api/v3",
    STABLE_BASE_ASSETS={
        "USDC", "BUSD", "DAI", "TUSD", "USDP",
        "USDD", "GUSD", "USN", "FEI", "FDUSD"
    },
    MAX_WORKERS=6,
    REQUEST_SLEEP=0.12,
    HEADERS={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    },

    COIN_DATA_CACHE_FILE="api_data_cache.json",
    FORCE_REFRESH=False,
)


