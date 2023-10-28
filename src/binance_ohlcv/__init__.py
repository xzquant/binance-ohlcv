import httpx
import zipfile
import io
import pandas as pd
import datetime
import os
import logging

logger = logging.getLogger(__name__)


def get_spot(symbol, timeframe, start, end):
    return get("spot", symbol, timeframe, start, end)


def get_futures_um(symbol, timeframe, start, end):
    return get("futures/um", symbol, timeframe, start, end)


def get_futures_cm(symbol, timeframe, start, end):
    return get("futures/cm", symbol, timeframe, start, end)


def get(type, symbol, timeframe, start, end):
    dfs = [
        get_daily_ohlcv(type, symbol, timeframe, date)
        for date in date_range(start, end)
    ]
    return pd.concat(dfs)


def date_range(start: datetime.date, end: datetime.date):
    date = start
    while date <= end:
        yield date
        date += datetime.timedelta(days=1)


def date2str(date: datetime.date):
    return date.strftime("%Y-%m-%d")


def daily_path(type, symbol, timeframe, date):
    path = os.path.expanduser(
        f"~/.binance-ohlcv/{type}/{symbol}/{timeframe}/{date2str(date)}.pkl"
    )
    return path


def get_daily_ohlcv(type, symbol, timeframe, date):
    try:
        df = get_daily_ohlcv_from_disk(type, symbol, timeframe, date)
    except FileNotFoundError:
        df = get_daily_ohlcv_from_binance(type, symbol, timeframe, date)
        path = daily_path(type, symbol, timeframe, date)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_pickle(path)
        logger.debug(f"save to disk: {path}")
    return df


def get_daily_ohlcv_from_disk(type, symbol, timeframe, date: datetime.date):
    path = daily_path(type, symbol, timeframe, date)
    df = pd.read_pickle(path)
    logger.debug(f"loaded from disk: {path}")
    return df


def get_daily_ohlcv_from_binance(type, symbol, timeframe, date: datetime.date):
    # example urls:
    # https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2023-10-27.zip
    # https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-10-27.zip
    url = f"https://data.binance.vision/data/{type}/daily/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{date2str(date)}.zip"

    logger.debug(f"fetch from binance: {url}")

    resp = httpx.get(url)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zipf:
        assert len(zipf.namelist()) == 1
        with zipf.open(zipf.namelist()[0]) as csvf:
            df = pd.read_csv(
                csvf,
                usecols=[0, 1, 2, 3, 4, 5],
                names=["timestamp", "open", "high", "low", "close", "volume"],
                header=None if type == "spot" else 0,
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            return df
