import os
import datetime
import pandas as pd
from binance_ohlcv import get_spot, get_futures_um, get_futures_cm


def test_spot(tmp_path):
    os.environ["BINANCE_OHLCV_CACHE_DIR"] = str(tmp_path)

    start = datetime.date(2023, 10, 1)
    end = datetime.date(2023, 10, 1)

    df = get_spot(symbol="BTCUSDT", timeframe="1h", start=start, end=end)
    assert df.index[0] == pd.Timestamp(2023, 10, 1, tz="utc")
    assert df.index[-1] == pd.Timestamp(2023, 10, 1, 23, tz="utc")
    assert len(df.index) == 24

    start = datetime.date(2023, 10, 1)
    end = datetime.date(2023, 10, 2)
    df = get_spot(symbol="BTCUSDT", timeframe="1h", start=start, end=end)
    assert df.index[0] == pd.Timestamp(2023, 10, 1, tz="utc")
    assert df.index[-1] == pd.Timestamp(2023, 10, 2, 23, tz="utc")
    assert len(df.index) == 48


def test_futures_um(tmp_path):
    os.environ["BINANCE_OHLCV_CACHE_DIR"] = str(tmp_path)

    start = datetime.date(2023, 10, 1)
    end = datetime.date(2023, 10, 1)

    df = get_futures_um(symbol="BTCUSDT", timeframe="1h", start=start, end=end)
    assert df.index[0] == pd.Timestamp(2023, 10, 1, tz="utc")
    assert df.index[-1] == pd.Timestamp(2023, 10, 1, 23, tz="utc")
    assert len(df.index) == 24

    start = datetime.date(2023, 10, 1)
    end = datetime.date(2023, 10, 2)
    df = get_futures_um(symbol="BTCUSDT", timeframe="1h", start=start, end=end)
    assert df.index[0] == pd.Timestamp(2023, 10, 1, tz="utc")
    assert df.index[-1] == pd.Timestamp(2023, 10, 2, 23, tz="utc")
    assert len(df.index) == 48


def test_futures_cm(tmp_path):
    os.environ["BINANCE_OHLCV_CACHE_DIR"] = str(tmp_path)

    start = datetime.date(2023, 10, 1)
    end = datetime.date(2023, 10, 1)

    df = get_futures_cm(symbol="BTCUSD_PERP", timeframe="1h", start=start, end=end)
    assert df.index[0] == pd.Timestamp(2023, 10, 1, tz="utc")
    assert df.index[-1] == pd.Timestamp(2023, 10, 1, 23, tz="utc")
    assert len(df.index) == 24

    start = datetime.date(2023, 10, 1)
    end = datetime.date(2023, 10, 2)
    df = get_futures_cm(symbol="BTCUSD_PERP", timeframe="1h", start=start, end=end)
    assert df.index[0] == pd.Timestamp(2023, 10, 1, tz="utc")
    assert df.index[-1] == pd.Timestamp(2023, 10, 2, 23, tz="utc")
    assert len(df.index) == 48
