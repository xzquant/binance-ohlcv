from binance_ohlcv import get_spot
import datetime
import pandas as pd
import os
import asyncio


def test_async(tmp_path):
    os.environ["BINANCE_OHLCV_CACHE_DIR"] = str(tmp_path)

    async def f():
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

    asyncio.run(f())
