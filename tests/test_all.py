import datetime
from binance_ohlcv import get_spot, get_futures_um, get_futures_cm


def test_spot():
    start = datetime.date(2023, 10, 1)
    end = datetime.date(2023, 10, 2)

    get_spot(symbol="BTCUSDT", timeframe="1h", start=start, end=end)
    get_futures_cm(symbol="BTCUSDT", timeframe="1h", start=start, end=end)
    get_futures_um(symbol="BTCUSDT", timeframe="1h", start=start, end=end)
