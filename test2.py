import binance_ohlcv as bo
import datetime
import logging

logger = logging.getLogger("binance_ohlcv")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    "%m-%d %H:%M:%S",
)
handler.setFormatter(formatter)
logger.addHandler(handler)

r = bo.get_spot(
    symbol="BTCUSDT",
    timeframe="1h",
    start=datetime.date(2023, 1, 5),
    end=datetime.date(2023, 1, 9),
)
print(r)
