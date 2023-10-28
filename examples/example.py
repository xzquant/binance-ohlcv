import binance_ohlcv as bo
from datetime import date

df1 = bo.get_spot(
    symbol="BTCUSDT", timeframe="1s", start=date(2023, 1, 1), end=date(2023, 1, 2)
)
df2 = bo.get_futures_um(
    symbol="BTCUSDT", timeframe="1m", start=date(2023, 1, 1), end=date(2023, 1, 2)
)
df3 = bo.get_futures_cm(
    symbol="BTCUSD_PERP", timeframe="1m", start=date(2023, 1, 1), end=date(2023, 1, 2)
)

print(df1)
print(df2)
print(df3)
