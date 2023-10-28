import httpx
import zipfile
import io
import pandas as pd

url = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-10-27.zip"
# url = "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-10-27.zip"
resp = httpx.get(url)
print(resp.content)

with zipfile.ZipFile(io.BytesIO(resp.content)) as zipf:
    assert len(zipf.namelist()) == 1
    with zipf.open(zipf.namelist()[0]) as csvf:
        df = pd.read_csv(
            csvf,
            usecols=[0, 1, 2, 3, 4, 5],
            names=["timestamp", "open", "high", "low", "close", "volume"],
            header=0,
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("timestamp", inplace=True)
        print(df)
