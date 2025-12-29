import requests
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

# =========================
# Config
# =========================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    linger_ms=50
)


BINANCE_URL = "https://api.binance.com/api/v3/klines"

ASSETS = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH"
}

INTERVAL = "1h"       # 1m | 5m | 1h | 1d
LIMIT = 1000          # max per request
START_DATE = "2025-12-26"

# =========================
# Fetch logic
# =========================

def fetch_klines(symbol, start_ts):
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT,
        "startTime": start_ts
    }
    r = requests.get(BINANCE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    rows = []
    ingest_ts = datetime.now(timezone.utc)
    start_ts = int(datetime.fromisoformat(START_DATE).timestamp() * 1000)

    for symbol, asset in ASSETS.items():
        print(f"Fetching {asset} historical prices...")
        current_ts = start_ts

        while True:
            data = fetch_klines(symbol, current_ts)
            if not data:
                break

            for k in data:
                open_ts = k[0]
                close_ts = k[6]

                payload = {
                    "asset": asset,
                    "price_usd": float(k[4]),           # close price
                    "market_cap": None,                 # not provided by Binance
                    "volume_24h": float(k[7]),          # quote volume (USDT)
                    "source_timestamp": datetime.fromtimestamp(
                        close_ts / 1000, tz=timezone.utc
                    ).isoformat(),
                    "ingest_timestamp": ingest_ts.isoformat(),
                    "source": "binance"
                }
                # print(payload)
                producer.send("crypto_prices", payload)

            current_ts = data[-1][0] + 1
    producer.flush()
