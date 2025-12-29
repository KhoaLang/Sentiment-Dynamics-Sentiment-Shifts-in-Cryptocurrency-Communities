import time
import json
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
ASSETS = {
    "bitcoin": "BTC",
    "ethereum": "ETH"
}

# producer = KafkaProducer(
#     bootstrap_servers="localhost:9092",
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     retries=5,
#     linger_ms=50
# )

def fetch_prices():
    params = {
        "ids": ",".join(ASSETS.keys()),
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_last_updated_at": "true"
    }
    r = requests.get(COINGECKO_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def produce_prices():
    data = fetch_prices()
    ingest_ts = datetime.now(timezone.utc).isoformat()

    for asset_id, symbol in ASSETS.items():
        payload = {
            "asset": symbol,
            "price_usd": data[asset_id]["usd"],
            "market_cap": data[asset_id]["usd_market_cap"],
            "volume_24h": data[asset_id]["usd_24h_vol"],
            "source_timestamp": data[asset_id]["last_updated_at"],
            "ingest_timestamp": ingest_ts,
            "source": "coingecko"
        }
        print(f"Payload run at {datetime.now()}", payload)
        # producer.send("crypto_prices", payload)
        with open("./output/crypto_price_sample.json", "w") as f:
            f.write(json.dumps(payload))
    # producer.flush()

if __name__ == "__main__":
    while True:
        try:
            produce_prices()
            time.sleep(10)  # safe polling interval
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)
