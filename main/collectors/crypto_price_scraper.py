import requests
import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

URL = "https://api.coingecko.com/api/v3/simple/price"

ASSETS = {
    "bitcoin": "BTC",
    "ethereum": "ETH"
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_prices():
    params = {
        "ids": ",".join(ASSETS.keys()),
        "vs_currencies": "usd"
    }
    r = requests.get(URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

while True:
    prices = fetch_prices()
    ts = datetime.now(timezone.utc).isoformat()

    for key, symbol in ASSETS.items():
        event = {
            "asset": symbol,
            "timestamp": ts,
            "price": prices[key]["usd"]
        }
        producer.send("price.crypto", value=event)
        print(event)

    producer.flush()
    time.sleep(60)
