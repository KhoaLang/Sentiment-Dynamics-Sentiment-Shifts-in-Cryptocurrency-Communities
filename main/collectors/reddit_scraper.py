import argparse
import requests
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from random import uniform
from kafka import KafkaProducer


# =========================
# Config
# =========================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
    "blockchain", "defi", "nft", "altcoin", "token", "coin",
    "trading", "investment", "price", "market", "bull", "bear"
]


# =========================
# Helpers
# =========================

def contains_crypto_keyword(text: str) -> bool:
    if not text:
        return False
    text = text.lower()
    return any(keyword in text for keyword in CRYPTO_KEYWORDS)


def extract_after_cursor(soup: BeautifulSoup) -> str | None:
    """
    Extract `after` cursor from old.reddit pagination
    """
    next_button = soup.select_one("span.next-button a")
    if not next_button:
        return None

    href = next_button.get("href")
    if not href or "after=" not in href:
        return None

    return href.split("after=")[-1]


def scrape_subreddit(
    subreddit_url: str,
    limit: int,
    max_pages: int,
    sleep_min: float,
    sleep_max: float
) -> list[dict]:

    base_url = subreddit_url.replace("www.reddit.com", "old.reddit.com")
    posts = []
    after = None
    page = 0

    while len(posts) < limit and page < max_pages:
        url = base_url
        if after:
            url = f"{base_url}?after={after}"

        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        for post in soup.select("div.thing"):
            title_tag = post.select_one("a.title")
            if not title_tag:
                continue

            title_text = title_tag.text.strip()
            if not contains_crypto_keyword(title_text):
                continue

            posts.append({
                "event_id": post.get("data-fullname"),
                "subreddit": post.get("data-subreddit"),
                "raw_text": title_text,
                "created_utc": datetime.fromtimestamp(int(post.get("data-timestamp")) / 1000, tz=timezone.utc),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "source_url": post.get("data-permalink"),
                "matched_keywords": [
                    kw for kw in CRYPTO_KEYWORDS if kw in title_text.lower()
                ]
            })

            if len(posts) >= limit:
                break
        
        # print(posts)

        after = extract_after_cursor(soup)
        if not after:
            break

        page += 1
        time.sleep(uniform(sleep_min, sleep_max))

    return posts


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=100,
        retries=5
    )


def send_posts_to_kafka(producer, topic, posts):
    for post in posts:
        producer.send(topic, key=post["event_id"], value=post)
    producer.flush()


# =========================
# CLI
# =========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Reddit HTML scraper with keyword filtering + pagination (Bronze → Kafka)"
    )

    parser.add_argument("--sites-file", default="sites_to_scrape.txt")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--kafka-topic", default="reddit.bronze")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--sleep-min", type=float, default=3.0)
    parser.add_argument("--sleep-max", type=float, default=6.0)

    return parser.parse_args()


# =========================
# Main
# =========================

def main():
    args = parse_args()
    producer = create_kafka_producer(args.kafka_bootstrap)

    with open(args.sites_file) as f:
        sites = [line.strip() for line in f if line.strip()]

    total = 0

    for site in sites:
        print(f"Scraping {site}")
        try:
            posts = scrape_subreddit(
                subreddit_url=site,
                limit=args.limit,
                max_pages=args.max_pages,
                sleep_min=args.sleep_min,
                sleep_max=args.sleep_max
            )

            if posts:
                send_posts_to_kafka(producer, args.kafka_topic, posts)
                print(f"  sent {len(posts)} posts")
                total += len(posts)
            else:
                print("  no matching posts found")

        except Exception as e:
            print(f"  Failed: {e}")

        time.sleep(uniform(args.sleep_min, args.sleep_max))

    producer.close()
    print(f"\nTotal events sent to Kafka: {total}")


if __name__ == "__main__":
    main()
