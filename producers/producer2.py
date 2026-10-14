"""Producer 2: rolling 24 h market statistics from Binance -> Kafka topic pipeline.market-stats

Source endpoint: GET /api/v3/ticker/24hr (public, no API key).
Different endpoint and different fields from Producer 1, so the two streams stay separate.
Sends one JSON message per symbol every POLL_SECONDS. Stop with Ctrl+C.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TOPIC = "pipeline.market-stats"
BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT").split(",")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))


def now_iso():
    """Current UTC time as ISO 8601 with milliseconds and a trailing Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def fetch_stats():
    """Call the source once and return the raw list of 24 h stat dicts."""
    resp = requests.get(
        f"{BASE_URL}/api/v3/ticker/24hr",
        params={"symbols": json.dumps(SYMBOLS, separators=(",", ":")), "type": "FULL"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def to_message(raw, captured_at):
    """Turn one raw API item into the message defined in contract.md."""
    return {
        "symbol": raw["symbol"],
        "last_price": float(raw["lastPrice"]),
        "price_change_pct": float(raw["priceChangePercent"]),
        "high_price": float(raw["highPrice"]),
        "low_price": float(raw["lowPrice"]),
        "volume": float(raw["volume"]),
        "quote_volume": float(raw["quoteVolume"]),
        "trade_count": int(raw["count"]),
        "captured_at": captured_at,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )
    print(f"[producer2] source={BASE_URL} topic={TOPIC} symbols={SYMBOLS} every {POLL_SECONDS}s")
    try:
        while True:
            started = time.time()
            try:
                stats = fetch_stats()
                captured_at = now_iso()
                futures = []
                for raw in stats:
                    msg = to_message(raw, captured_at)
                    futures.append(producer.send(TOPIC, key=msg["symbol"], value=msg))
                    print(f"[producer2] sent {json.dumps(msg)}", flush=True)
                producer.flush()
                # flush() does not raise when a message fails after its retries; get() does.
                for future in futures:
                    future.get(timeout=10)
            except requests.RequestException as err:
                print(f"[producer2] source request failed, will retry: {err}", file=sys.stderr, flush=True)
            except KafkaError as err:
                print(f"[producer2] Kafka send failed, will retry: {err}", file=sys.stderr, flush=True)
            time.sleep(max(0.0, POLL_SECONDS - (time.time() - started)))
    except KeyboardInterrupt:
        print("\n[producer2] stopping")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
