"""Producer 1: best bid/ask prices from Binance -> Kafka topic pipeline.prices

Source endpoint: GET /api/v3/ticker/bookTicker (public, no API key).
Different endpoint and different fields from Producer 2, so the two streams stay separate.
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
TOPIC = "pipeline.prices"
BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT").split(",") if s.strip()]
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))


def now_iso():
    """Current UTC time as ISO 8601 with milliseconds and a trailing Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def fetch_book():
    """Call the source once and return the raw list of best bid/ask dicts."""
    resp = requests.get(
        f"{BASE_URL}/api/v3/ticker/bookTicker",
        params={"symbols": json.dumps(SYMBOLS, separators=(",", ":"))},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def to_message(raw, captured_at):
    """Turn one raw API item into the message defined in contract.md."""
    return {
        "symbol": raw["symbol"],
        "bid_price": float(raw["bidPrice"]),
        "bid_qty": float(raw["bidQty"]),
        "ask_price": float(raw["askPrice"]),
        "ask_qty": float(raw["askQty"]),
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
    print(f"[producer1] source={BASE_URL} topic={TOPIC} symbols={SYMBOLS} every {POLL_SECONDS}s")
    try:
        while True:
            started = time.time()
            try:
                book = fetch_book()
                captured_at = now_iso()
                futures = []
                for raw in book:
                    msg = to_message(raw, captured_at)
                    futures.append(producer.send(TOPIC, key=msg["symbol"], value=msg))
                    print(f"[producer1] sent {json.dumps(msg)}", flush=True)
                producer.flush()
                # flush() does not raise when a message fails after its retries; get() does.
                for future in futures:
                    future.get(timeout=10)
            except requests.RequestException as err:
                print(f"[producer1] source request failed, will retry: {err}", file=sys.stderr, flush=True)
            except KafkaError as err:
                print(f"[producer1] Kafka send failed, will retry: {err}", file=sys.stderr, flush=True)
            time.sleep(max(0.0, POLL_SECONDS - (time.time() - started)))
    except KeyboardInterrupt:
        print("\n[producer1] stopping")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
