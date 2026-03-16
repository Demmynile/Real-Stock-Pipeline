import requests
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

API_KEY = os.getenv("RAPIDAPI_KEY") or os.getenv("API_KEY")
API_HOST = os.getenv("RAPIDAPI_HOST", "alpha-vantage.p.rapidapi.com")
URL = f"https://{API_HOST}/query"
SYMBOL = os.getenv("STOCK_SYMBOL", "AAPL")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

if not API_KEY:
    raise ValueError("Set RAPIDAPI_KEY (or API_KEY) in your .env before running ingestion.")

def create_producer():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except NoBrokersAvailable:
            print(f"Kafka broker {bootstrap_servers} not ready yet. Retrying in 5 seconds...")
            time.sleep(5)

producer = create_producer()

headers = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": API_HOST
}

while True:
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": SYMBOL,
        "datatype": "json"
    }

    try:
        response = requests.get(URL, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        retry_delay = 300 if status_code == 429 else POLL_INTERVAL_SECONDS
        print(f"RapidAPI request failed ({status_code or 'unknown status'}): {exc}. Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
        continue

    producer.send("events", data)
    producer.flush()

    print("Sent event to Kafka")

    time.sleep(POLL_INTERVAL_SECONDS)