import requests
import json
import time
import os
from kafka import KafkaProducer

API_KEY = os.getenv("RAPIDAPI_KEY")
API_HOST = os.getenv("RAPIDAPI_HOST", "alpha-vantage.p.rapidapi.com")
URL = f"https://{API_HOST}/query"
SYMBOL = os.getenv("STOCK_SYMBOL", "AAPL")

if not API_KEY:
    raise ValueError("Set RAPIDAPI_KEY (or API_KEY) in your .env before running ingestion.")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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

    response = requests.get(URL, headers=headers, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    producer.send("events", data)

    print("Sent event to Kafka")

    time.sleep(30) 