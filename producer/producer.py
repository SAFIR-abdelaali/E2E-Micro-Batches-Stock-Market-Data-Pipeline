import time
import json
import requests
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
import concurrent.futures

load_dotenv()
API_KEY=os.getenv("FinnHub_API_KEY")
BASE_URL="https://finnhub.io/api/v1/quote"
SYMBOLS=["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN", "BLK", "IBIT", "BINANCE:BTCUSDT", "BINANCE:ETHUSDT" ,"ORCL", "GLD"]
producer = KafkaProducer(
    bootstrap_servers=["host.docker.internal:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
def data_fetcher(symbol):
    url= f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response= requests.get(url)
        response.raise_for_status()
        data=response.json()
        data["symbol"]=symbol
        data["fetched_at"]=int(time.time())
        return data
    except Exception as e:
        print(f"Can't fetch from {symbol} due to : {e}")
        return None
#to ask for all the quots at the same time im using the concurrency (threadpoolexecutor)
while True:
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(SYMBOLS)) as executor:
        results=executor.map(data_fetcher, SYMBOLS)
        for i in results:
            if i:
                print(f"Producing quote: {i}")
                producer.send("stock-quotes",value=i)
    producer.flush() #ensuring all messages are sent to kafka
    time.sleep(10)
