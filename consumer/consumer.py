import boto3
import json
import time
import os
from kafka import KafkaConsumer
from botocore.client import Config
#connecting to minio s3

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "test1234")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")

s3= boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
    config=Config(s3={'addressing_style': 'path'})
)

bucket_name="raw-transactions"
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}.")
def create_consumer(group_id):
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        enable_auto_commit=bool(group_id),
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


consumer = create_consumer(KAFKA_GROUP_ID)
print(
    f"Consumer listening on topic '{KAFKA_TOPIC}' via {KAFKA_BOOTSTRAP_SERVERS} "
    f"(group_id='{KAFKA_GROUP_ID or 'none'}')"
)

empty_assignment_checks = 0
while True:
    packets = consumer.poll(timeout_ms=5000, max_records=100)
    if not consumer.assignment():
        empty_assignment_checks += 1
        print("No partition assignment yet. Waiting for broker coordinator...")
        if KAFKA_GROUP_ID and empty_assignment_checks >= 3:
            print(
                "Group mode is not assigning partitions in this broker setup. "
                "Switching to non-group mode."
            )
            consumer.close()
            KAFKA_GROUP_ID = None
            consumer = create_consumer(KAFKA_GROUP_ID)
            print(f"Consumer restarted in non-group mode for topic '{KAFKA_TOPIC}'.")
            empty_assignment_checks = 0
        continue

    empty_assignment_checks = 0
    if not packets:
        print("No new packets in the last 5s. Waiting...")
        continue

    for _, messages in packets.items():
        for message in messages:
            record=message.value
            symbol = record.get("symbol", "unknown")
            ts = record.get("fetched_at", int(time.time()))
            key = f"{symbol}/{ts}.json"
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(record),
                ContentType="application/json"
            )
            print(f"Saving record of {symbol} = s3://{bucket_name}/{key}")