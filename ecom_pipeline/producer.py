import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import sleep

import pandas as pd
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from tenacity import retry, stop_after_attempt, wait_exponential
from kafka.errors import KafkaTimeoutError
from kafka import DefaultSerializer, JsonSerializer, KafkaProducer

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
if KAFKA_BOOTSTRAP is None:
    raise RuntimeError("KAFKA_BOOTSTRAP environment var is missing!")


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=lambda exc: isinstance(exc, KafkaTimeoutError),
)
def ensure_topics(topics: dict):
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    for name, config in topics.items():
        try:
            admin.create_topics(
                {
                    name: {
                        "num_partitions": 1,
                        "replication_factor": 1,
                        "configs": config,
                    }
                }
            )
        except TopicAlreadyExistsError:
            pass
    admin.close()


def produce_records_stream(path: Path, topic: str, key_field: str):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        key_serializer=DefaultSerializer(),
        value_serializer=JsonSerializer(),
        enable_idempotence=True,
        acks="all",
    )
    df = pd.read_csv(path)
    for i, row in enumerate(df.to_dict(orient="records")):
        if row.get("price") != None:
            row["price"] = float(row["price"].lstrip("$"))
        producer.send(topic, key=row[key_field], value=row)
        if (i + 1) % 5 == 0:
            producer.flush()
            print("Flushing producer")
            sleep(0.05)
    producer.close()


def stream_orders_cli():
    produce_records_stream(Path("./data/generated/orders.csv"), "orders", "order_id")


def stream_products_cli():
    produce_records_stream(Path("./data/generated/products.csv"), "products", "id")


def stream_inventory_cli():
    produce_records_stream(
        Path("./data/generated/inventory.csv"), "inventory", "product_id"
    )


def run_all_producers():
    tasks = {
        "orders": stream_orders_cli,
        "products": stream_products_cli,
        "inventory": stream_inventory_cli,
    }
    topics = {"orders": {}, "inventory": {}, "products": {"cleanup.policy": "compact"}}
    ensure_topics(topics)
    with ThreadPoolExecutor(max_workers=3) as thread_exe:
        futures = {thread_exe.submit(func): name for name, func in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"{name} failed: {e}")
