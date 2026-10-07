import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from dotenv import load_dotenv

from ecom_pipeline.postgres import make_tables, write_data_to_postgres
from ecom_pipeline.producer import ensure_topics
from kafka import DefaultSerializer, JsonSerializer, KafkaConsumer, KafkaProducer

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
if KAFKA_BOOTSTRAP is None:
    raise RuntimeError("KAFKA_BOOTSTRAP environment var is missing!")


def consume_stream(topic: str, group_id: str, timeout: float = 50000):
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=JsonSerializer(),
        key_deserializer=DefaultSerializer(),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout,
    )
    try:
        for message in consumer:
            yield message.value, consumer
    finally:
        consumer.close()


def consume_stream_cli(topic: str, group_id: str):
    print(f"Consuming {topic}")
    for row_dict, consumer in consume_stream(topic, group_id):
        write_data_to_postgres(row_dict, topic)
        consumer.commit()


def join_orders_products_streams():
    table_dict = {}
    for row, consumer in consume_stream("products", "joined-stream"):
        table_dict[row["id"]] = row
        consumer.commit()
    for order_row, consumer in consume_stream("orders", "joined-stream"):
        product_row = table_dict.get(order_row["product_id"])
        if product_row == None:
            continue
        order_enriched_row = order_row | {
            "revenue": product_row.get("price") * order_row.get("quantity")
        }
        write_data_to_postgres(order_enriched_row, "orders_enriched")
        consumer.commit()


def alert_processor():
    ensure_topics({"alerts": {}})
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        key_serializer=DefaultSerializer(),
        value_serializer=JsonSerializer(),
        enable_idempotence=True,
        acks="all",
    )
    for msg, consumer in consume_stream("inventory", "inventory-alerts-group"):
        if msg["stock"] < 5:
            alert = {
                "product_id": msg["product_id"],
                "alert_type": "LOW_STOCK",
                "message": f"Stock for {msg['product_id']} is {msg['stock']}",
            }
            producer.send("alerts", key=msg["product_id"], value=alert)
            write_data_to_postgres(alert, "alerts")
        consumer.commit()
    producer.close()


def consume_all_streams():
    make_tables()
    tasks = {
        "orders": partial(consume_stream_cli, topic="orders", group_id="orders-group"),
        "products": partial(
            consume_stream_cli, topic="products", group_id="products-group"
        ),
        "inventory": partial(
            consume_stream_cli, topic="inventory", group_id="inventory-group"
        ),
        "join-orders-products": join_orders_products_streams,
        "process-alerts": alert_processor,
    }
    with ThreadPoolExecutor(max_workers=6) as thread_exe:
        futures = {thread_exe.submit(func): name for name, func in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"{name} failed: {e}")
