import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from confluent_kafka import Consumer, Producer


logger = logging.getLogger("shared")

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", "kafka:9092")

ORDERS_TOPIC = "orders-events"
PAYMENTS_TOPIC = "payment-results"


@contextmanager
def database(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row

    try:
        with connection:
            yield connection
    finally:
        connection.close()


def publish(topic, event):
    producer = Producer({
        "bootstrap.servers": KAFKA_ADDRESS,
        "enable.idempotence": True,
        "delivery.timeout.ms": 5000,
        "request.timeout.ms": 3000,
    })

    delivery_results = []

    def on_delivery(error, message):
        delivery_results.append(error)

    producer.produce(
        topic=topic,
        key=event["orderId"].encode("utf-8"),
        value=json.dumps(
            event,
            ensure_ascii=False,
        ).encode("utf-8"),
        on_delivery=on_delivery,
    )

    remaining = producer.flush(6)

    if (
        remaining != 0
        or not delivery_results
        or delivery_results[0] is not None
    ):
        raise RuntimeError("Kafka не подтвердила публикацию")

    logger.info(
        "Kafka: опубликовано %s, orderId=%s, eventId=%s",
        event["eventType"],
        event["orderId"],
        event["eventId"],
    )


def consume(topic, group_id, handler, stop_event):
    while not stop_event.is_set():
        consumer = None

        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_ADDRESS,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })

            consumer.subscribe([topic])

            logger.info(
                "Подписка: topic=%s, group=%s",
                topic,
                group_id,
            )

            while not stop_event.is_set():
                message = consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    raise RuntimeError(str(message.error()))

                event = json.loads(message.value())

                logger.info(
                    "Kafka: получено %s, orderId=%s, group=%s",
                    event["eventType"],
                    event["orderId"],
                    group_id,
                )

                handler(event)

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

        except Exception:
            logger.exception(
                "Ошибка потребителя group=%s",
                group_id,
            )

        finally:
            if consumer is not None:
                consumer.close()

        stop_event.wait(3)