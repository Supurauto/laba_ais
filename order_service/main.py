import json
import logging
import os
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pika
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Order] %(message)s",
)
logger = logging.getLogger("order")

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://lab:lab@rabbitmq:5672/",
)
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/orders.db")

EXCHANGE = "orders.events"
ORDER_QUEUE = "order.events"

stop_event = threading.Event()


class OrderItem(BaseModel):
    productId: int = Field(gt=0)
    quantity: int = Field(gt=0)


class CreateOrder(BaseModel):
    userId: int = Field(gt=0)
    items: list[OrderItem] = Field(min_length=1)
    totalAmount: float = Field(gt=0, allow_inf_nan=False)
    simulatePaymentFailure: bool = False


def connect_database():
    return sqlite3.connect(DATABASE_PATH, timeout=10)


def initialize_database():
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)

    demo_orders = [
        {
            "id": "101",
            "userId": 1,
            "createdAt": "2026-10-01T10:00:00Z",
            "items": [
                {"productId": 1, "quantity": 2},
                {"productId": 2, "quantity": 1},
            ],
            "totalAmount": 1850,
        },
        {
            "id": "102",
            "userId": 1,
            "createdAt": "2026-10-02T11:30:00Z",
            "items": [
                {"productId": 1, "quantity": 1},
                {"productId": 3, "quantity": 2},
            ],
            "totalAmount": 1350,
        },
        {
            "id": "103",
            "userId": 2,
            "createdAt": "2026-10-03T09:15:00Z",
            "items": [
                {"productId": 4, "quantity": 1},
            ],
            "totalAmount": 450,
        },
    ]

    connection = connect_database()
    try:
        with connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK (status IN (
                            'PENDING', 'CONFIRMED', 'CANCELLED'
                        ))
                )
                """
            )

            for order in demo_orders:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO orders
                        (id, user_id, payload, status)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        order["id"],
                        order["userId"],
                        json.dumps(order, ensure_ascii=False),
                        "CONFIRMED",
                    ),
                )
    finally:
        connection.close()


def save_order(order):
    connection = connect_database()
    try:
        with connection:
            connection.execute(
                """
                INSERT INTO orders (id, user_id, payload, status)
                VALUES (?, ?, ?, ?)
                """,
                (
                    order["id"],
                    order["userId"],
                    json.dumps(order, ensure_ascii=False),
                    "PENDING",
                ),
            )
    finally:
        connection.close()


def read_order(order_id):
    connection = connect_database()
    try:
        row = connection.execute(
            "SELECT payload, status FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None

    order = json.loads(row[0])
    order["status"] = row[1]
    return order


def change_status(order_id, new_status):
    connection = connect_database()
    try:
        with connection:
            cursor = connection.execute(
                """
                UPDATE orders
                SET status = ?
                WHERE id = ? AND status = 'PENDING'
                """,
                (new_status, order_id),
            )
            return cursor.rowcount == 1
    finally:
        connection.close()


def open_broker():
    parameters = pika.URLParameters(RABBITMQ_URL)
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 10
    parameters.socket_timeout = 5

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    bindings = {
        "payment.events": ["OrderCreated"],
        "order.events": ["PaymentCompleted", "PaymentFailed"],
        "notification.events": [
            "PaymentCompleted",
            "OrderCancelled",
        ],
    }

    for queue_name, event_names in bindings.items():
        channel.queue_declare(queue=queue_name, durable=True)

        for event_name in event_names:
            channel.queue_bind(
                exchange=EXCHANGE,
                queue=queue_name,
                routing_key=event_name,
            )

    channel.confirm_delivery()
    return connection, channel


def publish_event(channel, event_name, payload):
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=event_name,
        body=json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
        mandatory=True,
    )

    logger.info(
        "Опубликовано %s: %s",
        event_name,
        json.dumps(payload, ensure_ascii=False),
    )


def handle_payment_event(channel, method, properties, body):
    try:
        event = json.loads(body)
        order_id = str(event["orderId"])
    except (ValueError, KeyError, TypeError):
        logger.exception("Некорректное сообщение")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False,
        )
        return

    event_name = method.routing_key

    logger.info(
        "Получено %s: %s",
        event_name,
        json.dumps(event, ensure_ascii=False),
    )

    order = read_order(order_id)

    if order is None:
        logger.warning("Неизвестный orderId=%s", order_id)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    if event_name == "PaymentCompleted":
        changed = change_status(order_id, "CONFIRMED")

        if changed:
            logger.info(
                "orderId=%s: PENDING -> CONFIRMED",
                order_id,
            )
        else:
            logger.info(
                "orderId=%s: статус не изменён, "
                "заказ уже завершён",
                order_id,
            )

    elif event_name == "PaymentFailed":
        changed = change_status(order_id, "CANCELLED")

        if changed:
            logger.info(
                "orderId=%s: PENDING -> CANCELLED; компенсация",
                order_id,
            )
        else:
            logger.info(
                "orderId=%s: статус повторно не изменён",
                order_id,
            )

        current_order = read_order(order_id)

        if current_order["status"] == "CANCELLED":
            publish_event(
                channel,
                "OrderCancelled",
                {
                    "orderId": order_id,
                    "userId": current_order["userId"],
                    "reason": event.get(
                        "reason",
                        "Оплата отклонена",
                    ),
                },
            )

    channel.basic_ack(delivery_tag=method.delivery_tag)


def consume_payment_events():
    while not stop_event.is_set():
        connection = None

        try:
            connection, channel = open_broker()
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=ORDER_QUEUE,
                on_message_callback=handle_payment_event,
                auto_ack=False,
            )

            logger.info("Ожидание событий оплаты")

            while not stop_event.is_set():
                connection.process_data_events(time_limit=1)

        except Exception:
            logger.exception(
                "Ошибка потребителя; повторное подключение"
            )

        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except pika.exceptions.AMQPError:
                    logger.warning(
                        "Не удалось корректно закрыть соединение"
                    )

        stop_event.wait(3)


@asynccontextmanager
async def lifespan(app):
    initialize_database()
    stop_event.clear()

    consumer_thread = threading.Thread(
        target=consume_payment_events,
        daemon=True,
    )
    consumer_thread.start()

    yield

    stop_event.set()
    consumer_thread.join(timeout=5)


app = FastAPI(
    title="BookStore — Order",
    version="4.0",
    docs_url="/api/orders/docs",
    openapi_url="/api/orders/openapi.json",
    redoc_url=None,
    lifespan=lifespan,
)


@app.post("/orders", status_code=201, tags=["Order"])
def create_order(data: CreateOrder):
    order_id = str(uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    items = [item.model_dump() for item in data.items]

    order = {
        "id": order_id,
        "userId": data.userId,
        "items": items,
        "totalAmount": data.totalAmount,
        "createdAt": created_at,
    }

    event = {
        "orderId": order_id,
        "userId": data.userId,
        "items": items,
        "totalAmount": data.totalAmount,
        "createdAt": created_at,
        "simulatePaymentFailure": data.simulatePaymentFailure,
    }

    connection = None

    try:
        connection, channel = open_broker()

        save_order(order)
        logger.info(
            "orderId=%s: заказ сохранён, status=PENDING",
            order_id,
        )

        publish_event(channel, "OrderCreated", event)

    except pika.exceptions.AMQPError as error:
        change_status(order_id, "CANCELLED")

        logger.exception(
            "orderId=%s: ошибка публикации OrderCreated",
            order_id,
        )

        raise HTTPException(
            status_code=503,
            detail={
                "message": "Не удалось передать заказ на оплату",
                "orderId": order_id,
            },
        ) from error

    finally:
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except pika.exceptions.AMQPError:
                logger.warning(
                    "Не удалось корректно закрыть соединение"
                )

    return {"id": order_id, "status": "PENDING"}


@app.get("/orders/{order_id}", tags=["Order"])
def get_order(order_id: str):
    order = read_order(order_id)

    if order is None:
        raise HTTPException(
            status_code=404,
            detail="Заказ не найден",
        )

    return order


@app.get("/api/orders", tags=["Order"])
def get_orders(userId: int = Query(..., gt=0)):
    connection = connect_database()
    try:
        rows = connection.execute(
            """
            SELECT payload, status
            FROM orders
            WHERE user_id = ?
            ORDER BY rowid
            """,
            (userId,),
        ).fetchall()
    finally:
        connection.close()

    result = []

    for payload, status in rows:
        order = json.loads(payload)
        order["status"] = status
        result.append(order)

    return result