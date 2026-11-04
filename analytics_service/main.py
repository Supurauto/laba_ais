import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared import ORDERS_TOPIC, consume, database


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Analytics] %(message)s",
)
logger = logging.getLogger("analytics")

DATABASE_PATH = "/data/analytics.db"
stop_event = threading.Event()


def initialize_database():
    with database(DATABASE_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_stats (
                order_id TEXT PRIMARY KEY,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                version INTEGER NOT NULL
            )
            """
        )


def handle_order_event(event):
    with database(DATABASE_PATH) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM order_stats
            WHERE order_id = ?
            """,
            (event["orderId"],),
        ).fetchone()

        current_version = 0 if row is None else row["version"]

        if event["version"] <= current_version:
            return

        if event["version"] != current_version + 1:
            raise ValueError("Пропущено событие заказа")

        event_type = event["eventType"]

        if event_type == "OrderCreated":
            if row is not None:
                raise ValueError("Повторное создание заказа")

            amount = event["payload"]["totalAmount"]
            status = "PENDING"

        else:
            if row is None:
                raise ValueError("Отсутствует OrderCreated")

            amount = row["amount"]

            if event_type == "OrderPaid":
                status = "CONFIRMED"

            elif event_type == "OrderCancelled":
                status = "CANCELLED"

            else:
                raise ValueError("Неизвестный тип события")

        connection.execute(
            """
            INSERT INTO order_stats (
                order_id, amount, status, version
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(order_id)
            DO UPDATE SET
                amount = excluded.amount,
                status = excluded.status,
                version = excluded.version
            """,
            (
                event["orderId"],
                amount,
                status,
                event["version"],
            ),
        )

    logger.info(
        "Обработано %s: orderId=%s, status=%s, version=%s",
        event_type,
        event["orderId"],
        status,
        event["version"],
    )


@asynccontextmanager
async def lifespan(app):
    initialize_database()
    stop_event.clear()

    worker = threading.Thread(
        target=consume,
        args=(
            ORDERS_TOPIC,
            "order-analytics",
            handle_order_event,
            stop_event,
        ),
        daemon=True,
    )
    worker.start()

    try:
        yield
    finally:
        stop_event.set()
        worker.join(timeout=10)


app = FastAPI(
    title="BookStore — Analytics",
    version="6.0",
    docs_url="/analytics/docs",
    openapi_url="/analytics/openapi.json",
    redoc_url=None,
    lifespan=lifespan,
)


@app.get("/analytics/summary", tags=["Analytics"])
def get_summary():
    with database(DATABASE_PATH) as connection:
        rows = connection.execute(
            "SELECT amount, status FROM order_stats"
        ).fetchall()

    counts = {
        "PENDING": 0,
        "CONFIRMED": 0,
        "CANCELLED": 0,
    }

    total_amount = 0
    paid_amount = 0

    for row in rows:
        counts[row["status"]] += 1
        total_amount += row["amount"]

        if row["status"] == "CONFIRMED":
            paid_amount += row["amount"]

    total_orders = len(rows)

    return {
        "totalOrders": total_orders,
        "ordersByStatus": counts,
        "totalAmount": round(total_amount, 2),
        "paidAmount": round(paid_amount, 2),
        "averageOrderAmount": (
            round(total_amount / total_orders, 2)
            if total_orders
            else 0
        ),
    }