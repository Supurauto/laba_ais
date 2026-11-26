import json
import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel, Field
from payment_client import (
    check_payment,
    close_payment_channel,
    log_payment_client_settings,
)
from shared import (
    ORDERS_TOPIC,
    PAYMENTS_TOPIC,
    consume,
    database,
    publish,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Order] %(message)s",
)
logger = logging.getLogger("order")

EVENT_DB = "/data/events.db"
READ_DB = "/data/order_read.db"

stop_event = threading.Event()


class OrderItem(BaseModel):
    productId: int = Field(gt=0)
    quantity: int = Field(gt=0)


class CreateOrderInput(BaseModel):
    userId: int = Field(gt=0)
    items: list[OrderItem] = Field(min_length=1)
    totalAmount: float = Field(gt=0, allow_inf_nan=False)
    autoPay: bool = True
    simulatePaymentFailure: bool = False

class CheckPaymentInput(BaseModel):
    amount: float = Field(gt=0, allow_inf_nan=False)
    simulate_failure: bool = False

class CommandInput(BaseModel):
    expectedVersion: int | None = Field(default=None, ge=1)


class CancelInput(CommandInput):
    reason: str = Field(
        default="Отмена по запросу клиента",
        min_length=1,
    )


class CheckPaymentInput(BaseModel):
    amount: float = Field(gt=0, allow_inf_nan=False)
    simulate_failure: bool = False


def initialize_databases():
    with database(EVENT_DB) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                position INTEGER PRIMARY KEY AUTOINCREMENT,
                eventId TEXT NOT NULL UNIQUE,
                orderId TEXT NOT NULL,
                eventType TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                version INTEGER NOT NULL,
                published INTEGER NOT NULL DEFAULT 0,
                UNIQUE(orderId, version)
            )
            """
        )

    with database(READ_DB) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS order_view (
                order_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                document TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_order_view_user
            ON order_view(user_id)
            """
        )


def row_to_event(row):
    return {
        "eventId": row["eventId"],
        "orderId": row["orderId"],
        "eventType": row["eventType"],
        "payload": json.loads(row["payload"]),
        "timestamp": row["timestamp"],
        "version": row["version"],
    }


def load_events(connection, order_id):
    rows = connection.execute(
        """
        SELECT *
        FROM events
        WHERE orderId = ?
        ORDER BY version
        """,
        (order_id,),
    ).fetchall()

    return [row_to_event(row) for row in rows]


def apply_event(state, event):
    current_version = 0 if state is None else state["version"]

    if event["version"] != current_version + 1:
        raise ValueError("Нарушен порядок версий")

    event_type = event["eventType"]
    payload = event["payload"]

    if event_type == "OrderCreated":
        if state is not None:
            raise ValueError("Повторное создание заказа")

        state = {
            "id": event["orderId"],
            "userId": payload["userId"],
            "items": payload["items"],
            "totalAmount": payload["totalAmount"],
            "status": "PENDING",
            "createdAt": event["timestamp"],
            "autoPay": payload.get("autoPay", True),
        }

    else:
        if state is None:
            raise ValueError("Нет события OrderCreated")

        state = dict(state)

        if event_type == "OrderPaid":
            state["status"] = "CONFIRMED"
            state["transactionId"] = payload["transactionId"]

        elif event_type == "OrderCancelled":
            state["status"] = "CANCELLED"
            state["cancelReason"] = payload["reason"]

        else:
            raise ValueError("Неизвестное событие")

    state["version"] = event["version"]
    state["updatedAt"] = event["timestamp"]

    return state


def rehydrate(events):
    state = None

    for event in events:
        state = apply_event(state, event)

    return state


def append_event(
    order_id,
    event_type,
    payload,
    expected_version=None,
    from_payment=False,
):
    with database(EVENT_DB) as connection:
        connection.execute("BEGIN IMMEDIATE")

        events = load_events(connection, order_id)
        state = rehydrate(events)
        version = 0 if state is None else state["version"]

        if from_payment:
            if state is None or state["status"] != "PENDING":
                logger.info(
                    "Результат оплаты проигнорирован: orderId=%s",
                    order_id,
                )
                return None

        if (
            expected_version is not None
            and expected_version != version
        ):
            raise HTTPException(
                status_code=409,
                detail="Версия устарела. Прочитайте заказ повторно",
            )

        if event_type == "OrderCreated":
            if state is not None:
                raise HTTPException(
                    status_code=409,
                    detail="Заказ уже существует",
                )

        else:
            if state is None:
                raise HTTPException(
                    status_code=404,
                    detail="Заказ не найден",
                )

            if event_type == "OrderPaid":
                if state["status"] != "PENDING":
                    raise HTTPException(
                        status_code=409,
                        detail="Оплатить можно только ожидающий заказ",
                    )

            elif event_type == "OrderCancelled":
                if state["status"] == "CANCELLED":
                    raise HTTPException(
                        status_code=409,
                        detail="Заказ уже отменён",
                    )

            else:
                raise ValueError("Неизвестная команда")

        event = {
            "eventId": str(uuid4()),
            "orderId": order_id,
            "eventType": event_type,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": version + 1,
        }

        connection.execute(
            """
            INSERT INTO events (
                eventId, orderId, eventType,
                payload, timestamp, version
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event["eventId"],
                event["orderId"],
                event["eventType"],
                json.dumps(payload, ensure_ascii=False),
                event["timestamp"],
                event["version"],
            ),
        )

        new_state = apply_event(state, event)

    logger.info(
        "Сохранено %s: orderId=%s, status=%s, version=%s",
        event_type,
        order_id,
        new_state["status"],
        event["version"],
    )

    return {
        "id": order_id,
        "orderId": order_id,
        "eventId": event["eventId"],
        "eventType": event_type,
        "status": new_state["status"],
        "version": event["version"],
    }


def relay_events():
    while not stop_event.is_set():
        try:
            with database(EVENT_DB) as connection:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM events
                    WHERE published = 0
                    ORDER BY position
                    LIMIT 100
                    """
                ).fetchall()

            for row in rows:
                if stop_event.is_set():
                    break

                event = row_to_event(row)
                publish(ORDERS_TOPIC, event)

                with database(EVENT_DB) as connection:
                    connection.execute(
                        """
                        UPDATE events
                        SET published = 1
                        WHERE eventId = ?
                        """,
                        (event["eventId"],),
                    )

        except Exception:
            logger.exception("Ошибка публикации событий")

        stop_event.wait(1)


def update_read_model(event):
    with database(READ_DB) as connection:
        row = connection.execute(
            """
            SELECT document
            FROM order_view
            WHERE order_id = ?
            """,
            (event["orderId"],),
        ).fetchone()

        state = None if row is None else json.loads(row["document"])

        if state is not None and event["version"] <= state["version"]:
            return

        state = apply_event(state, event)

        connection.execute(
            """
            INSERT INTO order_view (
                order_id, user_id, document
            )
            VALUES (?, ?, ?)
            ON CONFLICT(order_id)
            DO UPDATE SET
                user_id = excluded.user_id,
                document = excluded.document
            """,
            (
                state["id"],
                state["userId"],
                json.dumps(state, ensure_ascii=False),
            ),
        )

    logger.info(
        "Read-модель: orderId=%s, status=%s, version=%s",
        state["id"],
        state["status"],
        state["version"],
    )


def handle_payment_result(event):
    payload = event["payload"]

    if event["eventType"] == "PaymentCompleted":
        append_event(
            order_id=event["orderId"],
            event_type="OrderPaid",
            payload={
                "transactionId": payload["transactionId"],
                "source": "Payment",
            },
            from_payment=True,
        )

    elif event["eventType"] == "PaymentFailed":
        append_event(
            order_id=event["orderId"],
            event_type="OrderCancelled",
            payload={
                "reason": payload["reason"],
                "source": "Payment",
            },
            from_payment=True,
        )


@asynccontextmanager
async def lifespan(app):
    initialize_databases()
    stop_event.clear()
    log_payment_client_settings()

    workers = [
        threading.Thread(
            target=relay_events,
            daemon=True,
        ),
        threading.Thread(
            target=consume,
            args=(
                ORDERS_TOPIC,
                "order-read-model",
                update_read_model,
                stop_event,
            ),
            daemon=True,
        ),
        threading.Thread(
            target=consume,
            args=(
                PAYMENTS_TOPIC,
                "order-payment-results",
                handle_payment_result,
                stop_event,
            ),
            daemon=True,
        ),
    ]

    for worker in workers:
        worker.start()

    try:
        yield
    finally:
        stop_event.set()

        for worker in workers:
            worker.join(timeout=10)

        close_payment_channel()


app = FastAPI(
    title="BookStore — Order",
    version="6.0",
    docs_url="/api/orders/docs",
    openapi_url="/api/orders/openapi.json",
    redoc_url=None,
    lifespan=lifespan,
)


@app.post("/orders", status_code=201, tags=["Commands"])
def create_order(
    data: CreateOrderInput,
    x_user_role: str | None = Header(default=None),
):
    if x_user_role not in ("user", "admin"):
        raise HTTPException(
            status_code=403,
            detail="Создание заказа доступно только пользователю или администратору",
        )

    return append_event(
        order_id=str(uuid4()),
        event_type="OrderCreated",
        payload=data.model_dump(),
    )

@app.post("/orders/{order_id}/pay", tags=["Commands"])
def pay_order(order_id: str, data: CommandInput):
    return append_event(
        order_id=order_id,
        event_type="OrderPaid",
        payload={
            "transactionId": str(uuid4()),
            "source": "ManualCommand",
        },
        expected_version=data.expectedVersion,
    )


@app.post("/orders/{order_id}/cancel", tags=["Commands"])
def cancel_order(order_id: str, data: CancelInput):
    return append_event(
        order_id=order_id,
        event_type="OrderCancelled",
        payload={
            "reason": data.reason,
            "source": "ManualCommand",
        },
        expected_version=data.expectedVersion,
    )


@app.get("/orders/{order_id}", tags=["Queries"])
def get_order(order_id: str):
    with database(READ_DB) as connection:
        row = connection.execute(
            """
            SELECT document
            FROM order_view
            WHERE order_id = ?
            """,
            (order_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Заказ отсутствует в read-модели",
        )

    return json.loads(row["document"])


@app.get("/api/orders", tags=["Queries"])
def get_orders(userId: int = Query(..., gt=0)):
    with database(READ_DB) as connection:
        rows = connection.execute(
            """
            SELECT document
            FROM order_view
            WHERE user_id = ?
            ORDER BY rowid
            """,
            (userId,),
        ).fetchall()

    return [json.loads(row["document"]) for row in rows]


@app.get("/debug/orders/{order_id}/events", tags=["Debug"])
def inspect_events(order_id: str):
    with database(EVENT_DB) as connection:
        events = load_events(connection, order_id)

    if not events:
        raise HTTPException(
            status_code=404,
            detail="События не найдены",
        )

    return {
        "events": events,
        "rehydratedState": rehydrate(events),
    }


@app.post(
    "/check-payment/{order_id}",
    tags=["Payment check"],
)
def check_payment_endpoint(
    order_id: str,
    data: CheckPaymentInput,
):
    return check_payment(
        order_id=order_id,
        amount=data.amount,
        simulate_failure=data.simulate_failure,
    )