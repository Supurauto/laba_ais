import logging
import threading

from shared import ORDERS_TOPIC, consume, database


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Notification] %(message)s",
)
logger = logging.getLogger("notification")

DATABASE_PATH = "/data/notifications.db"
stop_event = threading.Event()


def initialize_database():
    with database(DATABASE_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY
            )
            """
        )


def handle_order_event(event):
    event_type = event["eventType"]

    if event_type not in ("OrderPaid", "OrderCancelled"):
        return

    with database(DATABASE_PATH) as connection:
        row = connection.execute(
            """
            SELECT event_id
            FROM processed_events
            WHERE event_id = ?
            """,
            (event["eventId"],),
        ).fetchone()

        if row is not None:
            return

        if event_type == "OrderPaid":
            logger.info(
                "Отправлено уведомление клиенту: "
                "заказ %s оплачен",
                event["orderId"],
            )

        else:
            logger.info(
                "Отправлено уведомление клиенту: "
                "заказ %s отменён. Причина: %s",
                event["orderId"],
                event["payload"]["reason"],
            )

        connection.execute(
            """
            INSERT INTO processed_events (event_id)
            VALUES (?)
            """,
            (event["eventId"],),
        )


if __name__ == "__main__":
    initialize_database()

    try:
        consume(
            ORDERS_TOPIC,
            "notification-orders",
            handle_order_event,
            stop_event,
        )
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()