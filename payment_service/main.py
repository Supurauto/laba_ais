import logging
import threading
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from grpc_server import start_grpc_server
from shared import (
    ORDERS_TOPIC,
    PAYMENTS_TOPIC,
    consume,
    publish,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Payment] %(message)s",
)
logger = logging.getLogger("payment")

stop_event = threading.Event()


def handle_order_event(event):
    if event["eventType"] != "OrderCreated":
        return

    payload = event["payload"]

    if not payload.get("autoPay", True):
        logger.info(
            "Автооплата выключена: orderId=%s",
            event["orderId"],
        )
        return

    order_id = event["orderId"]

    if payload.get("simulatePaymentFailure", False):
        event_type = "PaymentFailed"
        result_payload = {
            "reason": "Учебная имитация отказа оплаты",
        }

    else:
        event_type = "PaymentCompleted"
        result_payload = {
            "transactionId": str(
                uuid5(
                    NAMESPACE_URL,
                    f"lab4-payment:{order_id}",
                )
            ),
        }

    result = {
        "eventId": str(
            uuid5(
                NAMESPACE_URL,
                f"payment-result:{event['eventId']}",
            )
        ),
        "orderId": order_id,
        "eventType": event_type,
        "payload": result_payload,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sourceEventId": event["eventId"],
    }

    publish(PAYMENTS_TOPIC, result)

    logger.info(
        "Результат оплаты: orderId=%s, event=%s",
        order_id,
        event_type,
    )


if __name__ == "__main__":
    grpc_server = start_grpc_server()

    try:
        consume(
            ORDERS_TOPIC,
            "payment-orders",
            handle_order_event,
            stop_event,
        )
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        grpc_server.stop(grace=5).wait()