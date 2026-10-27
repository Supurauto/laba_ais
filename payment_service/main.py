import json
import logging
import os
import time
from uuid import NAMESPACE_URL, uuid5

import pika
from grpc_server import start_grpc_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Payment] %(message)s",
)
logger = logging.getLogger("payment")

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://lab:lab@rabbitmq:5672/",
)

EXCHANGE = "orders.events"
QUEUE = "payment.events"


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


def handle_order_created(channel, method, properties, body):
    try:
        event = json.loads(body)
        order_id = str(event["orderId"])
        simulate_failure = event.get(
            "simulatePaymentFailure",
            False,
        )

        if not isinstance(simulate_failure, bool):
            raise ValueError(
                "simulatePaymentFailure должен быть boolean"
            )

    except (ValueError, KeyError, TypeError):
        logger.exception("Некорректное сообщение")
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False,
        )
        return

    logger.info(
        "Получено OrderCreated: %s",
        json.dumps(event, ensure_ascii=False),
    )

    if simulate_failure:
        logger.info(
            "orderId=%s: имитация отказа оплаты",
            order_id,
        )

        publish_event(
            channel,
            "PaymentFailed",
            {
                "orderId": order_id,
                "reason": "Учебная имитация отказа оплаты",
            },
        )

    else:
        transaction_id = str(
            uuid5(
                NAMESPACE_URL,
                f"lab4-payment:{order_id}",
            )
        )

        logger.info(
            "orderId=%s: имитация успешной оплаты",
            order_id,
        )

        publish_event(
            channel,
            "PaymentCompleted",
            {
                "orderId": order_id,
                "transactionId": transaction_id,
            },
        )

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    while True:
        connection = None

        try:
            connection, channel = open_broker()
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=QUEUE,
                on_message_callback=handle_order_created,
                auto_ack=False,
            )

            logger.info("Ожидание OrderCreated")
            channel.start_consuming()

        except KeyboardInterrupt:
            break

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

        time.sleep(3)


if __name__ == "__main__":
    grpc_server = start_grpc_server()

    try:
        main()
    finally:
        grpc_server.stop(grace=5).wait()