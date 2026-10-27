import json
import logging
import os
import time

import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Notification] %(message)s",
)
logger = logging.getLogger("notification")

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://lab:lab@rabbitmq:5672/",
)

EXCHANGE = "orders.events"
QUEUE = "notification.events"


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

    channel.queue_declare(
        queue=QUEUE,
        durable=True,
    )

    for event_name in ("PaymentCompleted", "OrderCancelled"):
        channel.queue_bind(
            exchange=EXCHANGE,
            queue=QUEUE,
            routing_key=event_name,
        )

    return connection, channel


def handle_notification(channel, method, properties, body):
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

    if event_name == "PaymentCompleted":
        logger.info(
            "Отправлено уведомление клиенту: "
            "заказ %s оплачен",
            order_id,
        )

    elif event_name == "OrderCancelled":
        logger.info(
            "Отправлено уведомление клиенту: "
            "заказ %s отменён. Причина: %s",
            order_id,
            event.get("reason", "Не указана"),
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
                on_message_callback=handle_notification,
                auto_ack=False,
            )

            logger.info(
                "Ожидание PaymentCompleted и OrderCancelled"
            )
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
    main()