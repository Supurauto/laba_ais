import logging
import os

import grpc
import pybreaker

import payment_pb2
import payment_pb2_grpc


logger = logging.getLogger("order.payment_client")

PAYMENT_GRPC_ADDRESS = os.getenv(
    "PAYMENT_GRPC_ADDRESS",
    "payment:50051",
)

GRPC_TIMEOUT_SECONDS = 3
FAILURE_THRESHOLD = 3
RESET_TIMEOUT_SECONDS = 10


class BreakerListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        logger.info(
            "Circuit Breaker: %s -> %s",
            old_state.name.upper(),
            new_state.name.upper(),
        )


breaker = pybreaker.CircuitBreaker(
    fail_max=FAILURE_THRESHOLD,
    reset_timeout=RESET_TIMEOUT_SECONDS,
    listeners=[BreakerListener()],
    name="payment",
)

channel = grpc.insecure_channel(PAYMENT_GRPC_ADDRESS)
stub = payment_pb2_grpc.PaymentServiceStub(channel)


def call_payment_grpc(order_id, amount, simulate_failure):
    request = payment_pb2.PaymentRequest(
        order_id=order_id,
        amount=amount,
        simulate_failure=simulate_failure,
    )

    logger.info(
        "gRPC вызов Payment: orderId=%s, simulate_failure=%s",
        order_id,
        simulate_failure,
    )

    return stub.CheckPaymentStatus(
        request,
        timeout=GRPC_TIMEOUT_SECONDS,
    )


def payment_fallback(order_id):
    return {
        "order_id": order_id,
        "transaction_id": "",
        "status": "PROCESSING",
        "message": (
            "Платёжная система временно недоступна. "
            "Повторите проверку позже."
        ),
        "circuit_state": breaker.current_state.upper(),
    }


def check_payment(order_id, amount, simulate_failure):
    try:
        response = breaker.call(
            call_payment_grpc,
            order_id,
            amount,
            simulate_failure,
        )

    except pybreaker.CircuitBreakerError as error:
        logger.warning(
            "orderId=%s: Circuit Breaker возвращает fallback: %s",
            order_id,
            error,
        )
        return payment_fallback(order_id)

    except grpc.RpcError as error:
        logger.warning(
            "orderId=%s: ошибка gRPC %s, возвращаем fallback",
            order_id,
            error.code().name,
        )
        return payment_fallback(order_id)

    return {
        "order_id": order_id,
        "transaction_id": response.transaction_id,
        "status": response.status,
        "message": response.message,
        "circuit_state": breaker.current_state.upper(),
    }


def log_payment_client_settings():
    logger.info(
        "Circuit Breaker: %s; fail_max=%s; "
        "reset_timeout=%s секунд; gRPC timeout=%s секунд",
        breaker.current_state.upper(),
        FAILURE_THRESHOLD,
        RESET_TIMEOUT_SECONDS,
        GRPC_TIMEOUT_SECONDS,
    )


def close_payment_channel():
    channel.close()