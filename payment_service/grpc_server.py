import logging
from concurrent import futures
from uuid import NAMESPACE_URL, uuid5

import grpc

import payment_pb2
import payment_pb2_grpc


logger = logging.getLogger("payment.grpc")


class PaymentGrpcService(
    payment_pb2_grpc.PaymentServiceServicer
):
    def CheckPaymentStatus(self, request, context):
        logger.info(
            "Получен gRPC CheckPaymentStatus: "
            "orderId=%s, amount=%s, simulate_failure=%s",
            request.order_id,
            request.amount,
            request.simulate_failure,
        )

        if request.simulate_failure:
            logger.warning(
                "orderId=%s: имитация gRPC UNAVAILABLE",
                request.order_id,
            )

            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "Учебная имитация недоступности Payment",
            )

        transaction_id = str(
            uuid5(
                NAMESPACE_URL,
                f"lab5-payment:{request.order_id}",
            )
        )

        logger.info(
            "orderId=%s: gRPC ответ SUCCESS",
            request.order_id,
        )

        return payment_pb2.PaymentResponse(
            transaction_id=transaction_id,
            status="SUCCESS",
            message="Учебная проверка оплаты выполнена успешно",
        )


def start_grpc_server():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    payment_pb2_grpc.add_PaymentServiceServicer_to_server(
        PaymentGrpcService(),
        server,
    )

    port = server.add_insecure_port("0.0.0.0:50051")

    if port == 0:
        raise RuntimeError(
            "Не удалось открыть gRPC-порт 50051"
        )

    server.start()
    logger.info("gRPC Payment запущен на порту 50051")

    return server