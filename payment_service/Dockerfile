FROM python:3.12-slim

WORKDIR /app

COPY payment_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/payment.proto ./proto/payment.proto

RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=. \
    --grpc_python_out=. \
    ./proto/payment.proto

COPY payment_service/main.py .
COPY payment_service/grpc_server.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]