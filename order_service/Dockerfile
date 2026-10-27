FROM python:3.12-slim

WORKDIR /app

COPY order_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/payment.proto ./proto/payment.proto

RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=. \
    --grpc_python_out=. \
    ./proto/payment.proto

COPY order_service/main.py .
COPY order_service/payment_client.py .

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]