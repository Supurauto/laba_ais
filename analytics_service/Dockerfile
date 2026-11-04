FROM python:3.12-slim

WORKDIR /app

COPY analytics_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared.py .
COPY analytics_service/main.py .

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]