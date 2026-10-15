from datetime import datetime
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

app = FastAPI(
    title="BookStore — Order", version="3.0",
    docs_url="/api/orders/docs", openapi_url="/api/orders/openapi.json", redoc_url=None,
)

class OrderItem(BaseModel):
    productId: int = Field(description="ID книги")
    quantity: int = Field(gt=0, description="Количество экземпляров")

class Order(BaseModel):
    id: int
    userId: int = Field(description="ID пользователя, оформившего заказ")
    createdAt: datetime = Field(description="Дата и время создания заказа")
    items: list[OrderItem]

orders = [
    {"id": 101, "userId": 1, "createdAt": "2026-10-01T10:00:00Z",
     "items": [{"productId": 1, "quantity": 2}, {"productId": 2, "quantity": 1}]},
    {"id": 102, "userId": 1, "createdAt": "2026-10-02T11:30:00Z",
     "items": [{"productId": 1, "quantity": 1}, {"productId": 3, "quantity": 2}]},
    {"id": 103, "userId": 2, "createdAt": "2026-10-03T09:15:00Z",
     "items": [{"productId": 4, "quantity": 1}]},
]

@app.get("/api/orders", response_model=list[Order], tags=["Order"], summary="Заказы пользователя")
def get_orders(userId: int = Query(..., gt=0)):
    return [order for order in orders if order["userId"] == userId]
