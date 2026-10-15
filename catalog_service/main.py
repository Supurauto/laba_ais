import logging
import os
import time
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn.error")
instance_id = os.getenv("INSTANCE_NAME", "catalog-local")
service_port = int(os.getenv("PORT", "8000"))
consul_url = os.getenv("CONSUL_URL", "http://localhost:8500")


def consul_request(path, **kwargs):
    with requests.Session() as session:
        session.trust_env = False
        return session.put(f"{consul_url}/v1/agent/service/{path}", timeout=3, **kwargs)


def register_in_consul():
    data = {"Name": "catalog", "ID": instance_id, "Address": instance_id, "Port": service_port}
    for _ in range(10):
        try:
            if consul_request("register", json=data).status_code == 200:
                logger.info("%s зарегистрирован в Consul", instance_id)
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    logger.warning("Не удалось зарегистрировать %s в Consul", instance_id)


@asynccontextmanager
async def lifespan(app):
    register_in_consul()
    yield
    try:
        consul_request(f"deregister/{instance_id}")
    except requests.RequestException:
        pass

app = FastAPI(
    title="BookStore — Catalog", version="3.0",
    description="Книги учебного магазина. Каждый экземпляр хранит данные отдельно в памяти.",
    docs_url="/api/catalog/docs", openapi_url="/api/catalog/openapi.json", redoc_url=None,
    lifespan=lifespan,
)

class ProductInput(BaseModel):
    name: str = Field(min_length=1, description="Название книги")
    price: float = Field(gt=0, allow_inf_nan=False, description="Цена в рублях")

class Product(ProductInput):
    id: int = Field(description="Уникальный ID книги")

class ProductPage(BaseModel):
    instance_id: str = Field(description="Экземпляр Catalog, обработавший запрос")
    total: int = Field(description="Число найденных книг")
    products: list[Product]


products = [
    {"id": 1, "name": "Мастер и Маргарита", "price": 650.0},
    {"id": 2, "name": "Преступление и наказание", "price": 550.0},
    {"id": 3, "name": "Маленький принц", "price": 350.0},
    {"id": 4, "name": "Евгений Онегин", "price": 450.0},
    {"id": 5, "name": "Война и мир", "price": 900.0},
]
next_product_id = 6


@app.get("/docs", include_in_schema=False)
def docs():
    return RedirectResponse("/api/catalog/docs")

@app.get("/products", include_in_schema=False)
@app.get("/api/catalog/products", response_model=ProductPage, tags=["Catalog"],
         summary="Список книг: пагинация или выборка по ID",
         responses={422: {"description": "Некорректные параметры"}})
def get_products(
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    ids: str | None = Query(None),
):
    if ids is not None:
        try:
            selected_ids = {int(value.strip()) for value in ids.split(",")}
            if any(value <= 0 for value in selected_ids):
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=422, detail="ids должен содержать положительные ID через запятую")
        selected = [product for product in products if product["id"] in selected_ids]
        logger.info("Пакетная загрузка книг: ids=%s", sorted(selected_ids))
        return {"instance_id": instance_id, "total": len(selected), "products": selected}
    return {"instance_id": instance_id, "total": len(products), "products": products[offset:offset + limit]}

@app.get("/products/{product_id}", include_in_schema=False)
@app.get("/api/catalog/products/{product_id}", response_model=Product, tags=["Catalog"],
         summary="Книга по ID", responses={404: {"description": "Книга не найдена"}})
def get_product(product_id: int):
    for product in products:
        if product["id"] == product_id:
            return product
    raise HTTPException(status_code=404, detail="Книга не найдена")

@app.post("/products", include_in_schema=False, status_code=201)
@app.post("/api/catalog/products", response_model=Product, status_code=201, tags=["Catalog"], summary="Добавить книгу")
async def create_product(product: ProductInput):
    global next_product_id
    item = {**product.model_dump(), "id": next_product_id}
    next_product_id += 1
    products.append(item)
    return item

@app.put("/products/{product_id}", include_in_schema=False)
@app.put("/api/catalog/products/{product_id}", response_model=Product, tags=["Catalog"],
         summary="Заменить название и цену книги", responses={404: {"description": "Книга не найдена"}})
async def update_product(product_id: int, product: ProductInput):
    item = get_product(product_id)
    item.update(product.model_dump())
    return item

@app.delete("/products/{product_id}", include_in_schema=False)
@app.delete("/api/catalog/products/{product_id}", tags=["Catalog"], summary="Удалить книгу",
            responses={404: {"description": "Книга не найдена"}})
async def delete_product(product_id: int):
    products.remove(get_product(product_id))
    return {"message": "Книга удалена"}