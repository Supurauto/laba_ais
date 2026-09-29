from fastapi import FastAPI, HTTPException
import os
import requests
import time

app = FastAPI()

products = []

instance_id = os.getenv("INSTANCE_NAME", "catalog-local")
service_port = int(os.getenv("PORT", "8000"))
consul_url = os.getenv("CONSUL_URL", "http://localhost:8500")


def register_in_consul():
    data = {
        "Name": "catalog",
        "ID": instance_id,
        "Address": instance_id,
        "Port": service_port
    }

    for _ in range(10):
        try:
            response = requests.put(
                f"{consul_url}/v1/agent/service/register",
                json=data,
                timeout=3
            )

            if response.status_code == 200:
                print(f"{instance_id} зарегистрирован в Consul")
                return

        except requests.exceptions.RequestException:
            pass

        time.sleep(2)

    print(f"Не удалось зарегистрировать {instance_id} в Consul")


def deregister_from_consul():
    try:
        requests.put(
            f"{consul_url}/v1/agent/service/deregister/{instance_id}",
            timeout=3
        )
    except requests.exceptions.RequestException:
        pass


@app.on_event("startup")
def startup_event():
    register_in_consul()


@app.on_event("shutdown")
def shutdown_event():
    deregister_from_consul()


@app.get("/products")
def get_products():
    return {
        "instance_id": instance_id,
        "products": products
    }


@app.post("/products")
def create_product(product: dict):
    product["id"] = len(products) + 1
    products.append(product)
    return product


@app.put("/products/{product_id}")
def update_product(product_id: int, product: dict):
    for item in products:
        if item["id"] == product_id:
            item.update(product)
            item["id"] = product_id
            return item

    raise HTTPException(status_code=404, detail="Товар не найден")


@app.delete("/products/{product_id}")
def delete_product(product_id: int):
    for item in products:
        if item["id"] == product_id:
            products.remove(item)
            return {"message": "Товар удален"}

    raise HTTPException(status_code=404, detail="Товар не найден")