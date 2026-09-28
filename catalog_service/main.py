from fastapi import FastAPI, HTTPException
import os

app = FastAPI()
products = []

instance_id = os.getenv("INSTANCE_NAME", "catalog-local")

@app.get("/products")
def get_products():
    return {"instance_id": instance_id, "products": products}

@app.post("/products")
def add_product(product: dict):
    product["id"] = len(products) + 1
    products.append(product)
    return product

@app.put("/products/{product_id}")
def update_product(product_id: int, updated_product: dict):
    for product in products:
        if product["id"] == product_id:
            product.update(updated_product)
            product["id"] == product_id
            return product
    raise HTTPException(status_code=404, detail="Product not found")

@app.delete("/products/{product_id}")
def delete_product(product_id: int):
    for product in products:
        if product["id"] == product_id:
            products.remove(product)
            return {"message": "Product deleted"}
    raise HTTPException(status_code=404, detail="Product not found")