import os
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional

INSTANCE_ID = os.getenv("INSTANCE_ID", "catalog-unknown")
SERVICE_PORT = int(os.getenv("PORT", 8002))
CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
SERVICE_HOST = os.getenv("SERVICE_HOST", "localhost")

products_db = {
    1: {"id": 1, "name": "Gaming Laptop", "price": 1500},
    2: {"id": 2, "name": "Mechanical Keyboard", "price": 150}
}

class ProductCreate(BaseModel):
    name: str
    price: float

def register_to_consul():
    payload = {
        "ID": INSTANCE_ID,
        "Name": "catalog-service",
        "Address": SERVICE_HOST,
        "Port": SERVICE_PORT,
        "Check": {
            "HTTP": f"http://{SERVICE_HOST}:{SERVICE_PORT}/health",
            "Interval": "10s"
        }
    }
    try:
        response = requests.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload)
        print(f"[Consul] Регистрация {INSTANCE_ID} успешна: {response.status_code}")
    except Exception as e:
        print(f"[Consul] Ошибка регистрации {INSTANCE_ID}: {e}")

def deregister_from_consul():
    try:
        response = requests.put(f"{CONSUL_URL}/v1/agent/service/deregister/{INSTANCE_ID}")
        print(f"[Consul] Дерегистрация {INSTANCE_ID} успешна: {response.status_code}")
    except Exception as e:
        print(f"[Consul] Ошибка дерегистрации {INSTANCE_ID}: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    register_to_consul()
    yield
    deregister_from_consul()

app = FastAPI(title="Catalog Service", lifespan=lifespan, root_path="/catalog")

@app.get("/health")
def health_check():
    return {"status": "healthy", "instance": INSTANCE_ID}

@app.get("/products")
def get_products():
    return {"instance_id": INSTANCE_ID, "products": list(products_db.values())}

@app.post("/products")
def create_product(product: ProductCreate):
    new_id = max(products_db.keys(), default=0) + 1
    products_db[new_id] = {"id": new_id, "name": product.name, "price": product.price}
    return {"instance_id": INSTANCE_ID, "product": products_db[new_id]}

@app.put("/products/{product_id}")
def update_product(product_id: int, product: ProductCreate):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    products_db[product_id].update({"name": product.name, "price": product.price})
    return {"instance_id": INSTANCE_ID, "product": products_db[product_id]}

@app.delete("/products/{product_id}")
def delete_product(product_id: int):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    del products_db[product_id]
    return {"instance_id": INSTANCE_ID, "message": "Product deleted"}