import os
import json
import sqlite3
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, Field
from shared.security import current_user, admin

DB = os.getenv('CATALOG_DB', '/data/catalog.db')
INSTANCE = os.getenv('INSTANCE_ID', 'catalog')

@asynccontextmanager
async def lifespan(app):
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB) as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, price REAL)')
        db.executemany('INSERT OR IGNORE INTO products VALUES(?,?,?)', [(1, 'Gaming Laptop', 1500), (2, 'Mechanical Keyboard', 150)])
    yield

app = FastAPI(title='Catalog RBAC', lifespan=lifespan, root_path='/catalog')

class ProductCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    price: float = Field(gt=0, allow_inf_nan=False)

@app.get('/health')
def health():
    return {'status': 'healthy', 'instance': INSTANCE}

@app.get('/products', dependencies=[Depends(current_user)])
def products():
    with sqlite3.connect(DB) as db:
        return {'instance_id': INSTANCE, 'products': [dict(zip(('id', 'name', 'price'), row)) for row in db.execute('SELECT * FROM products ORDER BY id')]}

@app.post('/products', status_code=201, dependencies=[Depends(admin)])
def create(body: ProductCreate):
    with sqlite3.connect(DB) as db:
        cursor = db.execute('INSERT INTO products(name,price) VALUES(?,?)', (body.name, body.price))
        return {'instance_id': INSTANCE, 'product': {'id': cursor.lastrowid, **body.model_dump()}}

@app.put('/products/{product_id}', dependencies=[Depends(admin)])
def update(product_id: int, body: ProductCreate):
    with sqlite3.connect(DB) as db:
        if not db.execute('UPDATE products SET name=?,price=? WHERE id=?', (body.name, body.price, product_id)).rowcount:
            raise HTTPException(404, 'Product not found')
    return {'product': {'id': product_id, **body.model_dump()}}

@app.delete('/products/{product_id}', dependencies=[Depends(admin)])
def delete(product_id: int):
    with sqlite3.connect(DB) as db:
        if not db.execute('DELETE FROM products WHERE id=?', (product_id,)).rowcount:
            raise HTTPException(404, 'Product not found')
    return {'deleted': product_id}

from shared.telemetry import install
install(app)
