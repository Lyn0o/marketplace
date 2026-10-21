import os, json, uuid, asyncio
import requests
import aio_pika
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import List

# Импорт клиента gRPC
from grpc_client import safe_check_payment

SERVICE_NAME = os.getenv("SERVICE_NAME", "order-service")
INSTANCE_ID  = os.getenv("INSTANCE_ID", "order-1")
SERVICE_PORT = int(os.getenv("PORT", 8003))
CONSUL_URL   = os.getenv("CONSUL_URL", "http://consul:8500")
SERVICE_HOST = os.getenv("SERVICE_HOST", "order")
RABBIT_URL   = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq/")

orders_db = {}

class OrderItem(BaseModel):
    productId: str
    quantity: int

class OrderCreate(BaseModel):
    userId: str
    items: List[OrderItem]
    totalAmount: float

def register_to_consul():
    payload = {
        "ID": INSTANCE_ID, "Name": SERVICE_NAME,
        "Address": SERVICE_HOST, "Port": SERVICE_PORT,
        "Check": {"HTTP": f"http://{SERVICE_HOST}:{SERVICE_PORT}/health", "Interval": "10s"}
    }
    try:
        requests.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload, timeout=3)
    except Exception as e:
        print("[Consul] register error:", e)

def deregister_from_consul():
    try:
        requests.put(f"{CONSUL_URL}/v1/agent/service/deregister/{INSTANCE_ID}", timeout=3)
    except Exception as e:
        print("[Consul] deregister error:", e)

rabbit_conn = None
rabbit_ch = None
rabbit_ex = None

async def ensure_connection():
    """Гарантирует живое соединение с RabbitMQ. При необходимости — реконнект."""
    global rabbit_conn, rabbit_ch, rabbit_ex
    if rabbit_conn is None or rabbit_conn.is_closed:
        rabbit_conn = await aio_pika.connect_robust(RABBIT_URL)
        rabbit_ch = await rabbit_conn.channel()
        rabbit_ex = await rabbit_ch.declare_exchange(
            "saga", aio_pika.ExchangeType.TOPIC, durable=True
        )
        print("[Order] RabbitMQ connected")

async def publish(rk, payload):
    await ensure_connection()
    print(f"[Order PUBLISH] {rk}: {payload}")
    await rabbit_ex.publish(
        aio_pika.Message(body=json.dumps(payload).encode()),
        routing_key=rk,
    )

# ---------- 2PC ФАЗА 2: решение координатора ----------
async def on_payment_prepared(message):
    async with message.process():
        data = json.loads(message.body)
        oid = data["orderId"]
        order = orders_db.get(oid)
        if not order or order["status"] != "PENDING":
            return
        if order["totalAmount"] <= 0:
            print(f"[Order 2PC] ABORT {oid}: некорректная сумма")
            await publish("AbortPayment", {"orderId": oid, "reason": "invalid amount"})
            return
        print(f"[Order 2PC] COMMIT {oid}")
        await publish("CommitPayment", {"orderId": oid})

async def on_payment_completed(message):
    async with message.process():
        data = json.loads(message.body)
        oid = data["orderId"]
        order = orders_db.get(oid)
        if not order or order["status"] == "CONFIRMED":
            return  # идемпотентность
        order["status"] = "CONFIRMED"
        order["transactionId"] = data.get("transactionId")
        print(f"[Order] {oid} → CONFIRMED")

async def on_payment_failed(message):
    """КОМПЕНСАЦИЯ: заказ не должен навсегда остаться в PENDING."""
    async with message.process():
        data = json.loads(message.body)
        oid = data["orderId"]
        order = orders_db.get(oid)
        if not order or order["status"] == "CANCELLED":
            return
        order["status"] = "CANCELLED"
        order["reason"] = data.get("reason", "payment failed")
        print(f"[Order] {oid} → CANCELLED (компенсация): {order['reason']}")
        await publish("OrderCancelled", {
            "orderId": oid,
            "userId": order["userId"],
            "reason": order["reason"],
        })

async def start_consumer_with_retry(attempts: int = 15, delay: float = 2.0):
    """Подключение к RabbitMQ с ретраями — брокер может стартовать дольше сервисов."""
    for i in range(1, attempts + 1):
        try:
            await ensure_connection()
            q_prep = await rabbit_ch.declare_queue("order.payment.prepared", durable=True)
            await q_prep.bind(rabbit_ex, routing_key="PreparedPayment")
            await q_prep.consume(on_payment_prepared)

            q_ok = await rabbit_ch.declare_queue("order.payment.completed", durable=True)
            await q_ok.bind(rabbit_ex, routing_key="PaymentCompleted")
            await q_ok.consume(on_payment_completed)

            q_fail = await rabbit_ch.declare_queue("order.payment.failed", durable=True)
            await q_fail.bind(rabbit_ex, routing_key="PaymentFailed")
            await q_fail.consume(on_payment_failed)

            print("[Order] consumer started, queues bound")
            return
        except Exception as e:
            print(f"[Order] RabbitMQ connect attempt {i}/{attempts} failed: {e}")
            await asyncio.sleep(delay)
    print("[Order] WARNING: consumer not started, but service continues")

@asynccontextmanager
async def lifespan(app):
    register_to_consul()
    await start_consumer_with_retry()
    yield
    if rabbit_conn and not rabbit_conn.is_closed:
        await rabbit_conn.close()
    deregister_from_consul()

app = FastAPI(title="Order Service", lifespan=lifespan, root_path="/orders")

@app.get("/health")
def health(): return {"status": "healthy", "instance": INSTANCE_ID}

@app.get("/")
def list_orders(): return {"orders": list(orders_db.values())}

@app.get("/{order_id}")
def get_order(order_id: str):
    if order_id not in orders_db:
        raise HTTPException(404, "Order not found")
    return orders_db[order_id]

@app.post("/", status_code=201)
async def create_order(order: OrderCreate):
    """КОД ПУБЛИКАЦИИ OrderCreated — старт саги и 2PC фазы 1."""
    oid = str(uuid.uuid4())
    new_order = {
        "orderId": oid,
        "userId": order.userId,
        "items": [i.dict() for i in order.items],
        "totalAmount": order.totalAmount,
        "status": "PENDING",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    orders_db[oid] = new_order
    await publish("OrderCreated", new_order)
    return new_order

# --- ЭНДПОИНТ ДЛЯ ЛР5 (gRPC + Circuit Breaker) ---
@app.get("/check-payment/{order_id}")
def check_payment(order_id: str, amount: float = 100.0, simulate_failure: bool = False):
    """
    Эндпоинт для тестирования gRPC-вызова и Circuit Breaker.
    Пример: /check-payment/123?simulate_failure=true
    """
    result = safe_check_payment(order_id, amount, simulate_failure)
    return result