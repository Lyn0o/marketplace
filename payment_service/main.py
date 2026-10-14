import os, json, uuid, asyncio, random
import requests, aio_pika
from contextlib import asynccontextmanager
from fastapi import FastAPI
from datetime import datetime, timezone

SERVICE_NAME = os.getenv("SERVICE_NAME", "payment-service")
INSTANCE_ID  = os.getenv("INSTANCE_ID", "payment-1")
SERVICE_PORT = int(os.getenv("PORT", 8004))
CONSUL_URL   = os.getenv("CONSUL_URL", "http://consul:8500")
SERVICE_HOST = os.getenv("SERVICE_HOST", "payment")
RABBIT_URL   = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq/")
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.3"))

prepared = {}
committed = {}

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
    global rabbit_conn, rabbit_ch, rabbit_ex
    if rabbit_conn is None or rabbit_conn.is_closed:
        rabbit_conn = await aio_pika.connect_robust(RABBIT_URL)
        rabbit_ch = await rabbit_conn.channel()
        rabbit_ex = await rabbit_ch.declare_exchange(
            "saga", aio_pika.ExchangeType.TOPIC, durable=True
        )
        print("[Payment] RabbitMQ connected")

async def publish(rk, payload):
    await ensure_connection()
    print(f"[Payment PUBLISH] {rk}: {payload}")
    await rabbit_ex.publish(
        aio_pika.Message(body=json.dumps(payload).encode()),
        routing_key=rk,
    )

# ---------- 2PC ФАЗА 1 — PREPARE ----------
async def on_order_created(message):
    """OrderCreated → проверяем, РЕЗЕРВИРУЕМ (не списываем), голосуем."""
    async with message.process():
        data = json.loads(message.body)
        oid = data["orderId"]
        amount = float(data.get("totalAmount", 0))

        if amount <= 0:
            print(f"[Payment 2PC] {oid}: сумма {amount} некорректна → NO")
            await publish("PaymentFailed", {"orderId": oid, "reason": "invalid amount"})
            return

        if random.random() < FAILURE_RATE:
            print(f"[Payment 2PC] {oid}: банк отклонил → NO")
            await publish("PaymentFailed", {"orderId": oid, "reason": "bank declined"})
            return

        tx = str(uuid.uuid4())
        prepared[oid] = {
            "transactionId": tx,
            "amount": amount,
            "preparedAt": datetime.now(timezone.utc).isoformat(),
        }
        print(f"[Payment 2PC] {oid}: RESERVED {amount} tx={tx} → YES")
        await publish("PreparedPayment", {"orderId": oid, "transactionId": tx, "amount": amount})

# ---------- 2PC ФАЗА 2 — COMMIT ----------
async def on_commit(message):
    async with message.process():
        data = json.loads(message.body)
        oid = data["orderId"]
        tx = prepared.pop(oid, None)
        if not tx:
            print(f"[Payment 2PC] COMMIT {oid}, резерва нет — игнор")
            return
        committed[oid] = {**tx, "committedAt": datetime.now(timezone.utc).isoformat()}
        print(f"[Payment 2PC] {oid}: COMMIT {tx['transactionId']}")
        await publish("PaymentCompleted", {"orderId": oid, "transactionId": tx["transactionId"]})

# ---------- 2PC ФАЗА 2 — ABORT ----------
async def on_abort(message):
    async with message.process():
        data = json.loads(message.body)
        oid = data["orderId"]
        prepared.pop(oid, None)
        reason = data.get("reason", "aborted by coordinator")
        print(f"[Payment 2PC] {oid}: ABORT резерва ({reason})")
        await publish("PaymentFailed", {"orderId": oid, "reason": reason})

async def start_consumer_with_retry(attempts: int = 15, delay: float = 2.0):
    for i in range(1, attempts + 1):
        try:
            await ensure_connection()
            q1 = await rabbit_ch.declare_queue("payment.order.created", durable=True)
            await q1.bind(rabbit_ex, routing_key="OrderCreated")
            await q1.consume(on_order_created)

            q2 = await rabbit_ch.declare_queue("payment.commit", durable=True)
            await q2.bind(rabbit_ex, routing_key="CommitPayment")
            await q2.consume(on_commit)

            q3 = await rabbit_ch.declare_queue("payment.abort", durable=True)
            await q3.bind(rabbit_ex, routing_key="AbortPayment")
            await q3.consume(on_abort)

            print("[Payment] consumer started")
            return
        except Exception as e:
            print(f"[Payment] RabbitMQ attempt {i}/{attempts} failed: {e}")
            await asyncio.sleep(delay)
    print("[Payment] WARNING: consumer not started")

@asynccontextmanager
async def lifespan(app):
    register_to_consul()
    await start_consumer_with_retry()
    yield
    if rabbit_conn and not rabbit_conn.is_closed:
        await rabbit_conn.close()
    deregister_from_consul()

app = FastAPI(title="Payment Service", lifespan=lifespan, root_path="/payments")

@app.get("/health")
def health(): return {"status": "healthy", "instance": INSTANCE_ID}

@app.get("/")
def list_payments(): return {"prepared": prepared, "committed": committed}

@app.get("/{order_id}")
def get_payment(order_id: str):
    return {"prepared": prepared.get(order_id), "committed": committed.get(order_id)}