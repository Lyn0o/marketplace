import os, json, asyncio
import requests, aio_pika
from contextlib import asynccontextmanager
from fastapi import FastAPI
from datetime import datetime, timezone

SERVICE_NAME = os.getenv("SERVICE_NAME", "notification-service")
INSTANCE_ID  = os.getenv("INSTANCE_ID", "notification-1")
SERVICE_PORT = int(os.getenv("PORT", 8006))
CONSUL_URL   = os.getenv("CONSUL_URL", "http://consul:8500")
SERVICE_HOST = os.getenv("SERVICE_HOST", "notification")
RABBIT_URL   = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq/")

notifications = []
seq = 0

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

async def ensure_connection():
    global rabbit_conn, rabbit_ch
    if rabbit_conn is None or rabbit_conn.is_closed:
        rabbit_conn = await aio_pika.connect_robust(RABBIT_URL)
        rabbit_ch = await rabbit_conn.channel()
        print("[Notification] RabbitMQ connected")

async def handle(message):
    global seq
    async with message.process():
        data = json.loads(message.body)
        seq += 1
        n = {
            "id": seq,
            "event": message.routing_key,
            "orderId": data.get("orderId"),
            "payload": data,
            "channel": "email",
            "sentAt": datetime.now(timezone.utc).isoformat(),
        }
        notifications.append(n)
        # ТРЕБУЕМАЯ ЧЕКЛИСТОМ СТРОКА:
        print(f"Отправлено уведомление клиенту | event={message.routing_key} | orderId={data.get('orderId')}")

async def start_consumer_with_retry(attempts: int = 15, delay: float = 2.0):
    for i in range(1, attempts + 1):
        try:
            await ensure_connection()
            ex = await rabbit_ch.declare_exchange("saga", aio_pika.ExchangeType.TOPIC, durable=True)

            q1 = await rabbit_ch.declare_queue("notify.payment.completed", durable=True)
            await q1.bind(ex, routing_key="PaymentCompleted")
            await q1.consume(handle)

            q2 = await rabbit_ch.declare_queue("notify.order.cancelled", durable=True)
            await q2.bind(ex, routing_key="OrderCancelled")
            await q2.consume(handle)

            print("[Notification] consumer started")
            return
        except Exception as e:
            print(f"[Notification] RabbitMQ attempt {i}/{attempts} failed: {e}")
            await asyncio.sleep(delay)
    print("[Notification] WARNING: consumer not started")

@asynccontextmanager
async def lifespan(app):
    register_to_consul()
    await start_consumer_with_retry()
    yield
    if rabbit_conn and not rabbit_conn.is_closed:
        await rabbit_conn.close()
    deregister_from_consul()

app = FastAPI(title="Notification Service", lifespan=lifespan, root_path="/notifications")

@app.get("/health")
def health(): return {"status": "healthy", "instance": INSTANCE_ID}

@app.get("/")
def list_notifications(): return {"notifications": notifications}