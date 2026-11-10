import os, json, uuid, asyncio
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import List, Optional
from aiokafka import AIOKafkaProducer

SERVICE_NAME = os.getenv("SERVICE_NAME", "order-service")
INSTANCE_ID  = os.getenv("INSTANCE_ID", "order-1")
SERVICE_PORT = int(os.getenv("PORT", 8003))
CONSUL_URL   = os.getenv("CONSUL_URL", "http://consul:8500")
SERVICE_HOST = os.getenv("SERVICE_HOST", "order")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

event_store = []
read_model = {}

class OrderItem(BaseModel):
    productId: str
    quantity: int

class OrderCreate(BaseModel):
    userId: str
    items: List[OrderItem]
    totalAmount: float

def rehydrate(order_id: str) -> Optional[dict]:
    events = [e for e in event_store if e["order_id"] == order_id]
    if not events: return None
    
    state = {"order_id": order_id, "status": "CREATED", "items": [], "totalAmount": 0.0, "version": 0}
    for event in sorted(events, key=lambda x: x["version"]):
        if event["event_type"] == "OrderCreated":
            state["status"] = "CREATED"
            state["items"] = event["payload"].get("items", [])
            state["totalAmount"] = event["payload"].get("totalAmount", 0.0)
        elif event["event_type"] == "OrderPaid":
            state["status"] = "PAID"
            state["transaction_id"] = event["payload"].get("transaction_id")
        elif event["event_type"] == "OrderCancelled":
            state["status"] = "CANCELLED"
            state["reason"] = event["payload"].get("reason", "Unknown")
        state["version"] = event["version"]
    return state

def update_read_model(event: dict):
    state = rehydrate(event["order_id"])
    if state: read_model[event["order_id"]] = state

kafka_producer: Optional[AIOKafkaProducer] = None

async def get_kafka_producer() -> AIOKafkaProducer:
    global kafka_producer
    if kafka_producer is None:
        print("[Kafka] Initializing producer (Lazy)...")
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await kafka_producer.start()
        print("[Kafka] Producer started successfully!")
    return kafka_producer

async def publish_event_to_kafka(event: dict):
    producer = await get_kafka_producer()
    print(f"[Kafka] Publishing: {event['event_type']} for {event['order_id']}")
    await producer.send_and_wait("orders-events", event)

def register_to_consul():
    try:
        requests.put(f"{CONSUL_URL}/v1/agent/service/register", json={
            "ID": INSTANCE_ID, "Name": SERVICE_NAME, "Address": SERVICE_HOST, "Port": SERVICE_PORT,
            "Check": {"HTTP": f"http://{SERVICE_HOST}:{SERVICE_PORT}/health", "Interval": "10s"}
        }, timeout=3)
    except Exception: pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    register_to_consul()
    yield
    if kafka_producer is not None: await kafka_producer.stop()

# ВАЖНО: НЕТ root_path="/orders" !!!
app = FastAPI(title="Order Service (CQRS + Event Sourcing)", lifespan=lifespan)

# --- READ SIDE (Сначала конкретные пути!) ---
@app.get("/health")
def health(): return {"status": "healthy", "instance": INSTANCE_ID}

@app.get("/")
async def list_orders(): return {"orders": list(read_model.values())}

@app.get("/_debug/event-store")
async def debug_event_store(): return {"events": event_store}

@app.get("/{order_id}") # ВСЕГДА ПОСЛЕДНИМ!
async def get_order(order_id: str):
    if order_id not in read_model:
        raise HTTPException(status_code=404, detail="Order not found in read model")
    return read_model[order_id]

# --- WRITE SIDE ---
@app.post("/", status_code=201)
async def create_order(command: OrderCreate):
    order_id = str(uuid.uuid4())
    event = {
        "event_id": str(uuid.uuid4()), "order_id": order_id, "event_type": "OrderCreated",
        "payload": {"userId": command.userId, "items": [item.model_dump() for item in command.items], "totalAmount": command.totalAmount},
        "timestamp": datetime.now(timezone.utc).isoformat(), "version": 1
    }
    event_store.append(event)
    update_read_model(event)
    await publish_event_to_kafka(event)
    return {"message": "Order created", "order_id": order_id, "status": "CREATED"}

@app.post("/{order_id}/pay", status_code=200)
async def pay_order(order_id: str):
    state = rehydrate(order_id)
    if not state: raise HTTPException(status_code=404, detail="Order not found")
    if state["status"] == "PAID": raise HTTPException(status_code=400, detail="Order is already paid")
    if state["status"] == "CANCELLED": raise HTTPException(status_code=400, detail="Cannot pay a cancelled order")
    
    event = {
        "event_id": str(uuid.uuid4()), "order_id": order_id, "event_type": "OrderPaid",
        "payload": {"transaction_id": str(uuid.uuid4()), "amount": state["totalAmount"]},
        "timestamp": datetime.now(timezone.utc).isoformat(), "version": state["version"] + 1
    }
    event_store.append(event)
    update_read_model(event)
    await publish_event_to_kafka(event)
    return {"message": "Order paid successfully", "status": "PAID"}

@app.post("/{order_id}/cancel", status_code=200)
async def cancel_order(order_id: str, reason: str = "Cancelled by user"):
    state = rehydrate(order_id)
    if not state: raise HTTPException(status_code=404, detail="Order not found")
    if state["status"] == "CANCELLED": raise HTTPException(status_code=400, detail="Order is already cancelled")
    if state["status"] == "PAID": raise HTTPException(status_code=400, detail="Cannot cancel already paid order")

    event = {
        "event_id": str(uuid.uuid4()), "order_id": order_id, "event_type": "OrderCancelled",
        "payload": {"reason": reason},
        "timestamp": datetime.now(timezone.utc).isoformat(), "version": state["version"] + 1
    }
    event_store.append(event)
    update_read_model(event)
    await publish_event_to_kafka(event)
    return {"message": "Order cancelled", "status": "CANCELLED"}