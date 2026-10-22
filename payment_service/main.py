import os, json, uuid, asyncio, random
import requests, aio_pika
from contextlib import asynccontextmanager
from fastapi import FastAPI
from datetime import datetime, timezone
import grpc
from concurrent import futures
import payment_pb2
import payment_pb2_grpc

SERVICE_NAME = os.getenv("SERVICE_NAME", "payment-service")
INSTANCE_ID  = os.getenv("INSTANCE_ID", "payment-1")
SERVICE_PORT = int(os.getenv("PORT", 8004))
CONSUL_URL   = os.getenv("CONSUL_URL", "http://consul:8500")
SERVICE_HOST = os.getenv("SERVICE_HOST", "payment")
RABBIT_URL   = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq/")

prepared, committed = {}, {}
rabbit_conn, rabbit_ch, rabbit_ex = None, None, None
grpc_server = None

class PaymentServiceServicer(payment_pb2_grpc.PaymentServiceServicer):
    def CheckPaymentStatus(self, request, context):
        print(f"[gRPC] Request for order {request.order_id}. simulate_failure={request.simulate_failure}", flush=True)
        
        if request.simulate_failure:
            print("[gRPC] SIMULATING FAILURE (UNAVAILABLE)", flush=True)
            context.abort(grpc.StatusCode.UNAVAILABLE, "Payment unavailable")
        
        if request.amount < 0:
            import time
            print("[gRPC] SIMULATING TIMEOUT", flush=True)
            time.sleep(3)

        return payment_pb2.PaymentResponse(
            transaction_id=str(uuid.uuid4()),
            status="SUCCESS",
            message="Payment checked successfully"
        )

def start_grpc_server():
    global grpc_server
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentServiceServicer(), grpc_server)
    grpc_server.add_insecure_port('[::]:50051')
    grpc_server.start()
    print("[gRPC] Server started on port 50051", flush=True)

async def ensure_connection():
    global rabbit_conn, rabbit_ch, rabbit_ex
    if rabbit_conn is None or rabbit_conn.is_closed:
        rabbit_conn = await aio_pika.connect_robust(RABBIT_URL)
        rabbit_ch = await rabbit_conn.channel()
        rabbit_ex = await rabbit_ch.declare_exchange("saga", aio_pika.ExchangeType.TOPIC, durable=True)
        print("[Payment] RabbitMQ connected", flush=True)

async def publish(rk, payload):
    await ensure_connection()
    print(f"[Payment PUBLISH] {rk}: {payload}", flush=True)
    await rabbit_ex.publish(aio_pika.Message(body=json.dumps(payload).encode()), routing_key=rk)

async def on_order_created(message):
    async with message.process(requeue=False) as msg_ctx:
        data = json.loads(message.body)
        oid, amount = data["orderId"], float(data.get("totalAmount", 0))

        if amount <= 0:
            print(f"[Payment 2PC] Order {oid}: invalid amount {amount}. Sending to DLQ!", flush=True)
            raise Exception("Invalid amount")

        if random.random() < 0.3:
            print(f"[Payment 2PC] {oid}: bank declined -> NO", flush=True)
            await publish("PaymentFailed", {"orderId": oid, "reason": "bank declined"})
            return

        tx = str(uuid.uuid4())
        prepared[oid] = {"transactionId": tx, "amount": amount}
        print(f"[Payment 2PC] {oid}: RESERVED {amount} tx={tx} -> YES", flush=True)
        await publish("PreparedPayment", {"orderId": oid, "transactionId": tx, "amount": amount})

async def on_commit(message):
    async with message.process():
        oid = json.loads(message.body)["orderId"]
        tx = prepared.pop(oid, None)
        if not tx: return
        committed[oid] = tx
        print(f"[Payment 2PC] {oid}: COMMIT {tx['transactionId']}", flush=True)
        await publish("PaymentCompleted", {"orderId": oid, "transactionId": tx["transactionId"]})

async def on_abort(message):
    async with message.process():
        oid = json.loads(message.body)["orderId"]
        prepared.pop(oid, None)
        print(f"[Payment 2PC] {oid}: ABORT", flush=True)
        await publish("PaymentFailed", {"orderId": oid, "reason": "aborted"})

async def start_consumer_with_retry(attempts: int = 15, delay: float = 2.0):
    for i in range(1, attempts + 1):
        try:
            await ensure_connection()
            dlx_ex = await rabbit_ch.declare_exchange("dlx_exchange", aio_pika.ExchangeType.DIRECT, durable=True)
            dlq = await rabbit_ch.declare_queue("payment.order.created.dlq", durable=True)
            await dlq.bind(dlx_ex, routing_key="dlq")

            q1 = await rabbit_ch.declare_queue(
                "payment.order.created", durable=True,
                arguments={"x-dead-letter-exchange": "dlx_exchange", "x-dead-letter-routing-key": "dlq"}
            )
            await q1.bind(rabbit_ex, routing_key="OrderCreated")
            await q1.consume(on_order_created)

            q2 = await rabbit_ch.declare_queue("payment.commit", durable=True)
            await q2.bind(rabbit_ex, routing_key="CommitPayment")
            await q2.consume(on_commit)

            q3 = await rabbit_ch.declare_queue("payment.abort", durable=True)
            await q3.bind(rabbit_ex, routing_key="AbortPayment")
            await q3.consume(on_abort)
            
            print("[Payment] Consumer & DLQ started successfully", flush=True)
            return
        except Exception as e:
            print(f"[Payment] RabbitMQ connect attempt {i}/{attempts} failed: {e}", flush=True)
            await asyncio.sleep(delay)

@asynccontextmanager
async def lifespan(app):
    global grpc_server
    start_grpc_server()
    await start_consumer_with_retry()
    yield
    if grpc_server:
        grpc_server.stop(0)
    if rabbit_conn and not rabbit_conn.is_closed: 
        await rabbit_conn.close()

app = FastAPI(title="Payment Service", lifespan=lifespan, root_path="/payments")

@app.get("/health")
def health(): return {"status": "healthy"}