import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Depends
from shared.security import admin
from order_service.store import EventStore
from order_service import commands, queries
from shared.events import consume_events, TOPIC

logging.basicConfig(level=logging.INFO)
store = EventStore(os.getenv('ORDER_DB', '/data/orders.db'))


async def publish_events():
    while True:
        producer = AIOKafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'), acks='all')
        try:
            await producer.start()
            while True:
                for event, kafka_sent, _ in store.pending():
                    if not kafka_sent:
                        await producer.send_and_wait(TOPIC, json.dumps(event).encode(), key=event['orderId'].encode())
                        store.mark(event['eventId'], 'kafka')
                    # Lab 6 controls payment explicitly, without the lab 5 auto-payment saga.
                    store.mark(event['eventId'], 'rabbit')
                await asyncio.sleep(0.25)
        except Exception:
            logging.exception('Kafka unavailable; outbox will retry')
            await asyncio.sleep(3)
        finally:
            await producer.stop()


@asynccontextmanager
async def lifespan(app):
    store.rebuild()
    tasks = [asyncio.create_task(publish_events()),
             asyncio.create_task(consume_events('orders-read-model-group', store.project))]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task


app = FastAPI(title='Order CQRS and Event Sourcing', lifespan=lifespan, root_path='/orders')


@app.get('/health')
def health():
    return {'status': 'healthy', 'pendingEvents': len(store.pending())}


@app.get('/check-payment/{order_id}', dependencies=[Depends(admin)])
def check_payment(order_id: str, amount: float = 100, simulate_failure: bool = False):
    from order_service.grpc_client import safe_check_payment
    return safe_check_payment(order_id, amount, simulate_failure)


app.include_router(commands.router(store))
app.include_router(queries.router(store))
