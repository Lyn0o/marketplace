"""Shared event contract and at-least-once Kafka consumption."""
import asyncio
import logging
import math
import os
from datetime import datetime
from typing import Literal
from uuid import UUID
from aiokafka import AIOKafkaConsumer
from pydantic import BaseModel, Field, ValidationError

TOPIC = 'orders-events'


class Event(BaseModel):
    eventId: UUID
    orderId: str = Field(min_length=1)
    eventType: Literal['OrderCreated', 'OrderPaid', 'OrderCancelled']
    payload: dict
    timestamp: datetime
    version: int = Field(gt=0)


def decode_event(value):
    event = Event.model_validate_json(value).model_dump(mode='json')
    payload = event['payload']
    if event['eventType'] in ('OrderCreated', 'OrderPaid'):
        key = 'totalAmount' if event['eventType'] == 'OrderCreated' else 'paidAmount'
        amount = payload.get(key)
        if isinstance(amount, bool) or not isinstance(amount, (int, float)) or not math.isfinite(amount) or amount <= 0:
            raise ValueError('Invalid event amount')
    if event['eventType'] == 'OrderCreated' and (not payload.get('userId') or not payload.get('items')):
        raise ValueError('Missing order owner or items')
    if event['eventType'] == 'OrderPaid' and not payload.get('paymentMethod'):
        raise ValueError('Missing payment method')
    if event['eventType'] == 'OrderCancelled' and not payload.get('reason'):
        raise ValueError('Missing cancellation reason')
    return event


async def consume_events(group, project):
    while True:
        consumer = AIOKafkaConsumer(TOPIC,
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'),
            group_id=group, auto_offset_reset='earliest', enable_auto_commit=False)
        try:
            await consumer.start()
            async for message in consumer:
                try:
                    event = decode_event(message.value)
                except (ValidationError, ValueError, TypeError):
                    logging.warning('Skipping malformed event at %s:%s', message.partition, message.offset)
                else:
                    # Retry storage errors; commit only after a durable projection.
                    project(event)
                    logging.info('%s processed %s version %s', group, event['eventType'], event['version'])
                await consumer.commit()
        except Exception:
            logging.exception('%s consumer retry', group)
            await asyncio.sleep(3)
        finally:
            await consumer.stop()
