import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from shared.events import TOPIC
from shared.telemetry import tracer, propagation, payments, log

COMPLETED = 'payment-completed'


async def payment_stream(service, handle):
    topic = TOPIC if service=='payment' else COMPLETED
    while True:
        consumer = AIOKafkaConsumer(topic, bootstrap_servers=os.environ['KAFKA_BOOTSTRAP'],
            group_id=f'{service}-payment-v1', enable_auto_commit=False, auto_offset_reset='earliest')
        producer = AIOKafkaProducer(bootstrap_servers=os.environ['KAFKA_BOOTSTRAP'],acks='all')
        try:
            await producer.start()
            await consumer.start()
            async for record in consumer:
                event = json.loads(record.value)
                if service=='payment' and event['eventType']=='OrderCreated':
                    with tracer.start_as_current_span('process_payment',context=propagation.extract(event.get('context',{}))):
                        context = {}
                        propagation.inject(context)
                        paid = dict(eventType='PaymentCompleted',orderId=event['orderId'],context=context,
                                    amount=event['payload']['totalAmount'])
                        await producer.send_and_wait(COMPLETED,json.dumps(paid).encode(),key=record.key)
                        payments.inc()
                        log('PaymentCompleted',orderId=event['orderId'])
                elif service!='payment':
                    operation = 'send_notification' if service=='notification' else 'confirm_order'
                    with tracer.start_as_current_span(operation,context=propagation.extract(event['context'])):
                        handle(event)
                        log(operation,orderId=event['orderId'])
                await consumer.commit()
        except Exception as exc:
            log(str(exc),'error')
            await asyncio.sleep(2)
        finally:
            await consumer.stop()
            await producer.stop()

