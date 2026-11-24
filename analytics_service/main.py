import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress
from fastapi import FastAPI, Depends
from shared.security import admin
from shared.events import consume_events
from analytics_service.projection import AnalyticsProjection

logging.basicConfig(level=logging.INFO)
projection = AnalyticsProjection(os.getenv('ANALYTICS_DB', '/data/analytics.db'))


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(consume_events('analytics-service-group', projection.apply))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title='Order Analytics', lifespan=lifespan, root_path='/analytics')


@app.get('/health')
def health():
    return {'status': 'healthy'}


@app.get('/summary', dependencies=[Depends(admin)])
def summary():
    return projection.summary()
