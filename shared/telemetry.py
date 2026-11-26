import json
import logging
import os
import time
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

name = os.getenv('OTEL_SERVICE_NAME', 'marketplace')
provider = TracerProvider(resource=Resource.create({'service.name':name}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint='http://jaeger:4318/v1/traces')))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(name)
propagation = TraceContextTextMapPropagator()
requests = Counter('http_requests_total','HTTP requests',['method','endpoint','status'])
duration = Histogram('http_request_duration_seconds','HTTP duration',['method','endpoint'])
errors = Counter('errors_total','HTTP errors')
orders = Counter('orders_created_total','Created orders')
payments = Counter('payments_completed_total','Completed payments')
cancellations = Counter('orders_cancelled_total','Cancelled orders')


def log(message, level='info', **fields):
    tid = trace.get_current_span().get_span_context().trace_id
    print(json.dumps(dict(service_name=name,level=level,trace_id=format(tid,'032x'),message=message,**fields)), flush=True)


def install(app):
    @app.get('/metrics', include_in_schema=False)
    def metrics():
        return Response(generate_latest(),headers={'Content-Type':CONTENT_TYPE_LATEST})

    @app.middleware('http')
    async def observe(request, call_next):
        if request.url.path in ('/metrics','/health'):
            return await call_next(request)
        start = time.perf_counter()
        status = 500
        operation = 'create_order' if name=='order' and request.method=='POST' and request.url.path.rstrip('/') in ('','/orders') else request.method
        with tracer.start_as_current_span(operation, context=propagation.extract(request.headers)) as span:
            try:
                response = await call_next(request)
                status = response.status_code
                return response
            finally:
                endpoint = getattr(request.scope.get('route'),'path','unmatched')
                span.set_attribute('http.route',endpoint)
                span.set_attribute('http.status_code',status)
                requests.labels(request.method,endpoint,status).inc()
                duration.labels(request.method,endpoint).observe(time.perf_counter()-start)
                if status >=400:
                    errors.inc()
                log(endpoint,'error' if status>=400 else 'info',status=status)

