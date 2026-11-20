import os
import hashlib
from contextlib import asynccontextmanager
import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, Response
from shared.security import decode_token

LIMIT = int(os.getenv('RATE_LIMIT', '60'))
WINDOW = int(os.getenv('RATE_WINDOW', '60'))
LUA = '''local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return {n, redis.call('TTL', KEYS[1])}'''
ROUTES = {'auth': 'http://auth:8001', 'catalog': 'http://catalog-1:8002',
          'orders': 'http://order:8003', 'analytics': 'http://analytics:8007',
          'payments': 'http://payment:8004', 'notifications': 'http://notification:8006',
          'graphql': 'http://graphql:8005'}

@asynccontextmanager
async def lifespan(app):
    app.state.redis = Redis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379'))
    app.state.http = httpx.AsyncClient(timeout=15, follow_redirects=False)
    yield
    await app.state.http.aclose()
    await app.state.redis.aclose()

app = FastAPI(title='Marketplace API Gateway', lifespan=lifespan)

@app.api_route('/orders', methods=['GET', 'POST'])
async def orders_root(request: Request):
    return await proxy('orders', '', request)

@app.get('/health')
def health():
    return {'status': 'healthy'}

@app.api_route('/{service}/{path:path}', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
async def proxy(service: str, path: str, request: Request):
    if service not in ROUTES:
        raise HTTPException(404, 'Unknown service')
    public = ((service == 'auth' and path in ('login', 'register') and request.method == 'POST')
              or (path in ('docs', 'openapi.json') and request.method == 'GET'))
    token = request.headers.get('authorization', '')
    # IP throttle runs before authentication, including malformed JWT and login attacks.
    identity = request.client.host
    try:
        ip_key = 'rate:ip:' + hashlib.sha256(identity.encode()).hexdigest()
        n, ttl = await request.app.state.redis.eval(LUA, 1, ip_key, WINDOW)
        if n > LIMIT:
            return JSONResponse({'detail': 'Rate limit exceeded'}, 429, headers={'Retry-After': str(max(ttl, 1)), 'X-RateLimit-Limit': str(LIMIT)})
    except RedisError:
        raise HTTPException(503, 'Rate limiter unavailable')
    if not public:
        if not token.startswith('Bearer '):
            raise HTTPException(401, 'Bearer token required', headers={'WWW-Authenticate': 'Bearer'})
        user = decode_token(token[7:])
        privileged = (service in ('analytics', 'payments', 'notifications') or
            (service == 'catalog' and request.method != 'GET') or
            (service == 'orders' and (path.startswith('admin/') or path.startswith('check-payment/'))))
        if privileged and user['role'] != 'admin':
            raise HTTPException(403, 'Admin role required')
    headers = {'authorization': token}
    if 'content-type' in request.headers:
        headers['content-type'] = request.headers['content-type']
    target_path = '/graphql' if service == 'graphql' else '/' + path
    try:
        response = await request.app.state.http.request(request.method, ROUTES[service] + target_path,
            params=request.query_params, content=await request.body(), headers=headers)
    except httpx.RequestError:
        raise HTTPException(502, 'Upstream unavailable')
    safe_headers = {k: v for k, v in response.headers.items() if k.lower() in ('content-type', 'www-authenticate', 'retry-after')}
    safe_headers['X-RateLimit-Limit'] = str(LIMIT)
    safe_headers['X-RateLimit-Remaining'] = str(max(0, LIMIT - n))
    return Response(response.content, response.status_code, headers=safe_headers)
