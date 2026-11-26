import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Literal
from uuid import uuid4

import bcrypt
import jwt
import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from shared.security import SECRET, ISSUER, AUDIENCE, current_user


def connect():
    return psycopg.connect(host='postgres', dbname='marketplace', user='marketplace',
                           password=os.environ['POSTGRES_PASSWORD'])


def digest(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


@asynccontextmanager
async def lifespan(app):
    with connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS roles (name text PRIMARY KEY)")
        db.execute("INSERT INTO roles VALUES ('admin'), ('user') ON CONFLICT DO NOTHING")
        db.execute("""CREATE TABLE IF NOT EXISTS users (id text PRIMARY KEY, email text UNIQUE NOT NULL,
                    password_hash text NOT NULL, role text NOT NULL REFERENCES roles(name))""")
        db.execute("INSERT INTO users VALUES (%s,'admin',%s,'admin') ON CONFLICT DO NOTHING",
                   ('admin', digest(os.environ['ADMIN_PASSWORD'])))
    yield


app = FastAPI(title='Marketplace Auth', lifespan=lifespan, root_path='/auth')


class Registration(BaseModel):
    email: str = Field(pattern=r'^[^\s@]+@[^\s@]+\.[^\s@]+$', max_length=254)
    password: str = Field(min_length=12, max_length=72)
    role: Literal['user', 'admin'] = 'user'


@app.post('/register', status_code=201)
def register(body: Registration):
    if body.role != 'user':
        raise HTTPException(403, 'Public registration assigns user role')
    if len(body.password.encode()) > 72:
        raise HTTPException(422, 'Password exceeds 72 bytes')
    uid = str(uuid4())
    with connect() as db:
        row = db.execute("""INSERT INTO users VALUES (%s,%s,%s,%s) ON CONFLICT (email)
                         DO NOTHING RETURNING id""", (uid, body.email.lower(), digest(body.password), body.role)).fetchone()
        if not row:
            raise HTTPException(409, 'Email already registered')
    return {'id': uid, 'email': body.email.lower(), 'role': body.role}


@app.post('/login')
async def login(request: Request):
    data = await request.json() if request.headers.get('content-type','').startswith('application/json') else await request.form()
    from starlette.concurrency import run_in_threadpool
    return await run_in_threadpool(authenticate, data)


def authenticate(data):
    email = str(data.get('email', data.get('username', ''))).lower()
    password = str(data.get('password','')).encode()
    with connect() as db:
        row = db.execute('SELECT id,password_hash,role FROM users WHERE email=%s',(email,)).fetchone()
    if not row or len(password)>72 or not bcrypt.checkpw(password,row[1].encode()):
        raise HTTPException(401, 'Invalid credentials')
    now = datetime.now(timezone.utc)
    token = jwt.encode(dict(sub=row[0],role=row[2],iat=now,exp=now+timedelta(minutes=30),
                            iss=ISSUER,aud=AUDIENCE), SECRET, algorithm='HS256')
    return {'access_token':token,'token_type':'bearer','expires_in':1800}


@app.get('/me')
def me(user=Depends(current_user)):
    return {'username':user['sub'],'role':user['role']}


@app.get('/health')
def health():
    return {'status':'healthy'}

from shared.telemetry import install
install(app)
