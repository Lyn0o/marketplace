import os
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
import jwt
from passlib.context import CryptContext
from pydantic import BaseModel, Field, ConfigDict
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from shared.security import SECRET, ISSUER, AUDIENCE, current_user

DB = os.getenv('AUTH_DB', '/data/auth.db')
pwd = CryptContext(schemes=['pbkdf2_sha256'])

@asynccontextmanager
async def lifespan(app):
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB) as db:
        db.execute('CREATE TABLE IF NOT EXISTS users(username TEXT PRIMARY KEY, password TEXT, role TEXT)')
        password = os.environ['ADMIN_PASSWORD']
        if len(password) < 12:
            raise RuntimeError('ADMIN_PASSWORD must contain at least 12 characters')
        db.execute('INSERT OR IGNORE INTO users VALUES(?,?,?)', ('admin', pwd.hash(password), 'admin'))
    yield

app = FastAPI(title='JWT Authentication', lifespan=lifespan, root_path='/auth')

class Registration(BaseModel):
    model_config = ConfigDict(extra='forbid')
    username: str = Field(min_length=3, max_length=64, pattern=r'^[a-zA-Z0-9_-]+$')
    password: str = Field(min_length=12, max_length=128)

@app.post('/register', status_code=201)
def register(body: Registration):
    with sqlite3.connect(DB) as db:
        try:
            db.execute('INSERT INTO users VALUES(?,?,?)', (body.username, pwd.hash(body.password), 'user'))
        except sqlite3.IntegrityError:
            raise HTTPException(409, 'Username already exists')
    return {'username': body.username, 'role': 'user'}

@app.post('/login')
def login(form: OAuth2PasswordRequestForm = Depends()):
    with sqlite3.connect(DB) as db:
        row = db.execute('SELECT password,role FROM users WHERE username=?', (form.username,)).fetchone()
    if not row or not pwd.verify(form.password, row[0]):
        raise HTTPException(401, 'Invalid credentials', headers={'WWW-Authenticate': 'Bearer'})
    now = datetime.now(timezone.utc)
    token = jwt.encode({'sub': form.username, 'role': row[1], 'iat': now,
        'exp': now + timedelta(minutes=30), 'iss': ISSUER, 'aud': AUDIENCE}, SECRET, algorithm='HS256')
    return {'access_token': token, 'token_type': 'bearer', 'expires_in': 1800}

@app.get('/me')
def me(user=Depends(current_user)):
    return {'username': user['sub'], 'role': user['role']}

@app.get('/health')
def health():
    return {'status': 'healthy'}
