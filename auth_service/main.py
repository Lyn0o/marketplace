from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel
from passlib.context import CryptContext
import jwt
from datetime import datetime, timedelta, timezone
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

app = FastAPI()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str):
    return pwd_context.hash(password)

# вынести в .env?
SECRET_KEY = "SECRET_KEY"
ALGORITHM = "HS256"


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

class User(BaseModel):
    username: str
    password: str


fake_users_db = {
    "alice": {
        "username": "alice",
        "hashed_password": hash_password("1234"),
    },
}

def get_current_user(token: str = Depends(oauth2_scheme)):
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    username: str = payload.get("sub")
    if username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный токен"
        )

    
    user = fake_users_db.get(username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Пользователь не найден"
        )
    return user

@app.post("/register")
def register(user: User):
    if user.username in fake_users_db:
        raise HTTPException(status_code=400, detail="Данный username уже существует")

    fake_users_db[user.username] = {
        "username": user.username,
        "hashed_password": hash_password(user.password)
    }
    #return {"msg": fake_users_db} #посмотреть все записи
    return {"msg": "Пользователь зарегистрирован", "username": user.username}

@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username not in fake_users_db:
        raise HTTPException(status_code=400, detail="Данного username не существует")
    
    if not pwd_context.verify(form_data.password, fake_users_db[form_data.username]["hashed_password"]):
        raise HTTPException(status_code=401, detail="Неверный пароль")
    
    to_encode = {
        "sub": form_data.username,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=60)
    }
    
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return {
        "access_token": encoded_jwt,
        "token_type": "bearer"
    }
    

@app.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return {"username": current_user["username"], "message": "Это защищённый эндпоинт"}