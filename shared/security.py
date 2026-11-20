"""JWT validation shared by the gateway and resource services."""
import os
import jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

SECRET = os.environ["JWT_SECRET"]
if len(SECRET) < 32:
    raise RuntimeError("JWT_SECRET must contain at least 32 characters")
ISSUER = "marketplace-auth"
AUDIENCE = "marketplace"
bearer = OAuth2PasswordBearer(tokenUrl="/auth/login")


def decode_token(token):
    try:
        claims = jwt.decode(token, SECRET, algorithms=["HS256"], issuer=ISSUER,
                            audience=AUDIENCE,
                            options={"require": ["exp", "iat", "sub", "role", "iss", "aud"]})
        if claims["role"] not in ("admin", "user") or not claims["sub"]:
            raise jwt.InvalidTokenError()
        return claims
    except jwt.PyJWTError:
        raise HTTPException(401, "Invalid or expired token", headers={"WWW-Authenticate": "Bearer"})


def current_user(token: str = Depends(bearer)):
    return decode_token(token)


def admin(user=Depends(current_user)):
    if user["role"] != "admin":
        raise HTTPException(403, "Admin role required")
    return user


def owner(user, order):
    if user["role"] != "admin" and user["sub"] != order["userId"]:
        raise HTTPException(403, "Order belongs to another user")
