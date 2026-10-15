import os
from datetime import datetime, Field, timedelta, timezone

import jwt
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.hash import pbkdf2_sha256
from pydantic import BaseModel, EmailStr, Field

app = FastAPI(
     title="BookStore — Auth", version="0.3",
     description="Регистрация продавцов, вход и проверка JWT.",
     docs_url="/api/auth/docs", openapi_url="/api/auth/openapi.json", redoc_url=None,
)

users = {}

SECRET_KEY = os.getenv("JWT_SECRET", "secret")
ALGORITHM = "HS256"

security = HTTPBearer()

class Credentials(BaseModel):
    email: EmailStr = Field(description="Электронная почта", examples=["mail@example.com"])
    password: str = Field(min_length=1, description="Пароль",examples=["study-password"])

class User(BaseModel):
    id: int = Field(description="ID пользователя; используется как userId в заказах")
    email: EmailStr
    
class RegistrationResult(BaseModel):
    message: str = Field(description="Сообщение о результате регистрации")
    user: User = Field(description="Данные зарегистрированного пользователя")

class Token(BaseModel):
    access_token: str = Field(description="JWT для заголовка Authorization: Bearer <token>")
    token_type: str = "bearer"
    
@app.get("/docs", include_in_schema=False)
def docs():
    return RedirectResponse(url="/api/auth/docs")

@app.post("/auth/register", include_in_schema=False)
@app.post("/api/auth/register", response_model=RegistrationResult, status_code=201,
        tags=["Auth"], summary="Регистрация пользователя", 
        responses={400: {"description": "Почта уже зарегистрирована"}})

def register(data: Credentials):
    if data.email in users:
        raise HTTPException(status_code=400, detail="User already exists")

    user = {
        "id": len(users) + 1,
        "email": str(data.email),
        "password_hash": pbkdf2_sha256.hash(data.password)
    }
    users[data.email] = user

    return {"message": "Пользователь зарегистрирован",
            "user": {"id": user["id"], "email": user["email"]}}


@app.post("/auth/login", include_in_schema=False)
@app.post("/api/auth/login", response_model=Token, tags=["Auth"], summary="Вход и получение JWT",	
          responses={401: {"description": "Неправильная почта или пароль"}})	

def login(data: Credentials):
    user = users.get(data.email)
    if not user or not pbkdf2_sha256.verify(data.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Неверная почта или пароль")

    payload = {
        "sub": str(user["id"]), 
        "exp": datetime.now(timezone.utc) + timedelta(hours=1)
    }	

    return {
        "access_token": jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM), 
        "token_type": "bearer"
    }

@app.get("/auth/me", include_in_schema=False)
@app.get("/api/auth/me", response_model=User, tags=["Auth"], summary="Текущий пользователь",	
         responses={401: {"description": "Токен отсутствует, недействителен или истёк"}})	

def me(credentials: HTTPAuthorizationCredentials | None = Depends(security)):
    if credentials is None:
        raise HTTPException(status_code=401, detail="Нужен Bearer-токен")

    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM],
                             options={"require": ["sub", "exp"]})
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Срок действия токена истёк")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Неверный токен")
    for user in users.values():
        if str(user["id"]) == payload["sub"]:
            return {"id": user["id"], "email": user["email"]}
    raise HTTPException(status_code=401, detail="Пользователь не найден")

        

@app.get("/auth/health", include_in_schema=False)
@app.get("/api/auth/health", tags=["Health"], summary="Проверка работоспособности")	

def health():
    return {"status": "ok"}