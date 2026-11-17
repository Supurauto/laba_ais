import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr, Field


SECRET_KEY = os.environ["JWT_SECRET"]
ALGORITHM = "HS256"
DATABASE_PATH = "/data/auth.db"

if len(SECRET_KEY.encode("utf-8")) < 32:
    raise RuntimeError("JWT_SECRET должен содержать не менее 32 байт")

password_hasher = PasswordHasher()


@contextmanager
def database():
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(DATABASE_PATH, timeout=10)
    connection.row_factory = sqlite3.Row

    try:
        with connection:
            yield connection
    finally:
        connection.close()


@asynccontextmanager
async def lifespan(app):
    with database() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('user', 'admin'))
            )
            """
        )

    yield


app = FastAPI(
    title="BookStore — Auth",
    version="7.0",
    docs_url="/api/auth/docs",
    openapi_url="/api/auth/openapi.json",
    redoc_url=None,
    lifespan=lifespan,
)


class Credentials(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class Registration(Credentials):
    role: Literal["user", "admin"] = "user"


@app.get("/docs", include_in_schema=False)
def docs():
    return RedirectResponse("/api/auth/docs")


@app.post("/auth/register", status_code=201)
@app.post("/api/auth/register", status_code=201, include_in_schema=False)
def register(data: Registration):
    email = str(data.email).lower()
    password_hash = password_hasher.hash(data.password)

    try:
        with database() as connection:
            cursor = connection.execute(
                """
                INSERT INTO users (email, password_hash, role)
                VALUES (?, ?, ?)
                """,
                (email, password_hash, data.role),
            )
            user_id = cursor.lastrowid

    except sqlite3.IntegrityError:
        raise HTTPException(
            status_code=409,
            detail="Пользователь с такой почтой уже существует",
        )

    return {
        "message": "Пользователь зарегистрирован",
        "user": {
            "id": user_id,
            "email": email,
            "role": data.role,
        },
    }


@app.post("/auth/login")
@app.post("/api/auth/login", include_in_schema=False)
def login(data: Credentials):
    with database() as connection:
        user = connection.execute(
            "SELECT * FROM users WHERE email = ?",
            (str(data.email).lower(),),
        ).fetchone()

    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Неверная почта или пароль",
        )

    try:
        password_hasher.verify(user["password_hash"], data.password)
    except (VerificationError, InvalidHashError):
        raise HTTPException(
            status_code=401,
            detail="Неверная почта или пароль",
        )

    now = datetime.now(timezone.utc)

    payload = {
        "sub": str(user["id"]),
        "role": user["role"],
        "iat": now,
        "exp": now + timedelta(hours=1),
    }

    return {
        "access_token": jwt.encode(
            payload,
            SECRET_KEY,
            algorithm=ALGORITHM,
        ),
        "token_type": "bearer",
    }


@app.get("/auth/me")
@app.get("/api/auth/me", include_in_schema=False)
def me(
    x_user_id: str | None = Header(default=None),
    x_user_role: str | None = Header(default=None),
):
    if (
        not x_user_id
        or not x_user_id.isascii()
        or not x_user_id.isdigit()
        or len(x_user_id) > 18
        or int(x_user_id) <= 0
        or x_user_role not in {"user", "admin"}
    ):
        raise HTTPException(
            status_code=403,
            detail="Нет доверенного контекста пользователя",
        )

    with database() as connection:
        user = connection.execute(
            "SELECT id, email, role FROM users WHERE id = ?",
            (int(x_user_id),),
        ).fetchone()

    if user is None:
        raise HTTPException(
            status_code=404,
            detail="Пользователь не найден",
        )

    return dict(user)


@app.get("/auth/health")
@app.get("/api/auth/health", include_in_schema=False)
def health():
    return {"status": "ok"}