import asyncio
import math
import os
import time
from contextlib import asynccontextmanager

import httpx
import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


SECRET_KEY = os.environ["JWT_SECRET"]

if len(SECRET_KEY.encode("utf-8")) < 32:
    raise RuntimeError("JWT_SECRET должен содержать не менее 32 байт")


WINDOW_SECONDS = 15 * 60
GENERAL_LIMIT = 100
LOGIN_LIMIT = 5

buckets = {}
rate_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app):
    async with httpx.AsyncClient(
        timeout=15,
        trust_env=False,
        follow_redirects=False,
    ) as client:
        app.state.client = client
        yield


app = FastAPI(
    title="BookStore — API Gateway",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


def rate_headers(limit, count, reset):
    return {
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": str(max(0, limit - count)),
        "X-RateLimit-Reset": str(math.ceil(reset)),
    }


async def check_rate(ip, is_login):
    now = time.time()

    async with rate_lock:
        for key, (_, reset) in list(buckets.items()):
            if reset <= now:
                del buckets[key]

        rules = [("general", GENERAL_LIMIT)]

        if is_login:
            rules.append(("login", LOGIN_LIMIT))

        states = []

        for name, limit in rules:
            key = (name, ip)
            count, reset = buckets.get(
                key,
                (0, now + WINDOW_SECONDS),
            )
            states.append((key, limit, count, reset))

        for key, limit, count, reset in states:
            if count >= limit:
                headers = rate_headers(limit, count, reset)
                headers["Retry-After"] = str(
                    max(1, math.ceil(reset - now))
                )
                return False, headers

        for key, limit, count, reset in states:
            buckets[key] = (count + 1, reset)

        key, limit, count, reset = states[-1]

        return True, rate_headers(limit, count + 1, reset)


PUBLIC_AUTH = {
    "/auth/register",
    "/auth/login",
    "/auth/health",
    "/api/auth/register",
    "/api/auth/login",
    "/api/auth/health",
}

PUBLIC_DOCS = {
    "/api/auth/docs",
    "/api/auth/openapi.json",
    "/api/catalog/docs",
    "/api/catalog/openapi.json",
    "/api/orders/docs",
    "/api/orders/openapi.json",
    "/analytics/docs",
    "/analytics/openapi.json",
}

LOGIN_PATHS = {
    "/auth/login",
    "/api/auth/login",
}


@app.middleware("http")
async def security(request: Request, call_next):
    path = request.url.path

    ip = request.headers.get("x-real-ip")

    if not ip:
        ip = request.client.host if request.client else "unknown"

    allowed, headers = await check_rate(
        ip,
        path in LOGIN_PATHS,
    )

    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"detail": "Слишком много запросов"},
            headers=headers,
        )

    request.state.user_id = None
    request.state.role = None

    if path not in PUBLIC_AUTH and path not in PUBLIC_DOCS:
        authorization = request.headers.get("authorization", "")
        scheme, separator, token = authorization.partition(" ")

        if not separator or scheme.lower() != "bearer" or not token:
            return JSONResponse(
                status_code=401,
                content={"detail": "Нужен Bearer-токен"},
                headers={
                    **headers,
                    "WWW-Authenticate": "Bearer",
                },
            )

        try:
            claims = jwt.decode(
                token,
                SECRET_KEY,
                algorithms=["HS256"],
                options={"require": ["sub", "role", "exp"]},
            )

            user_id = claims["sub"]
            role = claims["role"]

            if (
                not isinstance(user_id, str)
                or not user_id.isascii()
                or not user_id.isdigit()
                or len(user_id) > 18
                or int(user_id) <= 0
                or not isinstance(role, str)
                or role not in {"user", "admin"}
            ):
                raise ValueError("Некорректные claims")

        except (jwt.InvalidTokenError, ValueError, TypeError):
            return JSONResponse(
                status_code=401,
                content={"detail": "Токен недействителен или просрочен"},
                headers={
                    **headers,
                    "WWW-Authenticate": "Bearer",
                },
            )

        request.state.user_id = user_id
        request.state.role = role

    response = await call_next(request)
    response.headers.update(headers)

    return response


def choose_backend(path):
    if path == "/auth" or path.startswith("/auth/"):
        return "http://auth:8000"

    if path.startswith("/api/auth/"):
        return "http://auth:8000"

    if path.startswith("/api/catalog/"):
        return "http://nginx:8080"

    if (
        path == "/orders"
        or path.startswith("/orders/")
        or path == "/api/orders"
        or path.startswith("/api/orders/")
        or path.startswith("/debug/orders/")
        or path.startswith("/check-payment/")
    ):
        return "http://order:8000"

    if path.startswith("/analytics/"):
        return "http://analytics:8000"

    if path in {"/graphql", "/graphql/"}:
        return "http://graphql:8000"

    return None


def add_bearer_security(schema):
    components = schema.setdefault("components", {})
    schemes = components.setdefault("securitySchemes", {})

    schemes["GatewayBearer"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "Вставьте access_token, полученный при логине",
    }

    http_methods = {
        "get",
        "post",
        "put",
        "patch",
        "delete",
        "options",
        "head",
        "trace",
    }

    for path, operations in schema.get("paths", {}).items():
        for method, operation in operations.items():
            if method not in http_methods:
                continue

            if path in PUBLIC_AUTH:
                operation["security"] = []
            else:
                operation["security"] = [
                    {"GatewayBearer": []}
                ]

    return schema


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy(request: Request, path: str):
    backend = choose_backend(request.url.path)

    if backend is None:
        return JSONResponse(
            status_code=404,
            content={"detail": "Маршрут не найден"},
        )

    forwarded_headers = {}

    for name in ("accept", "content-type"):
        if name in request.headers:
            forwarded_headers[name] = request.headers[name]

    if request.state.user_id is not None:
        forwarded_headers["X-User-Id"] = request.state.user_id
        forwarded_headers["X-User-Role"] = request.state.role

    forwarded_headers["Accept-Encoding"] = "identity"

    try:
        upstream = await request.app.state.client.request(
            method=request.method,
            url=backend + request.url.path,
            params=request.query_params.multi_items(),
            headers=forwarded_headers,
            content=await request.body(),
        )

    except httpx.RequestError:
        return JSONResponse(
            status_code=502,
            content={"detail": "Внутренний сервис недоступен"},
        )

    if (
        upstream.status_code == 200
        and request.url.path.endswith("/openapi.json")
    ):
        try:
            schema = upstream.json()
        except ValueError:
            return JSONResponse(
                status_code=502,
                content={"detail": "Некорректная OpenAPI-схема"},
            )

        return JSONResponse(
            content=add_bearer_security(schema)
        )

    response_headers = {}

    for name in ("content-type", "content-language", "allow"):
        if name in upstream.headers:
            response_headers[name] = upstream.headers[name]

    if "location" in upstream.headers:
        location = upstream.headers["location"]

        if location.startswith(backend + "/"):
            location = location[len(backend):]

        response_headers["Location"] = location

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=response_headers,
    )