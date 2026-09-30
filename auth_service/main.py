from fastapi import FastAPI, HTTPException, Header
import jwt
from datetime import datetime, timedelta, timezone
app = FastAPI()

users = {}

SECRET_KEY = "secret"
ALGORITHM = "HS256"

@app.post("/auth/register")
def register(username: str, password: str):
    if username in users:
        raise HTTPException(status_code=400, detail="User already exists")

    users[username] = password

    return {
        "message": "User registered",
        "username": username
    }


@app.post("/auth/login")
def login(username: str, password: str):
    if username not in users or users[username] != password:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    payload = {
        "sub": username,
        "exp": datetime.now(timezone.utc) + timedelta(hours = 1)
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm = ALGORITHM)

    return {
        "access token": token,
        "token_type": "bearer"
    }

@app.get("/auth/me") 
def me(authorization: str | None = Header(default=None)): 
    if not authorization: 
        raise HTTPException( status_code=401, detail="Authorization is required" ) 
    if not authorization.startswith("Bearer "):
        raise HTTPException( status_code=401, detail="Invalid authorization header" ) 
    token = authorization.split(" ", 1)[1] 
    try: 
        payload = jwt.decode( token, SECRET_KEY, algorithms = ALGORITHM ) 
        username = payload.get("sub") 
        if not username: 
            raise HTTPException( status_code=401, detail="Invalid token" ) 
        return { "username": username } 
    except jwt.ExpiredSignatureError: raise HTTPException( status_code=401, detail="Token expired" ) 
    except jwt.InvalidTokenError: raise HTTPException( status_code=401, detail="Invalid token" )

@app.get("/auth/health")
def health():
    return {
        "status": "ok"
    }