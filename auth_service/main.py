from fastapi import FastAPI, HTTPException

app = FastAPI()

users = {}


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

    return {
        "message": "Login successful",
        "username": username
    }


@app.get("/auth/health")
def health():
    return {
        "status": "ok"
    }