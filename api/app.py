
from fastapi import FastAPI

appi = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}
