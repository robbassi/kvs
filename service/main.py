import os
import tempfile
from typing import Tuple, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from binio import kv_iter
from common import TOMBSTONE
from kvs import KVS

SEGMENTS_PATH = os.getenv("SEGMENTS_PATH", tempfile.mkdtemp())
LOG_PATH = os.getenv("LOG_PATH", tempfile.mkstemp()[1])
app = FastAPI()
kvs = KVS(SEGMENTS_PATH, LOG_PATH)


@app.get("/")
async def healthcheck():
    return "Success"


@app.get("/log")
def log() -> List[Tuple[str, str]]:
    """Get the commit log file contents. For debugging purposes."""
    return list(kv_iter(LOG_PATH))


@app.get("/kvs/{key}")
def get_key(key: str) -> str:
    """Get the value stored at a particular key."""
    value = kvs.get(key)
    if value is None or value is TOMBSTONE:
        raise HTTPException(status_code=404, detail=f"Key `{key}` was not found.")
    return value


@app.put("/kvs", status_code=204)
def set_key(key: str, value: str) -> JSONResponse:
    """Set a key-value pair"""
    kvs.set(key, value)
    return JSONResponse(status_code=204)


@app.delete("/kvs", status_code=204)
def unset_key(key: str) -> JSONResponse:
    """Unset a key-value pair"""
    kvs.unset(key)
    return JSONResponse(status_code=204)
