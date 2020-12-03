import os
import tempfile
from typing import Tuple, List, Union

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from common import TOMBSTONE
from binio import kv_iter
from kvs import KVS

SEGMENTS_PATH = os.getenv("SEGMENTS_PATH", tempfile.mkdtemp())
LOG_PATH = os.getenv("LOG_PATH", tempfile.mkstemp()[1])
app = FastAPI()
kvs = KVS(SEGMENTS_PATH, LOG_PATH)


class Message(BaseModel):
    message: str


@app.get("/")
async def healthcheck():
    return "Success"


@app.get("/log")
def log() -> List[Tuple[str, str]]:
    """Get the commit log file contents. For debugging purposes."""
    return list(kv_iter(LOG_PATH))


@app.get("/kvs/{key}", responses={404: {"model": Message}})
def get_key(key: str) -> Union[str, JSONResponse]:
    """Get the value stored at a particular key."""
    value = kvs.get(key)
    if value is None or value is TOMBSTONE:
        return JSONResponse({"message": f"Key `{key}` was not found."}, 404)
    return Response(status_code=200, content=value)


@app.put("/kvs", status_code=204)
def set_key(key: str, value: str) -> None:
    """Set a key-value pair"""
    kvs.set(key, value)
    return Response(status_code=204)


@app.delete("/kvs", status_code=204)
def unset_key(key: str) -> None:
    """Unset a key-value pair"""
    kvs.unset(key)
