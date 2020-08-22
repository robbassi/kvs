import os
import tempfile
from typing import List, Union

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kvs import KVS
from memtable import TOMBSTONE

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
def log() -> List[str]:
    """Get the commit log file contents. For debugging purposes."""
    with open(LOG_PATH, "r") as commit_file:
        return commit_file.readlines()


@app.get("/kvs/{key}", responses={404: {"model": Message}})
async def get_key(key: str) -> Union[str, JSONResponse]:
    """Get the value stored at a particular key."""
    value = kvs.get(key)
    if value is None or value is TOMBSTONE:
        return JSONResponse({"message": f"Key `{key}` was not found."}, 404)
    return value


@app.put("/kvs", status_code=204)
def set_key(key: str, value: str) -> None:
    """Set a key-value pair"""
    kvs.set(key, value)


@app.delete("/kvs", status_code=204)
def unset_key(key: str) -> None:
    """Unset a key-value pair"""
    kvs.unset(key)
