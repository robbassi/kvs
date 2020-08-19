from memtable import TOMBSTONE
import tempfile
from typing import Dict, List, Union

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kvs import KVS

SEGMENTS_PATH = tempfile.mkdtemp()
_, LOG_PATH = tempfile.mkstemp()
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


@app.get("/keys")
async def get_all() -> Dict[str, str]:
    """Get all key-value pairs. For debugging purposes."""
    return {key: value for (key, value) in kvs.memtable.entries() if value is not TOMBSTONE}


@app.get("/keys/{key}", responses={404: {"model": Message}})
async def get_key(key: str) -> Union[str, JSONResponse]:
    """Get the value stored at a particular key."""
    value = kvs.get(key)
    if value is None or value is TOMBSTONE:
        return JSONResponse({"message": f"Key `{key}` was not found."}, 404)
    return str(value)


@app.put("/keys")
def set_key(key: str, value: str) -> Dict[str, str]:
    """Set a key-value pair"""
    kvs.set(key, value)
    return {key: value}


@app.delete("/keys", status_code=204)
def unset_key(key: str) -> None:
    """Unset a key-value pair"""
    kvs.unset(key)