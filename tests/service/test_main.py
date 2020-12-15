from kvs import KVS
import tempfile
from fastapi.testclient import TestClient
import pytest


from service.main import app

client = TestClient(app)
SEGMENTS_PATH = tempfile.mkdtemp()
_, LOG_PATH = tempfile.mkstemp()


@pytest.fixture
def kvs():
    pairs = {"key1": "value1", "key2": "value2"}
    kvs_ = KVS(SEGMENTS_PATH, LOG_PATH)
    for (key, value) in pairs.items():
        kvs_.set(key, value)
    return kvs_


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == "Success"


def test_log_returns_lines_from_commit_file(mocker):
    log_entries = [["key1","value1"], ["key2","value2"]]
    mocker.patch("service.main.kv_iter", lambda _: log_entries)

    response = client.get("/log")
    assert response.status_code == 200
    assert response.json() == log_entries


def test_get_key_that_exists_returns_value(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/kvs/key1")
    assert response.status_code == 200
    assert response.json() == "value1"


def test_get_key_that_never_existed_returns_404_with_message(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/kvs/key3")
    assert response.status_code == 404
    assert "not found" in response.json()['detail']


def test_get_key_that_was_unset_returns_404_with_message(mocker, kvs):
    kvs.unset("key2")
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/kvs/key2")
    assert response.status_code == 404
    assert "not found" in response.json()['detail']


def test_set_key(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.put("/kvs?key=key3&value=value3")
    assert kvs.get("key3") == "value3"
    assert response.status_code == 204


def test_unset_key(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.delete("/kvs?key=key2")
    assert response.status_code == 204
    assert response.json() == None

