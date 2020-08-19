from kvs import KVS
import tempfile
from fastapi.testclient import TestClient
import pytest


from service.main import app
from memtable import Memtable, TOMBSTONE

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
    m = mocker.mock_open(read_data="key1,value1\nkey2,value2\n")
    mocker.patch("service.main.open", m)

    response = client.get("/log")
    assert response.status_code == 200
    assert response.json() == ["key1,value1\n", "key2,value2\n"]


def test_get_all_returns_all_pairs_that_were_not_unset(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/keys")
    assert response.status_code == 200
    assert response.json() == {"key1": "value1", "key2": "value2"}


def test_get_all_excludes_unset_pairs(mocker, kvs):
    kvs.unset("key1")
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/keys")
    assert response.status_code == 200
    assert response.json() == {"key2": "value2"}


def test_get_key_that_exists_returns_value(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/keys/key1")
    assert response.status_code == 200
    assert response.json() == "value1"


def test_get_key_that_never_existed_returns_404_with_message(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/keys/key3")
    assert response.status_code == 404
    assert "message" in response.json()


def test_get_key_that_was_unset_returns_404_with_message(mocker, kvs):
    kvs.unset("key2")
    mocker.patch("service.main.kvs", kvs)

    response = client.get("/keys/key2")
    assert response.status_code == 404
    assert "message" in response.json()


def test_set_key(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.put("/keys?key=key3&value=value3")
    assert kvs.get("key3") == "value3"
    assert response.status_code == 200
    assert response.json() == {"key3": "value3"}


def test_unset_key(mocker, kvs):
    mocker.patch("service.main.kvs", kvs)

    response = client.delete("/keys?key=key2")
    assert response.status_code == 204
    assert response.json() == None

