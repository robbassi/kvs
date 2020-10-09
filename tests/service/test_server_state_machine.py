import os
import tempfile

from typing import Dict, Optional, Union
import hypothesis.strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, rule

from memtable import Memtable, TOMBSTONE

from fastapi.testclient import TestClient
from service.main import app


class ServerDictComparison(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.client = TestClient(app)
        self.model: Dict[str, Optional[str]] = dict()

    keys = Bundle("keys")
    values = Bundle("values")

    @rule(target=keys, key=st.text(min_size=1))
    def add_key(self, key: str):
        return key

    @rule(target=values, value=st.text())
    def add_value(self, value: str):
        return value

    @rule(key=keys, value=values)
    def set(self, key: str, value: str):
        self.model[key] = value
        self.client.put(f"/kvs?key={key}&value={value}")

    @rule(key=keys)
    def unset(self, key: str):
        self.model[key] = None
        self.client.delete(f"/kvs?key={key}")

    @rule(key=keys)
    def values_agree(self, key: str):
        res = self.client.get(f"/kvs/{key}")
        if res.status_code == 404:
            assert self.model.get(key, None) == None
        else:
            assert res.text == self.model.get(key, None)


TestServer = ServerDictComparison.TestCase
