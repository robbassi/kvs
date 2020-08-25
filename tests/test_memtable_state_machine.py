import os
import tempfile
from typing import Dict

import hypothesis.strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, rule

from common import TOMBSTONE, Value
from memtable import Memtable


class MemtableDictComparison(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        _, self.tempf = tempfile.mkstemp()
        self.memtable = Memtable()
        self.model: Dict[str, Value] = dict()

    keys = Bundle("keys")
    values = Bundle("values")

    @rule(target=keys, key=st.text())
    def add_key(self, key: str) -> str:
        return key

    @rule(target=values, value=st.text())
    def add_value(self, value: str) -> str:
        return value

    @rule(key=keys, value=values)
    def set(self, key: str, value: str):
        self.model[key] = value
        self.memtable.set(key, value)

    @rule(key=keys)
    def unset(self, key: str):
        self.model[key] = TOMBSTONE
        self.memtable.unset(key)

    @rule(key=keys)
    def values_agree(self, key: str):
        assert self.memtable.get(key) == self.model.get(key, None)

    @rule()
    def memtable_is_ordered(self):
        assert list(self.memtable.entries()) == sorted(self.memtable.entries())

    # TODO: Current bytes bug (issue #31) breaks this rule
    # @rule()
    # def approximate_bytes_reflects_what_is_in_model(self):
    #     def add_up_bytes():
    #         bytes_ = 0
    #         for (key, value) in self.model.items():
    #             if value is TOMBSTONE:
    #                 bytes_ += len(key)
    #             else:
    #                 bytes_ += len(key) + len(value) #type: ignore [arg-type]
    #         return bytes_
    #     assert self.memtable.approximate_bytes() == add_up_bytes()

    def teardown(self):
        os.remove(self.tempf)


TestMemtable = MemtableDictComparison.TestCase
