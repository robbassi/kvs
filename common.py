from typing import Union

class Tombstone:
    pass

TOMBSTONE = Tombstone()
Value = Union[str, Tombstone]
