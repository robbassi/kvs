from typing import Iterable, Optional, Tuple, Type, TypeVar, Union
from .rbtree import RBTree, inorder_traversal

A = TypeVar("A")
class Tombstone:
    pass

TOMBSTONE = Tombstone()
MemValue = Union[str, Tombstone]

class Memtable:
    def __init__(self):
        self.bytes = 0
        self.tree: RBTree[MemValue] = RBTree()

    def set(self, k: str, v: str) -> None:
        self.bytes += len(k) + len(v)
        self.tree.insert(k, v)

    def get(self, k: str) -> Optional[MemValue]:
        return self.tree.search(k)

    def unset(self, k: str) -> None:
        value = self.get(k)
        if isinstance(value, Tombstone):
            return
        if value:
            self.bytes -= len(value)
        else:
            self.bytes += len(k)
        self.tree.insert(k, TOMBSTONE)

    def entries(self) -> Iterable[Tuple[str, MemValue]]:
        yield from ((key, value) for (key, value) in inorder_traversal(self.tree))
 
    def approximate_bytes(self):
        return self.bytes
