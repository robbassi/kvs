from typing import Iterator, Optional, Tuple

from common import TOMBSTONE, Value
from rbtree import RBTree, inorder_traversal

class Memtable:
    def __init__(self):
        self.bytes = 0
        self.tree = RBTree()

    def set(self, k: str, v: str) -> None:
        self.bytes += len(k) + len(v)
        self.tree.insert(k, v)

    def get(self, k: str) -> Optional[Value]:
        return self.tree.search(k)

    def unset(self, k: str) -> None:
        value = self.get(k)
        if value is TOMBSTONE:
            return
        if value is not None:
            self.bytes -= len(value) #type: ignore [arg-type]
        else:
            self.bytes += len(k)
        self.tree.insert(k, TOMBSTONE)

    def entries(self) -> Iterator[Tuple[str, str]]:
        yield from inorder_traversal(self.tree.root)
 
    def approximate_bytes(self) -> int:
        return self.bytes
