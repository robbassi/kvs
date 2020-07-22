from typing import Iterable, Optional, Tuple, Type, TypeVar, Union
from .rbtree import RBTree, inorder_traversal

A = TypeVar("A")
class Tombstone:

    def __len__(self):
        return 0

    pass

TOMBSTONE = Tombstone()

class Memtable:
    def __init__(self):
        self.bytes = 0
        self.tree: RBTree[Union[str, Tombstone]] = RBTree()

    def set(self, k: str, v: str) -> None:
        self.bytes += len(k) + len(v)
        self.tree.insert(k, v)

    def get(self, k: str) -> Optional[Union[str, Tombstone]]:
        return self.tree.search(k)

    def unset(self, k: str) -> None:
        value = self.get(k)
        if value:
            self.bytes -= len(value)
        else:
            self.bytes += len(k)
        self.tree.insert(k, TOMBSTONE)

    def entries(self) -> Iterable[Tuple[str, A]]:
        yield from ((node.key, node.value) for node in inorder_traversal(self.tree))
 
    def approximate_bytes(self):
        return self.bytes
