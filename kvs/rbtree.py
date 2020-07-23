from __future__ import annotations
from enum import Enum
from typing import Generic, Iterable, Optional, Tuple, TypeVar

A = TypeVar("A")

class Color(Enum):
    BLACK = 0
    RED = 1


RED = Color.RED
BLACK = Color.BLACK

def inorder_traversal(node: Node[A]) -> Iterable[Tuple[str, A]]:
    if node is None:
        return
    if node.left:
        yield from inorder_traversal(node.left)
    yield (node.key, node.value)
    if node.right:
        yield from inorder_traversal(node.right)


class Node(Generic[A]):
    def __init__(
        self,
        color: Color,
        key: str,
        value: A,
        parent: Optional[Node[A]],
        left: Optional[Node[A]],
        right: Optional[Node[A]],
    ):
        self.color = color
        self.key = key
        self.value = value
        self.parent = parent
        self.left = left
        self.right = right

    @property
    def uncle(self) -> Optional[Node[A]]:
        g = self.grandparent
        if g:
            if g.left and g.left == self.parent:
                return g.right
            return g.left
        return None

    @property
    def grandparent(self) -> Optional[Node[A]]:
        p = self.parent
        if p:
            return p.parent
        return None


class RBTree(Generic[A]):
    def __init__(self):
        self.root = None

    def search(self, key: str) -> Optional[A]:
        if self.root:
            return self.search_r(self.root, key)
        return None

    def search_r(self, root: Node[A], key: str):
        if key < root.key and root.left:
            return self.search_r(root.left, key)
        if key > root.key and root.right:
            return self.search_r(root.right, key)
        if key == root.key:
            return root.value
        return None

    def insert(self, key: str, value: A) -> None:
        if self.root:
            new_node = self.insert_r(self.root, key, value)
            if new_node:
                root = new_node
                while root.parent:
                    root = root.parent
                self.root = root
        else:
            self.root = Node(BLACK, key, value, None, None, None)

    def insert_r(self, root: Node[A], key: str, value: A) -> Optional[Node]:
        if key == root.key:
            root.value = value
            return None
        elif key < root.key:
            if root.left:
                return self.insert_r(root.left, key, value)

            node = Node(RED, key, value, root, None, None)
            root.left = node
            self.insert_repair(node)
            return node
        else:
            if root.right:
                return self.insert_r(root.right, key, value)

            node = Node(RED, key, value, root, None, None)
            root.right = node
            self.insert_repair(node)
            return node

    def insert_repair(self, node) -> None:
        uncle = node.uncle
        grandparent = node.grandparent

        if not node.parent:
            node.color = BLACK
        elif node.parent.color == BLACK:
            pass
        elif grandparent and uncle and uncle.color == RED:
            node.parent.color = BLACK
            uncle.color = BLACK
            grandparent.color = RED
            self.insert_repair(grandparent)
        elif grandparent:
            n = node
            p = node.parent
            g = grandparent
            if node == p.right and p == g.left:
                self.rotate_left(p)
                n = node.left
            elif node == p.left and p == g.right:
                self.rotate_right(p)
                n = node.right
            if n:
                p = n.parent
                g = n.grandparent
                if n == p.left:
                    self.rotate_right(g)
                else:
                    self.rotate_left(g)
                p.color = BLACK
                g.color = RED

    def rotate_left(self, node) -> None:
        new_node = node.right
        p = node.parent
        node.right = new_node.left
        new_node.left = node
        node.parent = new_node
        if node.right:
            node.right.parent = node
        if p:
            if node == p.left:
                p.left = new_node
            elif node == p.right:
                p.right = new_node
        new_node.parent = p

    def rotate_right(self, node) -> None:
        new_node = node.left
        p = node.parent
        node.left = new_node.right
        new_node.right = node
        node.parent = new_node
        if node.left:
            node.left.parent = node
        if p:
            if node == p.left:
                p.left = new_node
            elif node == p.right:
                p.right = new_node
        new_node.parent = p

