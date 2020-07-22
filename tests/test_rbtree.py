# from kvs.rbtree_typed import RbTree
# from kvs.rbtree_typed import BLACK, Node, RBTree

from typing import Dict
from hypothesis import given, infer

from kvs.rbtree import RBTree, inorder_traversal

@given(kv_pairs=infer)
def test_from_dict_always_returns_sorted_tree(kv_pairs: Dict[str, int]):
    tree = RBTree.from_dict(kv_pairs)
    assert list(node.key for node in inorder_traversal(tree)) == sorted(kv_pairs.keys())

@given(kv_pairs=infer)
def test_isomorphism_exists_with_dict(kv_pairs: Dict[str, int]):
    assert RBTree.from_dict(kv_pairs).to_dict() == kv_pairs
