from rbtree import RBTree, inorder_traversal

class Tombstone:
  pass

TOMBSTONE = Tombstone()

class Memtable:
  def __init__(self):
    self.bytes = 0
    self.tree = RBTree()

  def set(self, k, v):
    self.bytes += len(k) + len(v)
    self.tree.insert(k, v)

  def get(self, k):
    return self.tree.search(k)

  def unset(self, k):
    value = self.get(k)
    if value:
      self.bytes -= len(value)
    else:
      self.bytes += len(k)
    self.tree.insert(k, TOMBSTONE)

  def entries(self):
    yield from inorder_traversal(self.tree.root)
 
  def approximate_bytes(self):
    return self.bytes
