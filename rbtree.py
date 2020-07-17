BLACK=0
RED=1

def inorder_traversal(node):
    if node.left:
        yield from inorder_traversal(node.left)
    yield (node.key, node.value)
    if node.right:
        yield from inorder_traversal(node.right)

class Node:
    def __init__(self, color, key, value, parent, left, right):
        self.color = color
        self.key = key
        self.value = value
        self.parent = parent
        self.left = left
        self.right = right

    def uncle(self):
        g = self.grandparent()
        if g:
            if g.left and g.left == self.parent:
                return g.right
            else:
                return g.left
        return None

    def grandparent(self):
        p = self.parent
        if p:
            return p.parent
        return None

class RBTree:
    def __init__(self):
        self.root = None

    def search(self, k):
        if self.root:
            return self.search_r(self.root, k)
        return None
    
    def search_r(self, root, k):
        if k < root.key and root.left:
            return self.search_r(root.left, k)
        elif k > root.key and root.right:
            return self.search_r(root.right, k)
        elif k == root.key:
            return root.value
        return None

    def insert(self, k, v):
        if self.root:
            new_node = self.insert_r(self.root, k, v)
            if new_node:
                root = new_node
                while root.parent:
                    root = root.parent
                self.root = root
        else:
            self.root = Node(BLACK, k, v, None, None, None)

    def insert_r(self, root, k, v):
        if k == root.key:
            root.value = v
        elif k < root.key:
            if root.left:
                return self.insert_r(root.left, k, v)
            else:
                node = Node(RED, k, v, root, None, None)
                root.left = node
                self.insert_repair(node)
                return node
        else:
            if root.right:
                return self.insert_r(root.right, k, v)
            else:
                node = Node(RED, k, v, root, None, None)
                root.right = node
                self.insert_repair(node)
                return node
        return None

    def insert_repair(self, node):
        if not node.parent:
            node.color = BLACK
        elif node.parent.color == BLACK:
            pass
        elif node.uncle() and node.uncle().color == RED:
            node.parent.color = BLACK
            node.uncle().color = BLACK
            node.grandparent().color = RED
            self.insert_repair(node.grandparent())
        else:
            n = node
            p = node.parent
            g = node.grandparent()
            if node == p.right and p == g.left:
                self.rotate_left(p)
                n = node.left
            elif node == p.left and p == g.right:
                self.rotate_right(p)
                n = node.right
            if n:
                p = n.parent
                g = n.grandparent()
                if n == p.left:
                    self.rotate_right(g)
                else:
                    self.rotate_left(g)
                p.color = BLACK
                g.color = RED

    def rotate_left(self, node):
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

    def rotate_right(self, node):
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

