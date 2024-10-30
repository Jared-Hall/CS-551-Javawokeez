"""
Documentation for the B+ Tree class.
Author: Jared Hall jhall10@uoregon.edu
Description:
    This file contains our implementation of the b+ Tree
    Implemented from scratch with the aid of the visualizer and the lecture slides, and a data structures textbook.
    I think it is correct, but who knows.

    sparse documentation cuz am running out of time but will add more later. I think this is a pretty standard implementation
"""

class Node():
    __slots__ = ('_n', '_keys', '_values', '_isLeaf', '_next')

    def __init__(self, n):
        """Child nodes can be converted into parent nodes by setting self._isLeaf = False. Parent nodes
        simply act as a medium to traverse the tree."""
        self._n = n
        self._keys = []
        self._values = []
        self._isLeaf = True
        self._next = None

    def add(self, key, value):
        """Adds a key-value pair to the node."""
        # If the node is empty, simply insert the key-value pair.
        if not self._keys:
            self._keys.append(key)
            self._values.append([value])
            return None

        for i, item in enumerate(self._keys):
            # If new key matches existing key, add to list of values.
            if key == item:
                if value not in self._values[i]:
                    self._values[i].append(value)
                break

            # If new key is smaller than existing key, insert new key to the left of existing key.
            elif key < item:
                self._keys = self._keys[:i] + [key] + self._keys[i:]
                self._values = self._values[:i] + [[value]] + self._values[i:]
                break

            # If new key is larger than all existing keys, insert new key to the right of all
            # existing keys.
            elif i + 1 == len(self._keys):
                self._keys.append(key)
                self._values.append([value])

    def split(self):
        """Splits the node into two and stores them as child nodes."""
        left = Node(self._n)
        right = Node(self._n)
        left._next = right
        self._next = None
        mid = self._n // 2

        left._keys = self._keys[:mid]
        left._values = self._values[:mid]

        right._keys = self._keys[mid:]
        right._values = self._values[mid:]

        # When the node is split, set the parent key to the left-most key of the right child node.
        self._keys = [right._keys[0]]
        self._values = [left, right]
        self._isLeaf = False

    def is_full(self):
        """Returns True if the node is full."""
        return len(self._keys) == self._n

    def printNode(self, counter=0, parent=None):
        """Prints the keys at each level."""
        if not parent:
            parent = 'root'
        print(counter, str(self._keys), str(self._values), self._isLeaf, parent)
        # Recursively print the key of child nodes (if these exist).
        if not self._isLeaf:
            for item in self._values:
                item.printNode(counter + 1, str(self._keys))

    def items(self):
        for key, values in zip(self._keys, self._values):
            if isinstance(values, list):
                yield from [(key, value) for value in values]
        if not self._isLeaf:
            for item in self._values:
                yield from item.items()

    def keys(self):
        for keys, values in zip(self._keys, self._values):
            if isinstance(values, list):
                yield from [keys]
        if not self._isLeaf:
            for item in self._values:
                yield from item.keys()

    def __repr__(self):
        return F"Node ({str(self._keys)})"

class BPlusTree():
    """B+ tree object, consisting of nodes.
    Nodes will automatically be split into two once it is full. When a split occurs, a key will
    'float' upwards and be inserted into the parent node to act as a pivot.
    Attributes:
        order (int): The maximum number of keys each node can hold.
    """
    def __init__(self, n=2):
        self._n = n
        self.root = Node(n)

    def _find(self, node, key):
        """ For a given node and key, returns the index where the key should be inserted and the
        list of values at that index."""
        for i, item in enumerate(node._keys):
            if key < item:
                return node._values[i], i

        return node._values[i + 1], i + 1

    def _merge(self, parent, child, index):
        """For a parent and child node, extract a pivot from the child to be inserted into the keys
        of the parent. Insert the values from the child into the values of the parent.
        """
        parent._values.pop(index)
        pivot = child._keys[0]

        for i, item in enumerate(parent._keys):
            if pivot < item:
                parent._keys = parent._keys[:i] + [pivot] + parent._keys[i:]
                parent._values = parent._values[:i] + child._values + parent._values[i:]
                break

            elif i + 1 == len(parent._keys):
                parent._keys += [pivot]
                parent._values += child._values
                break

    def insert(self, key, value):
        """Inserts a key-value pair after traversing to a leaf node. If the leaf node is full, split
        the leaf node into two.
        """
        parent = None
        child = self.root

        # Traverse tree until leaf node is reached.
        while not child._isLeaf:
            parent = child
            child, index = self._find(child, key)

        child.add(key, value)

        # If the leaf node is full, split the leaf node into two.
        if child.is_full():
            child.split()

            # Once a leaf node is split, it consists of a internal node and two leaf nodes. These
            # need to be re-inserted back into the tree.
            if parent and not parent.is_full():
                self._merge(parent, child, index)

    def query(self, key):
        """Returns a value for a given key, and False if the key does not exist."""
        child = self.root
        while not child._isLeaf:
            child, index = self._find(child, key)
        for i, item in enumerate(child._keys):
            if key == item:
                return child._values[i]
        return False

    def rangeQuery(self, startKey, endKey):
        child = self.root
        while not child._isLeaf:
            child, index = self._find(child, startKey)
        print(child._isLeaf, child._next)
        current = child
        values = []
        while(list(current.keys())[0] < endKey):
            print("current Node: ", list(current.keys())[0])
            values.append(current._values)
            current = current._next
        return values

    def show(self):
        """Prints the keys at each level."""
        self.root.printNode()


if(__name__ == "__main__"):
    tree = BPlusTree()
    for i in range(20):
        key = 906659671+i
        value = (93, 0, 0, i)
        tree.insert(key, value)
    
    print("\n")
    print("====== Whole Tree =====")
    tree.show()
    print("=======================")
    print("\n")
    print("====== query =====")
    output = tree.query(906659672)
    print("Looking in tree for 906659676:", output)
    out2 = tree.rangeQuery(906659671, 906659680)
    print("Range query: ", out2)
    print("======================")

