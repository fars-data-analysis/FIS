
class FPTree(object):

    def __init__(self):
        self.root = Node(None, 0, [])
        self.summaries = Summary(0, {})

class Node(object):

    def __init__(self, item, count, children):
        self.item = item
        self.count = count
        self.children = children #dictionary of children

class Summary(object):

    def __init__(self, count, nodes):
        self.count = count
        self.nodes = nodes
