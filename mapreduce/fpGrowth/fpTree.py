
class FPTree(object):

    def __init__(self):
        self.root = Node(None, 0, {})
        self.summaries = {}

    def __repr__(self):
        return repr(self.root)

    def add(self, basket, count):
        curr = self.root
        curr.count += count

        for item in basket:
            if self.summaries.has_key(item):
                summary = self.summaries.get(item)
            else:
                summary = Summary(0,[])
                self.summaries[item] = summary
            summary.count += count

            if curr.children.has_key(item):
                child = curr.children.get(item)
            else:
                child = Node(item, 0, {})
                curr.addChild(child)
            summary.nodes.append(child)
            child.count += count
            curr = child
        return self


class Node(object):

    def __init__(self, item, count, children):
        self.item = item
        self.count = count
        self.children = children #dictionary of children

    def __repr__(self):
        return self.toString(0)

    def toString(self, level=0):
        if self.item == None:
            s = "Root("
        else:
            s = "(item="+str(self.item)
            s+= ", count="+str(self.count)
        tabs = "\t".join(['' for i in range(0,level+2)])
        for v in self.children.itervalues():
            s+= tabs+"\n"
            s+= tabs+v.toString(level=level+1)
        s+=")"
        return s

    def addChild(self, node):
        self.children[node.item] = node

class Summary(object):

    def __init__(self, count, nodes):
        self.count = count
        self.nodes = nodes
